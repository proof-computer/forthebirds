use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv6Addr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use forthebirds_net::{
    PeerTarget, PeerTransport, StaticPeer, StaticPeers, TransportConfig, TransportState,
    pinned_client,
};
use forthebirds_store::{
    ActionRecord, AppAction, Error, Host, Ledger, LedgerConfig, LedgerRecord, Result, action_id,
    check_record,
};
use forthebirds_sync::{
    CATCH_UP_PAGES, LinkStatus, PROTOCOL, ROUTE, Replicator, Runs, SyncConfig, TickSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const OTHER_TOKEN: &str = "abcdef0123456789abcdef0123456789";
/// `Ledger::changes` asks the store for 16 rows per page.
const CHANGES_PAGE: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Note {
    text: String,
}
impl AppAction for Note {
    fn validate_shape(&self) -> Result<()> {
        if self.text.is_empty() {
            return Err(Error::new("invalid", "empty note"));
        }
        Ok(())
    }
    fn evidence_ids(&self) -> Vec<&str> {
        Vec::new()
    }
    fn kind(&self) -> &'static str {
        "note"
    }
    fn is_content(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
enum Rec {
    Action(ActionRecord<Note>),
}
impl LedgerRecord for Rec {
    const APP: &'static str = "sync-lab";
    const MAX_KNOWN_SCHEMA: u32 = 1;
    fn id(&self) -> &str {
        match self {
            Self::Action(record) => &record.id,
        }
    }
    fn author(&self) -> &str {
        match self {
            Self::Action(record) => &record.author,
        }
    }
    fn schema(&self) -> u32 {
        match self {
            Self::Action(record) => record.schema,
        }
    }
    fn validate(&self, run: &str) -> Result<()> {
        check_record(self)?;
        match self {
            Self::Action(record) => record.validate_envelope(run),
        }
    }
    fn check_id(id: &str) -> Result<()> {
        if !id.starts_with("r:a:")
            || id.len() != 68
            || !id[4..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::new("invalid", "invalid record id"));
        }
        Ok(())
    }
    fn receipt(&self) -> Option<(u64, u64)> {
        match self {
            Self::Action(record) => Some((record.author_sequence, record.memory_tokens)),
        }
    }
}
impl From<ActionRecord<Note>> for Rec {
    fn from(record: ActionRecord<Note>) -> Self {
        Self::Action(record)
    }
}
impl TryFrom<Rec> for ActionRecord<Note> {
    type Error = Error;
    fn try_from(record: Rec) -> Result<Self> {
        match record {
            Rec::Action(record) => Ok(record),
        }
    }
}

#[derive(Clone)]
struct SyncHost {
    ledger: Ledger<Rec, ()>,
    refuse: Arc<AtomicBool>,
}
impl Host<Rec, ()> for SyncHost {
    type Error = Error;
    fn ledger(&self) -> &Ledger<Rec, ()> {
        &self.ledger
    }
    fn code(error: &Error) -> &str {
        error.code
    }
    async fn validate_import(&self, _record: &Rec) -> Result<()> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(Error::new("conflict", "import refused"));
        }
        Ok(())
    }
    fn observe(&self, _app: &mut (), _record: &Rec) {}
    fn recover_local(&self, _app: &mut (), _key: &str, _value: &Value) -> Result<()> {
        Ok(())
    }
}
impl SyncHost {
    async fn open(dir: &Path, run: &str, peer: &str) -> Self {
        let ledger = Ledger::open(LedgerConfig {
            database_path: dir.join(format!("{peer}-{run}.redb")),
            database_cache_bytes: 8 * 1024 * 1024,
            run_id: run.into(),
            peer_id: peer.into(),
            token_budget: Some(20_000),
            memory_budget: 20_000,
            memory_item_tokens: 64,
            // The cap test seeds more than one catch-up budget of records.
            max_documents: CATCH_UP_PAGES * CHANGES_PAGE + 32,
        })
        .await
        .unwrap();
        let host = Self {
            ledger,
            refuse: Arc::new(AtomicBool::new(false)),
        };
        let cache = host.ledger.recover(&host).await.unwrap();
        *host.ledger.cache.lock().await = cache;
        host
    }

    async fn append(&self, operation: &str, text: &str) {
        self.ledger
            .execute(self, operation, Note { text: text.into() }, 1, 1)
            .await
            .unwrap();
    }

    async fn ids(&self) -> BTreeSet<String> {
        let mut ids = BTreeSet::new();
        let mut after = None;
        loop {
            let page = self.ledger.page(after.as_deref(), 32).await.unwrap();
            for record in &page.records {
                ids.insert(record.id().to_owned());
            }
            after = page.next_cursor;
            if after.is_none() {
                break;
            }
        }
        ids
    }
}

struct Member {
    host: SyncHost,
    sync: Replicator<SyncHost, Rec, ()>,
}

struct Side {
    host: SyncHost,
    sync: Replicator<SyncHost, Rec, ()>,
    transport: PeerTransport,
    ad: PathBuf,
}

struct Peer {
    runs: Runs<SyncHost, Rec, ()>,
    members: BTreeMap<String, Member>,
    transport: PeerTransport,
    ad: PathBuf,
}

fn replicator(
    host: &SyncHost,
    run: &str,
    peer: &str,
    token: &str,
) -> Replicator<SyncHost, Rec, ()> {
    Replicator::new(
        host.clone(),
        SyncConfig {
            run_id: run.into(),
            peer_id: peer.into(),
            token: token.into(),
            request_timeout: Duration::from_secs(5),
        },
    )
    .unwrap()
}

async fn side(root: &Path, peer: &str, run: &str, token: &str) -> Side {
    let host = SyncHost::open(root, run, peer).await;
    let sync = replicator(&host, run, peer, token);
    let ad = root.join(format!("{peer}.json"));
    let transport = PeerTransport::start(
        &TransportConfig {
            listen: "[::1]:0".parse().unwrap(),
            advertisement_file: Some(ad.clone()),
            service: "swarm".into(),
            application: "sync-lab".into(),
        },
        &TransportState::default(),
        sync.router(),
    )
    .await
    .unwrap();
    Side {
        host,
        sync,
        transport,
        ad,
    }
}

async fn peer(root: &Path, name: &str, runs: &[&str], token: &str) -> Peer {
    let set = Runs::new(name, token).unwrap();
    let mut members = BTreeMap::new();
    for run in runs {
        let host = SyncHost::open(root, run, name).await;
        let sync = replicator(&host, run, name, token);
        set.insert(sync.clone()).await.unwrap();
        members.insert((*run).to_owned(), Member { host, sync });
    }
    let ad = root.join(format!("{name}.json"));
    let transport = PeerTransport::start(
        &TransportConfig {
            listen: "[::1]:0".parse().unwrap(),
            advertisement_file: Some(ad.clone()),
            service: "swarm".into(),
            application: "sync-lab".into(),
        },
        &TransportState::default(),
        set.router(),
    )
    .await
    .unwrap();
    Peer {
        runs: set,
        members,
        transport,
        ad,
    }
}

fn targets(name: &str, ad: &Path) -> BTreeMap<String, forthebirds_net::Result<PeerTarget>> {
    StaticPeers::new(vec![StaticPeer {
        name: name.into(),
        address: Ipv6Addr::LOCALHOST,
        advertisement_file: ad.into(),
    }])
    .unwrap()
    .read()
}

async fn tick(sync: &Replicator<SyncHost, Rec, ()>, name: &str, ad: &Path) -> TickSummary {
    let (_sender, mut shutdown) = tokio::sync::watch::channel(false);
    sync.tick(targets(name, ad), &mut shutdown).await
}

fn link<'a>(links: &'a BTreeMap<String, LinkStatus>, name: &str) -> &'a LinkStatus {
    links
        .get(name)
        .unwrap_or_else(|| panic!("missing link {name}"))
}

async fn post_exchange(
    ad: &Path,
    token: &str,
    run: &str,
    peer: &str,
    protocol: u32,
) -> (u16, Value) {
    let advertisement: Value = serde_json::from_slice(&std::fs::read(ad).unwrap()).unwrap();
    let pin = advertisement["tlsFingerprint"].as_str().unwrap();
    let port = advertisement["port"].as_u64().unwrap();
    let client = pinned_client(pin, Duration::from_secs(5)).unwrap();
    let response = client
        .client
        .post(format!("https://[::1]:{port}{ROUTE}"))
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&json!({
                "protocol": protocol,
                "run_id": run,
                "peer_id": peer,
                "records": [],
                "since": 0,
                "database_id": null
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    (status, body)
}

#[tokio::test]
async fn one_tick_of_each_member_replicates_both_runs_and_keeps_them_apart() {
    assert_eq!(PROTOCOL, 1);
    let root = tempfile::tempdir().unwrap();
    let a = peer(root.path(), "peer-a", &["run-1", "run-2"], TOKEN).await;
    let b = peer(root.path(), "peer-b", &["run-2", "run-1"], TOKEN).await;
    assert_eq!(
        a.runs.run_ids().await,
        vec!["run-1".to_owned(), "run-2".to_owned()]
    );
    assert!(a.runs.get("run-1").await.is_some());
    assert!(a.runs.get("run-3").await.is_none());

    a.members["run-1"].host.append("from-a", "alpha").await;
    b.members["run-1"].host.append("from-b", "beta").await;
    a.members["run-2"].host.append("from-a", "gamma").await;
    b.members["run-2"].host.append("from-b", "delta").await;

    let first = tick(&a.members["run-1"].sync, "peer-b", &b.ad).await;
    let second = tick(&a.members["run-2"].sync, "peer-b", &b.ad).await;
    assert_eq!(first.failure_code, "none");
    assert_eq!(second.failure_code, "none");

    let run_1 = BTreeSet::from([
        action_id("run-1", "peer-a", "from-a"),
        action_id("run-1", "peer-b", "from-b"),
    ]);
    let run_2 = BTreeSet::from([
        action_id("run-2", "peer-a", "from-a"),
        action_id("run-2", "peer-b", "from-b"),
    ]);
    assert_eq!(a.members["run-1"].host.ids().await, run_1);
    assert_eq!(b.members["run-1"].host.ids().await, run_1);
    assert_eq!(a.members["run-2"].host.ids().await, run_2);
    assert_eq!(b.members["run-2"].host.ids().await, run_2);
    assert!(run_1.is_disjoint(&run_2));

    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}

#[tokio::test]
async fn runs_router_converges_with_a_plain_replicator_both_ways() {
    let root = tempfile::tempdir().unwrap();
    let a = peer(root.path(), "peer-a", &["run-1"], TOKEN).await;
    let b = side(root.path(), "peer-b", "run-1", TOKEN).await;
    a.members["run-1"].host.append("from-a", "alpha").await;
    b.host.append("from-b", "beta").await;

    let pushed = tick(&a.members["run-1"].sync, "peer-b", &b.ad).await;
    assert_eq!(pushed.failure_code, "none");
    let pulled = tick(&b.sync, "peer-a", &a.ad).await;
    assert_eq!(pulled.failure_code, "none");

    let both = BTreeSet::from([
        action_id("run-1", "peer-a", "from-a"),
        action_id("run-1", "peer-b", "from-b"),
    ]);
    assert_eq!(a.members["run-1"].host.ids().await, both);
    assert_eq!(b.host.ids().await, both);

    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}

#[tokio::test]
async fn unknown_run_conflicts_wrong_bearer_is_unauthorized_and_membership_follows_the_router() {
    let root = tempfile::tempdir().unwrap();
    let host = SyncHost::open(root.path(), "run-1", "peer-b").await;
    host.append("kept", "stay").await;
    let run_1 = replicator(&host, "run-1", "peer-b", TOKEN);
    let run_2_host = SyncHost::open(root.path(), "run-2", "peer-b").await;
    let run_2 = replicator(&run_2_host, "run-2", "peer-b", TOKEN);
    let runs = Runs::new("peer-b", TOKEN).unwrap();
    runs.insert(run_1).await.unwrap();
    let ad = root.path().join("peer-b.json");
    let transport = PeerTransport::start(
        &TransportConfig {
            listen: "[::1]:0".parse().unwrap(),
            advertisement_file: Some(ad.clone()),
            service: "swarm".into(),
            application: "sync-lab".into(),
        },
        &TransportState::default(),
        runs.router(),
    )
    .await
    .unwrap();
    let before = host.ids().await;

    let (status, body) = post_exchange(&ad, TOKEN, "run-3", "peer-a", PROTOCOL).await;
    assert_eq!(status, 409);
    assert_eq!(body["code"], "conflict");
    assert_eq!(host.ids().await, before);

    let (status, body) = post_exchange(&ad, OTHER_TOKEN, "run-1", "peer-a", PROTOCOL).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], "unauthorized");
    assert_eq!(host.ids().await, before);

    let (status, body) = post_exchange(&ad, TOKEN, "run-1", "peer-a", 99).await;
    assert_eq!(status, 410);
    assert_eq!(body["code"], "upgrade_required");
    assert_eq!(host.ids().await, before);

    let (status, _) = post_exchange(&ad, TOKEN, "run-2", "peer-a", PROTOCOL).await;
    assert_eq!(status, 409);
    runs.insert(run_2).await.unwrap();
    let (status, _) = post_exchange(&ad, TOKEN, "run-2", "peer-a", PROTOCOL).await;
    assert_eq!(status, 200);
    let removed = runs.remove("run-2").await.expect("run-2 was a member");
    let (status, body) = post_exchange(&ad, TOKEN, "run-2", "peer-a", PROTOCOL).await;
    assert_eq!(status, 409);
    assert_eq!(body["code"], "conflict");
    runs.insert(removed).await.unwrap();
    let (status, _) = post_exchange(&ad, TOKEN, "run-2", "peer-a", PROTOCOL).await;
    assert_eq!(status, 200);
    assert_eq!(host.ids().await, before);

    transport.stop().await.unwrap();
}

#[tokio::test]
async fn insert_refuses_a_different_peer_token_or_duplicate_run() {
    let root = tempfile::tempdir().unwrap();
    let short = Runs::<SyncHost, Rec, ()>::new("peer-a", "short");
    assert_eq!(short.err().unwrap().code, "config");
    let named = Runs::<SyncHost, Rec, ()>::new("peer a", TOKEN);
    assert_eq!(named.err().unwrap().code, "config");

    let runs = Runs::new("peer-a", TOKEN).unwrap();
    let host = SyncHost::open(root.path(), "run-1", "peer-a").await;
    let other_peer = replicator(&host, "run-1", "peer-b", TOKEN);
    assert_eq!(runs.insert(other_peer).await.err().unwrap().code, "config");
    let other_token = replicator(&host, "run-1", "peer-a", OTHER_TOKEN);
    assert_eq!(runs.insert(other_token).await.err().unwrap().code, "config");
    let member = replicator(&host, "run-1", "peer-a", TOKEN);
    runs.insert(member.clone()).await.unwrap();
    assert_eq!(runs.insert(member).await.err().unwrap().code, "config");
    assert_eq!(runs.run_ids().await, vec!["run-1".to_owned()]);
}

#[tokio::test]
async fn a_few_records_are_caught_up_after_one_tick() {
    let root = tempfile::tempdir().unwrap();
    let a = side(root.path(), "peer-a", "run-1", TOKEN).await;
    let b = side(root.path(), "peer-b", "run-1", TOKEN).await;
    b.host.append("one", "alpha").await;
    b.host.append("two", "beta").await;
    b.host.append("three", "gamma").await;

    let summary = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(summary.failure_code, "none");
    let status = link(&a.sync.links().await, "peer-b").clone();
    assert_eq!(status.caught_up_ms, Some(status.last_attempt_ms));
    assert_eq!(status.error, None);
    assert_eq!(a.sync.caught_up_ms().await, status.caught_up_ms);
    assert_eq!(a.host.ids().await, b.host.ids().await);
    assert_eq!(a.host.ids().await.len(), 3);

    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}

#[tokio::test]
async fn a_backlog_past_the_page_cap_is_caught_up_only_on_the_draining_tick() {
    let over = CATCH_UP_PAGES * CHANGES_PAGE + 1;
    let root = tempfile::tempdir().unwrap();
    let a = side(root.path(), "peer-a", "run-1", TOKEN).await;
    let b = side(root.path(), "peer-b", "run-1", TOKEN).await;
    for n in 0..over {
        b.host.append(&format!("n{n}"), "x").await;
    }
    assert_eq!(b.host.ids().await.len(), over);

    let first = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(first.failure_code, "none");
    let capped = link(&a.sync.links().await, "peer-b").clone();
    assert_eq!(capped.caught_up_ms, None);
    assert!(capped.last_success_ms.is_some());
    assert!(a.host.ids().await.len() < over);
    assert_eq!(a.sync.caught_up_ms().await, None);

    let second = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(second.failure_code, "none");
    let drained = link(&a.sync.links().await, "peer-b").clone();
    assert_eq!(drained.caught_up_ms, Some(drained.last_attempt_ms));
    assert_eq!(a.sync.caught_up_ms().await, drained.caught_up_ms);
    assert_eq!(a.host.ids().await, b.host.ids().await);

    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}

#[tokio::test]
async fn a_failed_target_never_sets_caught_up() {
    let root = tempfile::tempdir().unwrap();
    let a = side(root.path(), "peer-a", "run-1", TOKEN).await;
    let b = side(root.path(), "peer-b", "run-1", OTHER_TOKEN).await;
    b.host.append("hidden", "nope").await;

    let summary = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(summary.failure_code, "auth");
    let status = link(&a.sync.links().await, "peer-b").clone();
    assert_eq!(status.error, Some("auth"));
    assert_eq!(status.caught_up_ms, None);
    assert_eq!(a.sync.caught_up_ms().await, None);
    assert!(a.host.ids().await.is_empty());
    assert_eq!(b.host.ids().await.len(), 1);

    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}
