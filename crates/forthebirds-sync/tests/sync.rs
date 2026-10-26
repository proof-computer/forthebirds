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
use forthebirds_sync::{LinkStatus, ROUTE, Replicator, SyncConfig, TickSummary};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

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
    async fn open(dir: &Path, run: &str, peer: &str, refuse: Arc<AtomicBool>) -> Self {
        let ledger = Ledger::open(LedgerConfig {
            database_path: dir.join(format!("{peer}.redb")),
            database_cache_bytes: 1024 * 1024,
            run_id: run.into(),
            peer_id: peer.into(),
            token_budget: Some(10_000),
            memory_budget: 10_000,
            memory_item_tokens: 64,
            max_documents: 1_000,
        })
        .await
        .unwrap();
        let host = Self { ledger, refuse };
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

struct Side {
    host: SyncHost,
    sync: Replicator<SyncHost, Rec, ()>,
    transport: PeerTransport,
    ad: PathBuf,
}

struct Lab {
    a: Side,
    b: Side,
    _root: tempfile::TempDir,
}

impl Lab {
    async fn open(run_a: &str, run_b: &str, token_a: &str, token_b: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let a = side(
            root.path(),
            "peer-a",
            run_a,
            token_a,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let b = side(
            root.path(),
            "peer-b",
            run_b,
            token_b,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        Self { a, b, _root: root }
    }

    async fn close(self) {
        self.a.transport.stop().await.unwrap();
        self.b.transport.stop().await.unwrap();
    }
}

async fn side(root: &Path, peer: &str, run: &str, token: &str, refuse: Arc<AtomicBool>) -> Side {
    let host = SyncHost::open(root, run, peer, refuse).await;
    let sync = Replicator::new(
        host.clone(),
        SyncConfig {
            run_id: run.into(),
            peer_id: peer.into(),
            token: token.into(),
            request_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
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

#[tokio::test]
async fn one_tick_replicates_both_records_and_notifies() {
    let lab = Lab::open("run-1", "run-1", TOKEN, TOKEN).await;
    lab.a.host.append("from-a", "alpha").await;
    lab.b.host.append("from-b", "beta").await;
    let id_a = action_id("run-1", "peer-a", "from-a");
    let id_b = action_id("run-1", "peer-b", "from-b");
    let received = lab.b.sync.received();
    let notified = received.notified();
    let summary = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(summary.failure_code, "none");
    assert!(
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .is_ok(),
        "receiver was not notified"
    );
    assert_eq!(
        lab.a.host.ids().await,
        BTreeSet::from([id_a.clone(), id_b.clone()])
    );
    assert_eq!(lab.b.host.ids().await, BTreeSet::from([id_a, id_b]));
    let links = lab.a.sync.links().await;
    assert_eq!(link(&links, "peer-b").error, None);
    assert!(link(&links, "peer-b").last_success_ms.is_some());
    lab.close().await;
}

#[tokio::test]
async fn forty_records_drain_in_one_tick_and_the_next_adds_nothing() {
    let lab = Lab::open("run-1", "run-1", TOKEN, TOKEN).await;
    for number in 0..40 {
        lab.a
            .host
            .append(&format!("n{number}"), &format!("text-{number}"))
            .await;
    }
    let summary = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(summary.failure_code, "none");
    let drained = lab.b.host.ids().await;
    assert_eq!(drained.len(), 40);
    assert_eq!(drained, lab.a.host.ids().await);
    let again = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(again.failure_code, "none");
    assert_eq!(lab.b.host.ids().await, drained);
    lab.close().await;
}

#[tokio::test]
async fn wrong_token_records_auth_and_imports_nothing() {
    let other = "abcdef0123456789abcdef0123456789";
    let lab = Lab::open("run-1", "run-1", TOKEN, other).await;
    lab.a.host.append("from-a", "alpha").await;
    lab.b.host.append("from-b", "beta").await;
    let summary = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(summary.failure_code, "auth");
    assert_eq!(
        link(&lab.a.sync.links().await, "peer-b").error,
        Some("auth")
    );
    assert_eq!(lab.a.host.ids().await.len(), 1);
    assert_eq!(lab.b.host.ids().await.len(), 1);
    lab.close().await;
}

#[tokio::test]
async fn other_run_records_exchange_and_answers_conflict() {
    let lab = Lab::open("run-1", "run-2", TOKEN, TOKEN).await;
    lab.a.host.append("from-a", "alpha").await;
    lab.b.host.append("from-b", "beta").await;
    let summary = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(summary.failure_code, "exchange");
    assert_eq!(
        link(&lab.a.sync.links().await, "peer-b").error,
        Some("exchange")
    );
    assert_eq!(lab.a.host.ids().await.len(), 1);
    assert_eq!(lab.b.host.ids().await.len(), 1);

    let advertisement: Value = serde_json::from_slice(&std::fs::read(&lab.b.ad).unwrap()).unwrap();
    let pin = advertisement["tlsFingerprint"].as_str().unwrap();
    let port = advertisement["port"].as_u64().unwrap();
    let client = pinned_client(pin, Duration::from_secs(5)).unwrap();
    let response = client
        .client
        .post(format!("https://[::1]:{port}{ROUTE}"))
        .bearer_auth(TOKEN)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&json!({
                "protocol": 1,
                "run_id": "run-1",
                "peer_id": "peer-a",
                "records": [],
                "since": 0,
                "database_id": null
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["code"], "conflict");
    lab.close().await;
}

#[tokio::test]
async fn mismatched_target_name_records_identity() {
    let lab = Lab::open("run-1", "run-1", TOKEN, TOKEN).await;
    lab.a.host.append("from-a", "alpha").await;
    lab.b.host.append("from-b", "beta").await;
    let before = lab.a.host.ids().await;
    let summary = tick(&lab.a.sync, "not-peer-b", &lab.b.ad).await;
    assert_eq!(summary.failure_code, "identity");
    assert_eq!(
        link(&lab.a.sync.links().await, "not-peer-b").error,
        Some("identity")
    );
    assert_eq!(lab.a.host.ids().await, before);
    lab.close().await;
}

#[tokio::test]
async fn refused_import_holds_the_cursor_until_the_next_tick() {
    let root = tempfile::tempdir().unwrap();
    let refuse = Arc::new(AtomicBool::new(false));
    let a = side(
        root.path(),
        "peer-a",
        "run-1",
        TOKEN,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let b = side(root.path(), "peer-b", "run-1", TOKEN, Arc::clone(&refuse)).await;
    a.host.append("held", "wait").await;
    refuse.store(true, Ordering::SeqCst);
    let blocked = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(blocked.failure_code, "exchange");
    assert!(b.host.ids().await.is_empty());
    refuse.store(false, Ordering::SeqCst);
    let resumed = tick(&a.sync, "peer-b", &b.ad).await;
    assert_eq!(resumed.failure_code, "none");
    assert_eq!(b.host.ids().await, a.host.ids().await);
    assert_eq!(b.host.ids().await.len(), 1);
    a.transport.stop().await.unwrap();
    b.transport.stop().await.unwrap();
}

#[tokio::test]
async fn rotation_drops_the_old_pin_and_still_converges() {
    let lab = Lab::open("run-1", "run-1", TOKEN, TOKEN).await;
    lab.a.host.append("first", "one").await;
    lab.b.host.append("second", "two").await;
    let first = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(first.failure_code, "none");
    assert_eq!(lab.a.host.ids().await.len(), 2);
    lab.b.transport.rotate_now().await.unwrap();
    lab.a.host.append("third", "three").await;
    let second = tick(&lab.a.sync, "peer-b", &lab.b.ad).await;
    assert_eq!(second.failure_code, "none");
    assert_eq!(lab.b.host.ids().await.len(), 3);
    assert_eq!(lab.a.host.ids().await, lab.b.host.ids().await);
    lab.close().await;
}

#[tokio::test]
async fn new_refuses_a_short_token_and_a_bad_peer_id() {
    let root = tempfile::tempdir().unwrap();
    let host = SyncHost::open(
        root.path(),
        "run-1",
        "peer-a",
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let short = Replicator::<_, Rec, ()>::new(
        host.clone(),
        SyncConfig {
            run_id: "run-1".into(),
            peer_id: "peer-a".into(),
            token: "a".repeat(31),
            request_timeout: Duration::from_secs(1),
        },
    );
    assert_eq!(short.err().unwrap().code, "config");
    let named = Replicator::<_, Rec, ()>::new(
        host,
        SyncConfig {
            run_id: "run-1".into(),
            peer_id: "peer a".into(),
            token: TOKEN.into(),
            request_timeout: Duration::from_secs(1),
        },
    );
    assert_eq!(named.err().unwrap().code, "config");
}

#[tokio::test]
async fn run_returns_once_shutdown_is_set() {
    let root = tempfile::tempdir().unwrap();
    let host = SyncHost::open(
        root.path(),
        "run-1",
        "peer-a",
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let sync = Replicator::new(
        host,
        SyncConfig {
            run_id: "run-1".into(),
            peer_id: "peer-a".into(),
            token: TOKEN.into(),
            request_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        sync.run(Duration::from_secs(60), BTreeMap::new, receiver)
            .await;
    });
    sender.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}
