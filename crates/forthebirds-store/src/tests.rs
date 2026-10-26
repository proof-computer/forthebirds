use super::*;
use rouchdb::{Database, OpenOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TestAction {
    note: String,
}
impl AppAction for TestAction {
    fn validate_shape(&self) -> Result<()> {
        if self.note.is_empty() {
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
enum TestRecord {
    Action(ActionRecord<TestAction>),
    Evidence(EvidenceRecord<String>),
}
impl LedgerRecord for TestRecord {
    const APP: &'static str = "ledger-test";
    const MAX_KNOWN_SCHEMA: u32 = 1;
    fn id(&self) -> &str {
        match self {
            Self::Action(record) => &record.id,
            Self::Evidence(record) => &record.id,
        }
    }
    fn author(&self) -> &str {
        match self {
            Self::Action(record) => &record.author,
            Self::Evidence(record) => &record.observed_by,
        }
    }
    fn schema(&self) -> u32 {
        match self {
            Self::Action(record) => record.schema,
            Self::Evidence(record) => record.schema,
        }
    }
    fn validate(&self, run: &str) -> Result<()> {
        check_record(self)?;
        match self {
            Self::Action(record) => record.validate_envelope(run),
            Self::Evidence(record) => record.validate_envelope(run),
        }
    }
    fn check_id(id: &str) -> Result<()> {
        if !(id.starts_with("r:a:") || id.starts_with("r:e:"))
            || id.len() != 68
            || hex::decode(&id[4..]).is_err()
        {
            return Err(Error::new("invalid", "invalid record ID"));
        }
        Ok(())
    }
    fn receipt(&self) -> Option<(u64, u64)> {
        match self {
            Self::Action(record) => Some((record.author_sequence, record.memory_tokens)),
            Self::Evidence(_) => None,
        }
    }
}
impl From<ActionRecord<TestAction>> for TestRecord {
    fn from(record: ActionRecord<TestAction>) -> Self {
        Self::Action(record)
    }
}
impl From<EvidenceRecord<String>> for TestRecord {
    fn from(record: EvidenceRecord<String>) -> Self {
        Self::Evidence(record)
    }
}
impl TryFrom<TestRecord> for ActionRecord<TestAction> {
    type Error = Error;
    fn try_from(record: TestRecord) -> Result<Self> {
        match record {
            TestRecord::Action(record) => Ok(record),
            TestRecord::Evidence(_) => Err(Error::new("storage", "wrong action type")),
        }
    }
}
impl TryFrom<TestRecord> for EvidenceRecord<String> {
    type Error = Error;
    fn try_from(record: TestRecord) -> Result<Self> {
        match record {
            TestRecord::Evidence(record) => Ok(record),
            TestRecord::Action(_) => Err(Error::new("storage", "wrong evidence type")),
        }
    }
}

#[derive(Clone)]
struct TestHost {
    ledger: Ledger<TestRecord, ()>,
}
impl Host<TestRecord, ()> for TestHost {
    type Error = Error;
    fn ledger(&self) -> &Ledger<TestRecord, ()> {
        &self.ledger
    }
    fn code(error: &Error) -> &str {
        error.code
    }
    async fn validate_import(&self, _record: &TestRecord) -> Result<()> {
        Ok(())
    }
    fn observe(&self, _app: &mut (), _record: &TestRecord) {}
    fn recover_local(&self, _app: &mut (), _key: &str, _value: &Value) -> Result<()> {
        Ok(())
    }
}

fn config(dir: &std::path::Path, name: &str) -> LedgerConfig {
    LedgerConfig {
        database_path: dir.join(name),
        database_cache_bytes: 1024 * 1024,
        run_id: "run-1".into(),
        peer_id: "peer-1".into(),
        token_budget: Some(100),
        memory_budget: 100,
        memory_item_tokens: 40,
        max_documents: 100,
    }
}
async fn opened(config: LedgerConfig) -> TestHost {
    let ledger = Ledger::open(config).await.unwrap();
    let host = TestHost { ledger };
    let cache = host.ledger.recover(&host).await.unwrap();
    *host.ledger.cache.lock().await = cache;
    host
}
fn note(text: &str) -> TestAction {
    TestAction { note: text.into() }
}

#[tokio::test]
async fn open_writes_the_owner_and_refuses_another_budget_or_unowned_data() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), "owned.redb");
    let ledger = Ledger::<TestRecord, ()>::open(cfg.clone()).await.unwrap();
    let owner = ledger.db.get("owner").await.unwrap().data;
    assert_eq!(owner["schema"], 1);
    assert_eq!(owner["run_id"], "run-1");
    assert_eq!(owner["peer_id"], "peer-1");
    assert_eq!(owner["token_budget"], 100);
    assert_eq!(owner["memory_budget"], 100);
    drop(ledger);

    let mut other_run = cfg.clone();
    other_run.run_id = "run-2".into();
    assert_eq!(
        Ledger::<TestRecord, ()>::open(other_run)
            .await
            .unwrap_err()
            .code,
        "config"
    );
    let mut other_peer = cfg.clone();
    other_peer.peer_id = "peer-2".into();
    assert_eq!(
        Ledger::<TestRecord, ()>::open(other_peer)
            .await
            .unwrap_err()
            .code,
        "config"
    );
    let mut other_budget = cfg.clone();
    other_budget.memory_budget = 50;
    assert_eq!(
        Ledger::<TestRecord, ()>::open(other_budget)
            .await
            .unwrap_err()
            .code,
        "config"
    );

    let stray = dir.path().join("stray.redb");
    let db = Database::open_with(
        &stray,
        TestRecord::APP,
        OpenOptions::new().cache_size(1024 * 1024),
    )
    .unwrap();
    db.put("stray", json!({"x":1})).await.unwrap();
    drop(db);
    let mut unowned = cfg.clone();
    unowned.database_path = stray;
    let err = Ledger::<TestRecord, ()>::open(unowned).await.unwrap_err();
    assert_eq!(err.code, "storage");
    assert_eq!(err.message, "refusing an unowned nonempty database");
}

#[tokio::test]
async fn execute_is_idempotent_sequences_receipts_and_refuses_over_budget() {
    let dir = tempfile::tempdir().unwrap();
    let host = opened(config(dir.path(), "exec.redb")).await;
    let action = note("kept");
    let first = host
        .ledger
        .execute(&host, "op", action.clone(), 10, 1)
        .await
        .unwrap();
    let again = host
        .ledger
        .execute(&host, "op", action, 99, 1)
        .await
        .unwrap();
    assert_eq!(again.id, first.id);
    assert_eq!(again.author_sequence, 1);
    assert_eq!(again.memory_tokens, 10);
    assert_eq!(again.memory_remaining, 90);
    let err = host
        .ledger
        .execute(&host, "op", note("other"), 10, 1)
        .await
        .unwrap_err();
    assert_eq!(err.code, "conflict");
    assert_eq!(err.message, "operation ID reused with another action");

    let second = host
        .ledger
        .execute(&host, "next", note("second"), 10, 1)
        .await
        .unwrap();
    assert_eq!(second.author_sequence, 2);
    assert_eq!(second.memory_remaining, 80);

    let too_wide = host
        .ledger
        .execute(&host, "wide", note("wide"), 41, 1)
        .await
        .unwrap_err();
    assert_eq!(too_wide.code, "budget_exhausted");
    assert_eq!(too_wide.message, "memory write allowance exceeded");

    host.ledger
        .execute(&host, "fill", note("fill"), 40, 1)
        .await
        .unwrap();
    let rest = host
        .ledger
        .execute(&host, "rest", note("rest"), 40, 1)
        .await
        .unwrap();
    assert_eq!(rest.memory_remaining, 0);
    let over = host
        .ledger
        .execute(&host, "over", note("over"), 30, 1)
        .await
        .unwrap_err();
    assert_eq!(over.code, "budget_exhausted");
    assert_eq!(over.message, "memory write allowance exceeded");
}

#[tokio::test]
async fn reopen_restores_allowances_sequences_and_grants() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), "recover.redb");
    let host = opened(cfg.clone()).await;
    host.ledger
        .execute(&host, "op", note("saved"), 15, 1)
        .await
        .unwrap();
    host.ledger
        .reserve_inference(&host, "gen", 40)
        .await
        .unwrap();
    host.ledger
        .grant_writing_allowance(&host, 150, "operator", "raise")
        .await
        .unwrap();
    let restored = host.ledger.recover(&host).await.unwrap();
    let documents = restored.documents;
    let memory_charged = restored.memory_charged;
    let local_sequence = restored.local_sequence;
    let inference_charged = restored.inference_charged;
    let pending_tokens = restored.pending_tokens;
    let writing_grants = restored.writing_grants;
    let memory_limit = restored.memory_limit;
    assert_eq!(memory_charged, 15);
    assert_eq!(local_sequence, 1);
    assert_eq!(inference_charged, 40);
    assert_eq!(pending_tokens, 40);
    assert_eq!(writing_grants, 1);
    assert_eq!(memory_limit, 150);
    drop(host);

    let reopened = opened(cfg).await;
    let again = reopened.ledger.recover(&reopened).await.unwrap();
    assert_eq!(again.documents, documents);
    assert_eq!(again.memory_charged, memory_charged);
    assert_eq!(again.local_sequence, local_sequence);
    assert_eq!(again.inference_charged, inference_charged);
    assert_eq!(again.pending_tokens, pending_tokens);
    assert_eq!(again.writing_grants, writing_grants);
    assert_eq!(again.memory_limit, memory_limit);
}

#[tokio::test]
async fn inference_reserve_conflicts_settles_and_refuses_over_budget() {
    let dir = tempfile::tempdir().unwrap();
    let host = opened(config(dir.path(), "infer.redb")).await;
    let id = host
        .ledger
        .reserve_inference(&host, "gen", 30)
        .await
        .unwrap();
    let again = host
        .ledger
        .reserve_inference(&host, "gen", 30)
        .await
        .unwrap_err();
    assert_eq!(again.code, "conflict");
    host.ledger.settle_inference(&host, &id, 10).await.unwrap();
    let cache = host.ledger.cache.lock().await;
    assert_eq!(cache.inference_charged, 10);
    assert_eq!(cache.pending_tokens, 0);
    drop(cache);
    let over = host
        .ledger
        .reserve_inference(&host, "huge", 1_000)
        .await
        .unwrap_err();
    assert_eq!(over.code, "budget_exhausted");
    assert_eq!(over.message, "durable peer inference allowance exhausted");
}

#[tokio::test]
async fn writing_grant_must_rise_and_repeats_only_with_the_same_attribution() {
    let dir = tempfile::tempdir().unwrap();
    let host = opened(config(dir.path(), "grant.redb")).await;
    let flat = host
        .ledger
        .grant_writing_allowance(&host, 100, "operator", "same")
        .await
        .unwrap_err();
    assert_eq!(flat.code, "invalid");
    assert_eq!(
        flat.message,
        "writing grant must increase the cumulative limit"
    );
    let receipt = host
        .ledger
        .grant_writing_allowance(&host, 180, "operator", "raise")
        .await
        .unwrap();
    let repeat = host
        .ledger
        .grant_writing_allowance(&host, 180, "operator", "raise")
        .await
        .unwrap();
    assert_eq!(repeat, receipt);
    let other = host
        .ledger
        .grant_writing_allowance(&host, 180, "operator", "different")
        .await
        .unwrap_err();
    assert_eq!(other.code, "conflict");
    assert_eq!(
        other.message,
        "writing grant already has different attribution"
    );
}

#[tokio::test]
async fn changes_import_refuses_own_author_conflict_and_oversize_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut source_cfg = config(dir.path(), "source.redb");
    source_cfg.peer_id = "peer-a".into();
    let mut dest_cfg = config(dir.path(), "dest.redb");
    dest_cfg.peer_id = "peer-b".into();
    let source = opened(source_cfg).await;
    let dest = opened(dest_cfg).await;
    source
        .ledger
        .execute(&source, "shared", note("hello"), 5, 1)
        .await
        .unwrap();
    let page = source.ledger.changes(0, None).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(
        dest.ledger
            .import(&dest, page.records.clone())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        dest.ledger
            .import(&dest, page.records.clone())
            .await
            .unwrap(),
        0
    );

    let TestRecord::Action(mut changed) = page.records[0].clone() else {
        panic!("action record");
    };
    changed.action.note = "rewritten".into();
    let conflict = dest
        .ledger
        .import(&dest, vec![TestRecord::Action(changed)])
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "conflict");
    assert_eq!(
        conflict.message,
        "immutable record ID has different content"
    );

    let own_id = action_id(&dest.ledger.config.run_id, "peer-b", "mine");
    let own = TestRecord::Action(ActionRecord {
        id: own_id,
        schema: 1,
        run_id: dest.ledger.config.run_id.clone(),
        author: "peer-b".into(),
        operation_id: "mine".into(),
        author_sequence: 1,
        created_at_ms: 1,
        action: note("local"),
        memory_tokens: 1,
        memory_remaining: 99,
    });
    let refused = dest.ledger.import(&dest, vec![own]).await.unwrap_err();
    assert_eq!(refused.code, "conflict");
    assert_eq!(
        refused.message,
        "remote peer cannot create this peer's own records"
    );

    let mut bulk = Vec::new();
    for n in 0..17 {
        let operation = format!("bulk-{n}");
        bulk.push(TestRecord::Action(ActionRecord {
            id: action_id(&source.ledger.config.run_id, "peer-a", &operation),
            schema: 1,
            run_id: source.ledger.config.run_id.clone(),
            author: "peer-a".into(),
            operation_id: operation,
            author_sequence: n + 1,
            created_at_ms: 1,
            action: note("bulk"),
            memory_tokens: 1,
            memory_remaining: 99,
        }));
    }
    let limited = dest.ledger.import(&dest, bulk).await.unwrap_err();
    assert_eq!(limited.code, "limit");
    assert_eq!(
        limited.message,
        "at most sixteen records per replication page"
    );

    let kept = serde_json::to_value(&page.records[0]).unwrap();
    let mut newer = kept.clone();
    newer["data"]["schema"] = json!(99);
    #[derive(Deserialize)]
    struct Wrapped {
        #[serde(deserialize_with = "decode_page")]
        records: Vec<TestRecord>,
    }
    let decoded: Wrapped = serde_json::from_value(json!({"records":[kept, newer]})).unwrap();
    assert_eq!(decoded.records.len(), 1);
}

#[tokio::test]
async fn max_documents_refuses_with_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), "full.redb");
    cfg.max_documents = 0;
    let host = opened(cfg).await;
    let err = host
        .ledger
        .execute(&host, "op", note("no room"), 1, 1)
        .await
        .unwrap_err();
    assert_eq!(err.code, "capacity");
    assert_eq!(err.message, "database document allowance exhausted");
}
