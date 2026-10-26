//! Immutable attributed records in one local RouchDB database, with writing and inference allowances and replication pages.

mod error;
mod log;
mod record;
mod recover;
mod replication;
mod writing_allowance;

#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use record::{
    ActionRecord, AppAction, EvidenceRecord, LedgerRecord, action_id, check_record, decode_page,
    digest, evidence_id, identifier, now,
};
pub use replication::ChangesPage;

use rouchdb::{Database, OpenOptions, RouchError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Clone)]
pub struct LedgerConfig {
    pub database_path: PathBuf,
    pub database_cache_bytes: usize,
    pub run_id: String,
    pub peer_id: String,
    pub token_budget: Option<u64>,
    pub memory_budget: u64,
    pub memory_item_tokens: u64,
    pub max_documents: usize,
}

pub struct Ledger<R, X> {
    pub db: Arc<Database>,
    pub config: Arc<LedgerConfig>,
    pub cache: Arc<Mutex<Cache<X>>>,
    write_slots: Arc<Semaphore>,
    _record: PhantomData<fn() -> R>,
}
impl<R, X> std::fmt::Debug for Ledger<R, X> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl<R, X> Clone for Ledger<R, X> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            config: Arc::clone(&self.config),
            cache: Arc::clone(&self.cache),
            write_slots: Arc::clone(&self.write_slots),
            _record: PhantomData,
        }
    }
}

#[derive(Default)]
pub struct Cache<X> {
    pub documents: u64,
    pub inference_charged: u64,
    pub pending_tokens: u64,
    pub memory_charged: u64,
    pub memory_limit: u64,
    pub writing_grants: u64,
    pub local_sequence: u64,
    pub poisoned: bool,
    pub app: X,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<R> {
    pub records: Vec<R>,
    pub next_cursor: Option<String>,
}

pub trait Host<R: LedgerRecord, X>: Clone + Send + Sync + 'static {
    type Error: From<Error> + Send + 'static;
    fn ledger(&self) -> &Ledger<R, X>;
    fn code(error: &Self::Error) -> &str;
    fn validate_import(
        &self,
        record: &R,
    ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;
    fn observe(&self, app: &mut X, record: &R);
    fn recover_local(
        &self,
        app: &mut X,
        key: &str,
        value: &Value,
    ) -> std::result::Result<(), Self::Error>;
}

impl<R: LedgerRecord, X: Default + Send + 'static> Ledger<R, X> {
    pub async fn open(config: LedgerConfig) -> Result<Self> {
        let path = config.database_path.clone();
        let cache_bytes = config.database_cache_bytes;
        let db = tokio::task::spawn_blocking(move || {
            Database::open_with(path, R::APP, OpenOptions::new().cache_size(cache_bytes))
        })
        .await
        .map_err(|_| Error::new("storage", "database opener failed"))?
        .map_err(storage)?;
        let owner = json!({"schema":1,"run_id":config.run_id,"peer_id":config.peer_id,"token_budget":config.token_budget,"memory_budget":config.memory_budget});
        match db.get("owner").await {
            Ok(doc) => {
                if doc.data != owner {
                    return Err(Error::new(
                        "config",
                        "database belongs to different run/peer/budget settings",
                    ));
                }
            }
            Err(RouchError::NotFound(_)) => {
                if db.info().await.map_err(storage)?.doc_count != 0 {
                    return Err(Error::new(
                        "storage",
                        "refusing an unowned nonempty database",
                    ));
                }
                db.put("owner", owner).await.map_err(storage)?;
            }
            Err(e) => return Err(storage(e)),
        }
        Ok(Self::with_database(db, config))
    }

    pub fn with_database(db: Database, config: LedgerConfig) -> Self {
        Self {
            db: Arc::new(db),
            config: Arc::new(config),
            cache: Arc::new(Mutex::new(Cache::default())),
            write_slots: Arc::new(Semaphore::new(8)),
            _record: PhantomData,
        }
    }

    pub async fn owned_write<H, T, F, Fut>(
        &self,
        host: H,
        operation: F,
    ) -> std::result::Result<T, H::Error>
    where
        H: Host<R, X>,
        T: Send + 'static,
        F: FnOnce(H) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = std::result::Result<T, H::Error>> + Send + 'static,
    {
        let permit = self.write_slots.clone().try_acquire_owned().map_err(|_| {
            H::Error::from(Error::new(
                "capacity",
                "eight database mutations are already admitted",
            ))
        })?;
        // Dropping the caller's JoinHandle detaches, rather than cancels, this
        // bounded owner task. It retains admission and the accounting lock
        // until commit/readback and all in-memory counters are settled.
        match tokio::spawn(async move {
            let _permit = permit;
            let result = operation(host).await;
            if let Err(error) = &result
                && H::code(error) == "storage"
            {
                log::line(format_args!(
                    "{app} storage write failed; inspect store.write_blocked before resuming",
                    app = R::APP
                ));
            }
            result
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.cache.lock().await.poisoned = true;
                Err(H::Error::from(Error::new(
                    "storage",
                    "database owner task failed; reopen to recover",
                )))
            }
        }
    }

    pub async fn drain_writes(&self) -> Result<()> {
        let _all = self
            .write_slots
            .clone()
            .acquire_many_owned(8)
            .await
            .map_err(|_| Error::new("storage", "database write admission closed"))?;
        Ok(())
    }

    pub async fn get_value(&self, id: &str) -> Result<Option<Value>> {
        match self.db.get(id).await {
            Ok(doc) => Ok(Some(doc.data)),
            Err(RouchError::NotFound(_)) => Ok(None),
            Err(e) => Err(storage(e)),
        }
    }

    pub async fn get(&self, id: &str) -> Result<R> {
        R::check_id(id)?;
        let value = self
            .get_value(id)
            .await?
            .ok_or_else(|| Error::new("missing_reference", "research object does not exist"))?;
        decode(&value)
    }

    pub async fn page(&self, after: Option<&str>, limit: usize) -> Result<Page<R>> {
        if !(1..=32).contains(&limit) {
            return Err(Error::new("invalid", "page limit must be 1–32"));
        }
        if let Some(id) = after {
            R::check_id(id)?;
        }
        let response = self
            .db
            .all_docs(rouchdb::AllDocsOptions {
                start_key: Some(after.unwrap_or("r:").into()),
                end_key: Some("r;".into()),
                skip: u64::from(after.is_some()),
                limit: Some(limit as u64),
                include_docs: true,
                ..Default::default()
            })
            .await
            .map_err(storage)?;
        let next_cursor = if response.rows.len() == limit {
            response.rows.last().map(|r| r.key.clone())
        } else {
            None
        };
        let records = response
            .rows
            .into_iter()
            .map(|r| {
                decode(
                    &r.doc
                        .ok_or_else(|| Error::new("storage", "missing document body"))?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            records,
            next_cursor,
        })
    }

    pub async fn save_evidence<H, E>(
        &self,
        host: &H,
        evidence: E,
    ) -> std::result::Result<EvidenceRecord<E>, H::Error>
    where
        H: Host<R, X>,
        E: Serialize + Clone + Send + 'static,
        R: From<EvidenceRecord<E>>,
        EvidenceRecord<E>: TryFrom<R> + Clone,
    {
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            owner
                .ledger()
                .clone()
                .save_evidence_owned(evidence)
                .await
                .map_err(H::Error::from)
        })
        .await
    }

    async fn save_evidence_owned<E>(&self, evidence: E) -> Result<EvidenceRecord<E>>
    where
        E: Serialize + Clone,
        R: From<EvidenceRecord<E>>,
        EvidenceRecord<E>: TryFrom<R> + Clone,
    {
        let id = evidence_id(&self.config.run_id, &self.config.peer_id, &evidence);
        let mut cache = self.cache.lock().await;
        if let Some(old) = self.get_value(&id).await? {
            let record: R = decode(&old)?;
            return match EvidenceRecord::<E>::try_from(record) {
                Ok(found) => Ok(found),
                Err(_) => Err(Error::new("storage", "wrong evidence type")),
            };
        }
        let record = EvidenceRecord {
            id,
            schema: 1,
            run_id: self.config.run_id.clone(),
            observed_by: self.config.peer_id.clone(),
            observed_at_ms: now(),
            evidence,
        };
        let public = R::from(record.clone());
        public.validate(&self.config.run_id)?;
        self.insert(&mut cache, &record.id, json!({"record": public}))
            .await?;
        Ok(record)
    }

    pub async fn existing_action<A>(&self, id: &str, action: &A) -> Result<Option<ActionRecord<A>>>
    where
        A: AppAction,
        ActionRecord<A>: TryFrom<R>,
    {
        let Some(value) = self.get_value(id).await? else {
            return Ok(None);
        };
        let record: R = decode(&value)?;
        match ActionRecord::<A>::try_from(record) {
            Ok(old) if &old.action == action => Ok(Some(old)),
            _ => Err(Error::new(
                "conflict",
                "operation ID reused with another action",
            )),
        }
    }

    pub async fn append_action<A>(
        &self,
        cache: &mut Cache<X>,
        id: String,
        schema: u32,
        operation: &str,
        action: A,
        memory_tokens: u64,
    ) -> Result<ActionRecord<A>>
    where
        A: AppAction,
        R: From<ActionRecord<A>>,
    {
        if memory_tokens > self.config.memory_item_tokens
            || memory_tokens > cache.memory_limit.saturating_sub(cache.memory_charged)
        {
            return Err(Error::new(
                "budget_exhausted",
                "memory write allowance exceeded",
            ));
        }
        let sequence = cache
            .local_sequence
            .checked_add(1)
            .ok_or_else(|| Error::new("storage", "sequence overflow"))?;
        let record = ActionRecord {
            id,
            schema,
            run_id: self.config.run_id.clone(),
            author: self.config.peer_id.clone(),
            operation_id: operation.into(),
            author_sequence: sequence,
            created_at_ms: now(),
            action,
            memory_tokens,
            memory_remaining: cache.memory_limit - cache.memory_charged - memory_tokens,
        };
        let public = R::from(record.clone());
        public.validate(&self.config.run_id)?;
        self.insert(cache, &record.id, json!({"record": public}))
            .await?;
        cache.local_sequence = sequence;
        cache.memory_charged += memory_tokens;
        Ok(record)
    }

    pub async fn execute<H, A>(
        &self,
        host: &H,
        operation: &str,
        action: A,
        memory_tokens: u64,
        schema: u32,
    ) -> std::result::Result<ActionRecord<A>, H::Error>
    where
        H: Host<R, X>,
        A: AppAction,
        R: From<ActionRecord<A>>,
        ActionRecord<A>: TryFrom<R>,
    {
        identifier(operation)?;
        let operation = operation.to_owned();
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            action.validate_shape().map_err(H::Error::from)?;
            let ledger = owner.ledger().clone();
            let id = action_id(&ledger.config.run_id, &ledger.config.peer_id, &operation);
            let mut cache = ledger.cache.lock().await;
            if let Some(old) = ledger
                .existing_action(&id, &action)
                .await
                .map_err(H::Error::from)?
            {
                return Ok(old);
            }
            ledger
                .append_action(&mut cache, id, schema, &operation, action, memory_tokens)
                .await
                .map_err(H::Error::from)
        })
        .await
    }

    pub async fn insert(&self, cache: &mut Cache<X>, id: &str, value: Value) -> Result<()> {
        if cache.poisoned {
            return Err(Error::new(
                "storage",
                "writes blocked after an uncertain database outcome; reopen to recover",
            ));
        }
        if cache.documents >= self.config.max_documents as u64 {
            return Err(Error::new(
                "capacity",
                "database document allowance exhausted",
            ));
        }
        cache.poisoned = true;
        if let Err(error) = self.db.put(id, value.clone()).await {
            // A commit may have succeeded before a post-write error. A readback
            // of the exact immutable value resolves it; otherwise stop writes.
            match self.get_value(id).await {
                Ok(Some(saved)) if saved == value => {}
                _ => {
                    cache.poisoned = true;
                    return Err(storage(error));
                }
            }
        }
        cache.documents += 1;
        cache.poisoned = false;
        Ok(())
    }
}

pub fn storage(error: RouchError) -> Error {
    let code = match error {
        RouchError::Conflict => "conflict",
        _ => "storage",
    };
    Error::new(code, "database operation failed")
}

pub fn decode<T: DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::from_value(
        value
            .get("record")
            .cloned()
            .ok_or_else(|| Error::new("storage", "missing record envelope"))?,
    )
    .map_err(|_| Error::new("storage", "invalid persisted research record"))
}

pub fn key(prefix: &str, run: &str, peer: &str, op: &str) -> String {
    format!(
        "{prefix}:{}",
        digest(&serde_json::to_vec(&(run, peer, op)).expect("key"))
    )
}
