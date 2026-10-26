use super::log;
use super::{Error, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Application payload carried inside an [`ActionRecord`].
pub trait AppAction:
    Serialize + DeserializeOwned + Clone + PartialEq + Send + Sync + 'static
{
    fn validate_shape(&self) -> Result<()>;
    fn evidence_ids(&self) -> Vec<&str>;
    fn kind(&self) -> &'static str;
    fn is_content(&self) -> bool;
}

/// One immutable attributed record.
///
/// The wire contract the ledger relies on: a record serializes adjacently
/// tagged (`{"kind": …, "data": {…, "schema": …}}`, which [`decode_page`]
/// reads at `entry["data"]["schema"]`), and every record id starts with `r:`.
pub trait LedgerRecord: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const APP: &'static str;
    const MAX_KNOWN_SCHEMA: u32;
    fn id(&self) -> &str;
    fn author(&self) -> &str;
    fn schema(&self) -> u32;
    fn validate(&self, run: &str) -> Result<()>;
    fn check_id(id: &str) -> Result<()>;
    /// `(author_sequence, memory_tokens)` of an action record.
    fn receipt(&self) -> Option<(u64, u64)>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionRecord<A> {
    pub id: String,
    pub schema: u32,
    pub run_id: String,
    pub author: String,
    pub operation_id: String,
    pub author_sequence: u64,
    pub created_at_ms: u64,
    pub action: A,
    pub memory_tokens: u64,
    pub memory_remaining: u64,
}
impl<A: AppAction> ActionRecord<A> {
    pub fn validate_envelope(&self, run: &str) -> Result<()> {
        if self.run_id != run || self.id != action_id(run, &self.author, &self.operation_id) {
            return Err(Error::new("conflict", "action identity/run mismatch"));
        }
        identifier(&self.operation_id)?;
        self.action.validate_shape()?;
        if self.memory_tokens > 4096 || self.memory_remaining > 1_000_000 {
            return Err(Error::new("invalid", "invalid receipt bounds"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRecord<E> {
    pub id: String,
    pub schema: u32,
    pub run_id: String,
    pub observed_by: String,
    pub observed_at_ms: u64,
    pub evidence: E,
}
impl<E: Serialize> EvidenceRecord<E> {
    pub fn validate_envelope(&self, run: &str) -> Result<()> {
        if self.schema != 1
            || self.run_id != run
            || self.id != evidence_id(run, &self.observed_by, &self.evidence)
        {
            return Err(Error::new("conflict", "evidence identity/run mismatch"));
        }
        Ok(())
    }
}

pub fn check_record<R: LedgerRecord>(record: &R) -> Result<()> {
    R::check_id(record.id())?;
    identifier(record.author())?;
    if serde_json::to_vec(record)
        .map_err(|_| Error::new("invalid", "cannot serialize record"))?
        .len()
        > 96 * 1024
    {
        return Err(Error::new("limit", "record exceeds 96 KiB"));
    }
    Ok(())
}

pub fn action_id(run: &str, author: &str, operation: &str) -> String {
    format!(
        "r:a:{}",
        digest(&serde_json::to_vec(&(run, author, operation)).expect("identity"))
    )
}

pub fn evidence_id<E: Serialize>(run: &str, author: &str, evidence: &E) -> String {
    format!(
        "r:e:{}",
        digest(&serde_json::to_vec(&(run, author, evidence)).expect("evidence"))
    )
}

/// Decode a bounded exchange page, skipping only unsigned newer schemas.
pub fn decode_page<'de, D, R>(deserializer: D) -> std::result::Result<Vec<R>, D::Error>
where
    D: serde::Deserializer<'de>,
    R: LedgerRecord,
{
    let entries = Vec::<serde_json::Value>::deserialize(deserializer)?;
    if entries.len() > 16 {
        return Err(serde::de::Error::custom(
            "at most sixteen records per replication page",
        ));
    }
    let mut records = Vec::with_capacity(entries.len());
    let mut skipped = 0;
    for entry in entries {
        if entry["data"]["schema"]
            .as_u64()
            .is_some_and(|schema| schema > u64::from(R::MAX_KNOWN_SCHEMA))
        {
            skipped += 1;
        } else {
            records.push(serde_json::from_value(entry).map_err(serde::de::Error::custom)?);
        }
    }
    if skipped > 0 {
        let max_known = R::MAX_KNOWN_SCHEMA;
        log::line(format_args!(
            "{app} exchange skipped={skipped} reason=newer-schema max_known={max_known}",
            app = R::APP
        ));
    }
    Ok(records)
}

pub fn identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
    {
        return Err(Error::new(
            "invalid",
            "identifier must be 1–128 ASCII letters, digits, -, _, . or :",
        ));
    }
    Ok(())
}

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
