use forthebirds_store::{ChangesPage, LedgerRecord};
use serde::{Deserialize, Serialize};

/// One page sent to a peer, plus the cursor asking for their page in return.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(serialize = "R: LedgerRecord", deserialize = "R: LedgerRecord"))]
pub struct Exchange<R> {
    pub protocol: u32,
    pub run_id: String,
    pub peer_id: String,
    #[serde(deserialize_with = "forthebirds_store::decode_page")]
    pub records: Vec<R>,
    pub since: u64,
    pub database_id: Option<String>,
}

/// The page a peer returns for an [`Exchange`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(serialize = "R: LedgerRecord", deserialize = "R: LedgerRecord"))]
pub struct ExchangeReply<R> {
    pub protocol: u32,
    pub run_id: String,
    pub peer_id: String,
    pub page: ChangesPage<R>,
}
