use super::*;
use rouchdb::{ChangesOptions, Seq};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(serialize = "R: LedgerRecord", deserialize = "R: LedgerRecord"))]
pub struct ChangesPage<R> {
    pub database_id: String,
    #[serde(deserialize_with = "super::decode_page")]
    pub records: Vec<R>,
    pub last_seq: u64,
    pub has_more: bool,
}

impl<R: LedgerRecord, X: Default + Send + 'static> Ledger<R, X> {
    pub async fn database_id(&self) -> Result<String> {
        Ok(digest(
            self.db.adapter().id().await.map_err(storage)?.as_bytes(),
        ))
    }

    pub async fn changes(
        &self,
        since: u64,
        known_database: Option<&str>,
    ) -> Result<ChangesPage<R>> {
        let database_id = self.database_id().await?;
        let since = if known_database == Some(database_id.as_str()) {
            since
        } else {
            0
        };
        let page = self
            .db
            .changes(ChangesOptions {
                since: Seq::Num(since),
                limit: Some(16),
                include_docs: true,
                ..Default::default()
            })
            .await
            .map_err(storage)?;
        let has_more = page.results.len() == 16;
        let mut records = Vec::new();
        for entry in page.results {
            if entry.id.starts_with("r:") {
                if entry.deleted {
                    return Err(Error::new(
                        "storage",
                        "immutable research record was deleted",
                    ));
                }
                records.push(decode(
                    &entry
                        .doc
                        .ok_or_else(|| Error::new("storage", "missing changed document"))?,
                )?);
            }
        }
        Ok(ChangesPage {
            database_id,
            records,
            last_seq: page.last_seq.as_num(),
            has_more,
        })
    }

    pub async fn import<H: Host<R, X>>(
        &self,
        host: &H,
        records: Vec<R>,
    ) -> std::result::Result<usize, H::Error> {
        if records.is_empty() {
            return Ok(0);
        }
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            let ledger = owner.ledger().clone();
            ledger.import_owned(&owner, records).await
        })
        .await
    }

    async fn import_owned<H: Host<R, X>>(
        &self,
        host: &H,
        records: Vec<R>,
    ) -> std::result::Result<usize, H::Error> {
        if records.len() > 16 {
            return Err(Error::new("limit", "at most sixteen records per replication page").into());
        }
        // Validate shapes before any writes. References are checked in source
        // commit order. A partial prefix is safe to retry; callers do not
        // advance their cursor until the whole page succeeds.
        for record in &records {
            record.validate(&self.config.run_id)?;
        }
        let mut cache = self.cache.lock().await;
        let mut added = 0;
        for record in records {
            let id = record.id().to_string();
            if let Some(old) = self.get_value(&id).await? {
                if serde_json::to_value(decode::<R>(&old)?)
                    .map_err(|_| Error::new("storage", "invalid existing record"))?
                    != serde_json::to_value(&record)
                        .map_err(|_| Error::new("invalid", "invalid incoming record"))?
                {
                    return Err(Error::new(
                        "conflict",
                        "immutable record ID has different content",
                    )
                    .into());
                }
                continue;
            }
            if record.author() == self.config.peer_id {
                return Err(Error::new(
                    "conflict",
                    "remote peer cannot create this peer's own records",
                )
                .into());
            }
            host.validate_import(&record).await?;
            self.insert(&mut cache, &id, json!({"record": record}))
                .await?;
            host.observe(&mut cache.app, &record);
            added += 1;
        }
        Ok(added)
    }
}
