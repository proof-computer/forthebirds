use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WritingGrant {
    schema: u8,
    run_id: String,
    peer_id: String,
    previous_limit: u64,
    limit: u64,
    actor: String,
    reason: String,
    granted_at_ms: u64,
}
fn grant_key(limit: u64) -> String {
    format!("local:writing_allowance:{limit:020}")
}

impl<R: LedgerRecord, X: Default + Send + 'static> Ledger<R, X> {
    /// Read-only operator plan; the mutation rechecks the same conditions under its lock.
    pub async fn preview_writing_allowance(
        &self,
        limit: u64,
        actor: &str,
        reason: &str,
    ) -> Result<Value> {
        identifier(actor)?;
        if !(1..=1_000_000).contains(&limit) || reason.trim().is_empty() || reason.len() > 512 {
            return Err(Error::new(
                "invalid",
                "invalid writing grant limit or reason",
            ));
        }
        let cache = self.cache.lock().await;
        if let Some(old) = self.get_value(&grant_key(limit)).await? {
            let receipt = old
                .get("writing_grant")
                .ok_or_else(|| Error::new("storage", "missing writing grant envelope"))?;
            if receipt["actor"] != actor || receipt["reason"] != reason {
                return Err(Error::new(
                    "conflict",
                    "writing grant already has different attribution",
                ));
            }
            return Ok(
                json!({"already_applied":true,"receipt":receipt,"effective_limit":cache.memory_limit,"charged":cache.memory_charged}),
            );
        }
        if limit <= cache.memory_limit {
            return Err(Error::new(
                "invalid",
                "writing grant must increase the cumulative limit",
            ));
        }
        Ok(
            json!({"already_applied":false,"previous_limit":cache.memory_limit,"limit":limit,"charged":cache.memory_charged,"remaining_after":limit.saturating_sub(cache.memory_charged),"actor":actor,"reason":reason}),
        )
    }

    /// Operator-only offline mutation; deliberately absent from model tools and exchange.
    pub async fn grant_writing_allowance<H: Host<R, X>>(
        &self,
        host: &H,
        limit: u64,
        actor: &str,
        reason: &str,
    ) -> std::result::Result<Value, H::Error> {
        identifier(actor)?;
        if !(1..=1_000_000).contains(&limit) || reason.trim().is_empty() || reason.len() > 512 {
            return Err(Error::new("invalid", "invalid writing grant limit or reason").into());
        }
        let actor = actor.to_owned();
        let reason = reason.to_owned();
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            let ledger = owner.ledger().clone();
            let mut cache = ledger.cache.lock().await;
            let key = grant_key(limit);
            if let Some(old) = ledger.get_value(&key).await? {
                let receipt = old
                    .get("writing_grant")
                    .cloned()
                    .ok_or_else(|| Error::new("storage", "missing writing grant envelope"))?;
                if receipt["actor"] == actor && receipt["reason"] == reason {
                    return Ok(receipt);
                }
                return Err(Error::new(
                    "conflict",
                    "writing grant already has different attribution",
                )
                .into());
            }
            if limit <= cache.memory_limit {
                return Err(Error::new(
                    "invalid",
                    "writing grant must increase the cumulative limit",
                )
                .into());
            }
            let grant = WritingGrant {
                schema: 1,
                run_id: ledger.config.run_id.clone(),
                peer_id: ledger.config.peer_id.clone(),
                previous_limit: cache.memory_limit,
                limit,
                actor,
                reason,
                granted_at_ms: now(),
            };
            let value = serde_json::to_value(grant).expect("writing grant serialization");
            ledger
                .insert(&mut cache, &key, json!({"writing_grant": value.clone()}))
                .await?;
            cache.memory_limit = limit;
            cache.writing_grants += 1;
            Ok(value)
        })
        .await
    }

    pub fn recover_writing_grant(
        &self,
        cache: &mut Cache<X>,
        key: &str,
        value: &Value,
    ) -> Result<()> {
        let grant: WritingGrant = serde_json::from_value(
            value
                .get("writing_grant")
                .cloned()
                .ok_or_else(|| Error::new("storage", "missing writing grant envelope"))?,
        )
        .map_err(|_| Error::new("storage", "invalid writing allowance grant"))?;
        if grant.schema != 1
            || grant.run_id != self.config.run_id
            || grant.peer_id != self.config.peer_id
            || key != grant_key(grant.limit)
            || grant.previous_limit != cache.memory_limit
            || grant.limit <= grant.previous_limit
            || grant.limit > 1_000_000
            || identifier(&grant.actor).is_err()
            || grant.reason.trim().is_empty()
            || grant.reason.len() > 512
            || grant.granted_at_ms == 0
        {
            return Err(Error::new(
                "storage",
                "invalid writing allowance grant chain",
            ));
        }
        cache.memory_limit = grant.limit;
        cache.writing_grants += 1;
        Ok(())
    }
}
