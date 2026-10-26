use super::*;
use rouchdb::AllDocsOptions;
use serde_json::{Value, json};

impl<R: LedgerRecord, X: Default + Send + 'static> Ledger<R, X> {
    pub async fn recover<H: Host<R, X>>(
        &self,
        host: &H,
    ) -> std::result::Result<Cache<X>, H::Error> {
        let mut cache = Cache {
            memory_limit: self.config.memory_budget,
            ..Cache::default()
        };
        let mut after: Option<String> = None;
        let mut refunds = 0u64;
        loop {
            let page = self
                .db
                .all_docs(AllDocsOptions {
                    start_key: after.clone(),
                    skip: u64::from(after.is_some()),
                    limit: Some(32),
                    include_docs: true,
                    ..Default::default()
                })
                .await
                .map_err(storage)?;
            if page.rows.is_empty() {
                break;
            }
            after = page.rows.last().map(|r| r.key.clone());
            for row in page.rows {
                cache.documents += 1;
                let value = row
                    .doc
                    .ok_or_else(|| Error::new("storage", "missing stored body"))?;
                if row.key.starts_with("r:") {
                    let record: R = decode(&value)?;
                    record.validate(&self.config.run_id)?;
                    if record.id() != row.key {
                        return Err(Error::new("storage", "document ID mismatch").into());
                    }
                    host.observe(&mut cache.app, &record);
                    if record.author() == self.config.peer_id
                        && let Some((author_sequence, memory_tokens)) = record.receipt()
                    {
                        cache.memory_charged = cache
                            .memory_charged
                            .checked_add(memory_tokens)
                            .ok_or_else(|| Error::new("storage", "memory accounting overflow"))?;
                        cache.local_sequence = cache.local_sequence.max(author_sequence);
                    }
                } else if row.key.starts_with("local:writing_allowance:") {
                    self.recover_writing_grant(&mut cache, &row.key, &value)?;
                } else if row.key.starts_with("local:reserve:") {
                    let amount = value
                        .get("amount")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Error::new("storage", "invalid reservation"))?;
                    cache.inference_charged = cache
                        .inference_charged
                        .checked_add(amount)
                        .ok_or_else(|| Error::new("storage", "inference accounting overflow"))?;
                    cache.pending_tokens = cache
                        .pending_tokens
                        .checked_add(amount)
                        .ok_or_else(|| Error::new("storage", "pending accounting overflow"))?;
                } else if row.key.starts_with("local:settle:") {
                    let reserve = value
                        .get("reservation_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::new("storage", "invalid settlement"))?;
                    let reservation = self
                        .get_value(reserve)
                        .await?
                        .ok_or_else(|| Error::new("storage", "settlement has no reservation"))?;
                    let amount = reservation
                        .get("amount")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Error::new("storage", "invalid reservation amount"))?;
                    let actual = value
                        .get("actual")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| Error::new("storage", "invalid actual usage"))?;
                    refunds = refunds
                        .checked_add(
                            amount
                                .checked_sub(actual)
                                .ok_or_else(|| Error::new("storage", "invalid refund"))?,
                        )
                        .ok_or_else(|| Error::new("storage", "refund overflow"))?;
                } else {
                    host.recover_local(&mut cache.app, &row.key, &value)?;
                }
            }
        }
        cache.inference_charged = cache
            .inference_charged
            .checked_sub(refunds)
            .ok_or_else(|| Error::new("storage", "negative inference charge"))?;
        // Second bounded pass counts settled reservations; no unbounded set of IDs.
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .db
                .all_docs(AllDocsOptions {
                    start_key: Some(cursor.clone().unwrap_or_else(|| "local:settle:".into())),
                    end_key: Some("local:settle;".into()),
                    skip: u64::from(cursor.is_some()),
                    limit: Some(32),
                    include_docs: true,
                    ..Default::default()
                })
                .await
                .map_err(storage)?;
            if page.rows.is_empty() {
                break;
            }
            cursor = page.rows.last().map(|r| r.key.clone());
            for row in page.rows {
                let value = row
                    .doc
                    .ok_or_else(|| Error::new("storage", "missing settlement"))?;
                let id = value
                    .get("reservation_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::new("storage", "missing reservation ID"))?;
                let amount = self
                    .get_value(id)
                    .await?
                    .and_then(|v| v.get("amount").and_then(Value::as_u64))
                    .ok_or_else(|| Error::new("storage", "missing reservation"))?;
                cache.pending_tokens = cache
                    .pending_tokens
                    .checked_sub(amount)
                    .ok_or_else(|| Error::new("storage", "invalid pending total"))?;
            }
        }
        if self
            .config
            .token_budget
            .is_some_and(|limit| cache.inference_charged > limit)
            || cache.memory_charged > cache.memory_limit
        {
            return Err(
                Error::new("storage", "persisted usage exceeds configured allowance").into(),
            );
        }
        Ok(cache)
    }

    /// A generation attempt is never repeated, including an uncertain attempt on reopen.
    pub async fn inference_reserved(&self, operation: &str) -> Result<bool> {
        identifier(operation)?;
        Ok(self
            .get_value(&key(
                "local:reserve",
                &self.config.run_id,
                &self.config.peer_id,
                operation,
            ))
            .await?
            .is_some())
    }

    pub async fn reserve_inference<H: Host<R, X>>(
        &self,
        host: &H,
        operation: &str,
        amount: u64,
    ) -> std::result::Result<String, H::Error> {
        identifier(operation)?;
        let operation = operation.to_owned();
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            owner
                .ledger()
                .clone()
                .reserve_inference_owned(&operation, amount)
                .await
                .map_err(H::Error::from)
        })
        .await
    }

    async fn reserve_inference_owned(&self, operation: &str, amount: u64) -> Result<String> {
        identifier(operation)?;
        let id = key(
            "local:reserve",
            &self.config.run_id,
            &self.config.peer_id,
            operation,
        );
        let mut cache = self.cache.lock().await;
        if self.get_value(&id).await?.is_some() {
            return Err(Error::new(
                "conflict",
                "inference operation already reserved; it must not be generated again",
            ));
        }
        if amount == 0
            || self
                .config
                .token_budget
                .is_some_and(|limit| amount > limit.saturating_sub(cache.inference_charged))
        {
            return Err(Error::new(
                "budget_exhausted",
                "durable peer inference allowance exhausted",
            ));
        }
        let charged = cache
            .inference_charged
            .checked_add(amount)
            .ok_or_else(|| Error::new("storage", "inference accounting overflow"))?;
        let pending = cache
            .pending_tokens
            .checked_add(amount)
            .ok_or_else(|| Error::new("storage", "pending accounting overflow"))?;
        self.insert(
            &mut cache,
            &id,
            json!({"operation_id":operation,"amount":amount}),
        )
        .await?;
        cache.inference_charged = charged;
        cache.pending_tokens = pending;
        Ok(id)
    }

    pub async fn settle_inference<H: Host<R, X>>(
        &self,
        host: &H,
        reservation_id: &str,
        actual: u64,
    ) -> std::result::Result<(), H::Error> {
        let reservation_id = reservation_id.to_owned();
        let host = host.clone();
        self.owned_write(host, move |owner| async move {
            owner
                .ledger()
                .clone()
                .settle_inference_owned(&reservation_id, actual)
                .await
                .map_err(H::Error::from)
        })
        .await
    }

    async fn settle_inference_owned(&self, reservation_id: &str, actual: u64) -> Result<()> {
        let mut cache = self.cache.lock().await;
        let reservation = self
            .get_value(reservation_id)
            .await?
            .ok_or_else(|| Error::new("missing_reference", "missing inference reservation"))?;
        let amount = reservation
            .get("amount")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::new("storage", "invalid reservation"))?;
        let operation = reservation
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("storage", "invalid operation"))?;
        if reservation_id
            != key(
                "local:reserve",
                &self.config.run_id,
                &self.config.peer_id,
                operation,
            )
            || actual > amount
        {
            return Err(Error::new("invalid", "invalid inference settlement"));
        }
        let id = key(
            "local:settle",
            &self.config.run_id,
            &self.config.peer_id,
            operation,
        );
        let value = json!({"reservation_id":reservation_id,"actual":actual});
        if let Some(old) = self.get_value(&id).await? {
            if old == value {
                return Ok(());
            }
            return Err(Error::new(
                "conflict",
                "inference settlement already differs",
            ));
        }
        self.insert(&mut cache, &id, value).await?;
        cache.inference_charged -= amount - actual;
        cache.pending_tokens -= amount;
        Ok(())
    }
}
