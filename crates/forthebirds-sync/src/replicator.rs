use crate::exchange::{Exchange, ExchangeReply};
use crate::{BODY_LIMIT, CATCH_UP_PAGES, PROTOCOL, ROUTE};
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use forthebirds_net::{PeerTarget, PinnedClient, pinned_client};
use forthebirds_store::{Error as StoreError, Host, LedgerRecord};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, watch};

/// Identity and bearer token for one local peer.
#[derive(Clone)]
pub struct SyncConfig {
    pub run_id: String,
    pub peer_id: String,
    pub token: String,
    pub request_timeout: Duration,
}

/// Last attempt against one named target.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LinkStatus {
    pub last_attempt_ms: u64,
    pub last_success_ms: Option<u64>,
    pub error: Option<&'static str>,
    /// Attempt time when a catch-up finished because neither side had more
    /// pages. Unchanged when the page cap or an error stops the attempt.
    pub caught_up_ms: Option<u64>,
}

/// Values a consumer passes to `forthebirds_net::publish_summary`.
#[derive(Clone, Debug)]
pub struct TickSummary {
    pub last_success_ms: Option<u64>,
    /// `"none"` when every target succeeded.
    pub failure_code: &'static str,
}

#[derive(Clone, Default)]
struct Cursor {
    push: u64,
    pull: u64,
    remote_database: Option<String>,
}

struct Inner<H, R, X> {
    host: H,
    config: SyncConfig,
    received: Arc<Notify>,
    cursors: Mutex<BTreeMap<String, Cursor>>,
    clients: Mutex<BTreeMap<String, PinnedClient>>,
    links: Mutex<BTreeMap<String, LinkStatus>>,
    _marker: PhantomData<fn() -> (R, X)>,
}

/// Push-pull loop over store pages and pinned peer targets.
///
/// Clones share the cursors, clients and link map.
pub struct Replicator<H, R, X> {
    inner: Arc<Inner<H, R, X>>,
}

impl<H, R, X> Clone for Replicator<H, R, X> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<H, R, X> Replicator<H, R, X> {
    /// The run this replicator serves.
    pub fn run_id(&self) -> &str {
        &self.inner.config.run_id
    }

    pub(crate) fn peer_id(&self) -> &str {
        &self.inner.config.peer_id
    }

    pub(crate) fn token(&self) -> &str {
        &self.inner.config.token
    }
}

impl<H, R, X> Replicator<H, R, X>
where
    H: Host<R, X>,
    R: LedgerRecord,
    X: Default + Send + 'static,
{
    /// Refuse a `run_id` or `peer_id` that is not an identifier, or a token
    /// that is not 32–512 visible ASCII bytes. The refusal code is `config`.
    pub fn new(host: H, config: SyncConfig) -> forthebirds_store::Result<Self> {
        if forthebirds_store::identifier(&config.run_id).is_err()
            || forthebirds_store::identifier(&config.peer_id).is_err()
        {
            return Err(StoreError::new(
                "config",
                "run_id and peer_id must be identifiers",
            ));
        }
        if !(32..=512).contains(&config.token.len())
            || !config.token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(StoreError::new(
                "config",
                "token must be 32–512 visible ASCII bytes",
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                host,
                config,
                received: Arc::new(Notify::new()),
                cursors: Mutex::new(BTreeMap::new()),
                clients: Mutex::new(BTreeMap::new()),
                links: Mutex::new(BTreeMap::new()),
                _marker: PhantomData,
            }),
        })
    }

    /// `POST` [`ROUTE`] with a body limit of [`BODY_LIMIT`].
    pub fn router(&self) -> Router {
        let replicator = self.clone();
        Router::new()
            .route(
                ROUTE,
                post(move |headers: HeaderMap, body: axum::body::Bytes| {
                    let replicator = replicator.clone();
                    async move { handle_exchange(replicator, headers, body).await }
                }),
            )
            .layer(DefaultBodyLimit::max(BODY_LIMIT))
    }

    /// Import `incoming` and answer with the local page it asked for.
    pub async fn exchange(&self, incoming: Exchange<R>) -> Result<ExchangeReply<R>, H::Error> {
        if incoming.protocol != PROTOCOL {
            return Err(fail(
                "upgrade_required",
                "mixed-protocol replication refused; peers require protocol 1",
            ));
        }
        if incoming.run_id != self.inner.config.run_id
            || incoming.peer_id == self.inner.config.peer_id
        {
            return Err(fail("conflict", "peer protocol/run/identity mismatch"));
        }
        forthebirds_store::identifier(&incoming.peer_id)?;
        let added = self
            .inner
            .host
            .ledger()
            .import(&self.inner.host, incoming.records)
            .await?;
        if added > 0 {
            self.inner.received.notify_one();
        }
        let page = self
            .inner
            .host
            .ledger()
            .changes(incoming.since, incoming.database_id.as_deref())
            .await?;
        Ok(ExchangeReply {
            protocol: PROTOCOL,
            run_id: self.inner.config.run_id.clone(),
            peer_id: self.inner.config.peer_id.clone(),
            page,
        })
    }

    /// Exchange one page with `target`. `Ok(true)` means either side has more.
    ///
    /// The cursor advances only after the reply's page imports.
    pub async fn sync_once(&self, target: &PeerTarget) -> Result<bool, H::Error> {
        let key = target.cursor_key();
        let client = self.client_for(&key, target).await?;
        let mut cursors = self.inner.cursors.lock().await;
        let cursor = cursors.entry(key).or_default();
        let own_id = self.inner.host.ledger().database_id().await?;
        let outgoing = self
            .inner
            .host
            .ledger()
            .changes(cursor.push, Some(&own_id))
            .await?;
        let request = Exchange {
            protocol: PROTOCOL,
            run_id: self.inner.config.run_id.clone(),
            peer_id: self.inner.config.peer_id.clone(),
            records: outgoing.records,
            since: cursor.pull,
            database_id: cursor.remote_database.clone(),
        };
        let url = format!("{url}api/sync/v1/exchange", url = target.url);
        let body = serde_json::to_vec(&request)
            .map_err(|_| fail::<H::Error>("exchange", "cannot encode exchange"))?;
        let response = client
            .client
            .post(url)
            .bearer_auth(&self.inner.config.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| carry::<H::Error>(&client.request_error(&error)))?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(fail("auth", "peer rejected bearer token"));
        }
        if !status.is_success() {
            return Err(fail("exchange", "peer rejected exchange"));
        }
        let bytes = read_limited(response, BODY_LIMIT)
            .await
            .map_err(|_| fail::<H::Error>("exchange", "invalid peer response body"))?;
        let incoming: ExchangeReply<R> = serde_json::from_slice(&bytes)
            .map_err(|_| fail::<H::Error>("exchange", "invalid peer response"))?;
        if incoming.protocol != PROTOCOL
            || incoming.run_id != self.inner.config.run_id
            || incoming.peer_id != target.name
        {
            return Err(fail("identity", "peer response identity mismatch"));
        }
        let added = self
            .inner
            .host
            .ledger()
            .import(&self.inner.host, incoming.page.records)
            .await?;
        if added > 0 {
            self.inner.received.notify_one();
        }
        cursor.push = outgoing.last_seq;
        cursor.pull = incoming.page.last_seq;
        cursor.remote_database = Some(incoming.page.database_id);
        Ok(outgoing.has_more || incoming.page.has_more)
    }

    /// Drop stale cursors and clients, then catch up each live target.
    ///
    /// An `Err` target records its error code and is not contacted. A shutdown
    /// between pages returns the summary of targets already finished.
    pub async fn tick(
        &self,
        targets: BTreeMap<String, forthebirds_net::Result<PeerTarget>>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> TickSummary {
        let live_keys: BTreeSet<String> = targets
            .values()
            .filter_map(|target| target.as_ref().ok().map(PeerTarget::cursor_key))
            .collect();
        self.inner
            .cursors
            .lock()
            .await
            .retain(|key, _| live_keys.contains(key));
        self.inner
            .clients
            .lock()
            .await
            .retain(|key, _| live_keys.contains(key));
        self.inner
            .links
            .lock()
            .await
            .retain(|name, _| targets.contains_key(name));
        let mut failure_code = "none";
        if *shutdown.borrow() {
            return self.summary(failure_code).await;
        }
        for (name, target) in targets {
            if *shutdown.borrow() {
                break;
            }
            let outcome = match target {
                Err(error) => Err(error.code),
                Ok(target) => self.catch_up(&target, shutdown).await,
            };
            if outcome == Err("shutdown") {
                break;
            }
            let mut links = self.inner.links.lock().await;
            let entry = links.entry(name).or_default();
            entry.last_attempt_ms = forthebirds_store::now();
            match outcome {
                Ok(drained) => {
                    entry.last_success_ms = Some(entry.last_attempt_ms);
                    entry.error = None;
                    if drained {
                        entry.caught_up_ms = Some(entry.last_attempt_ms);
                    }
                }
                Err(code) => {
                    failure_code = code;
                    entry.error = Some(code);
                }
            }
        }
        self.summary(failure_code).await
    }

    /// Link status recorded by the latest ticks, keyed by target name.
    pub async fn links(&self) -> BTreeMap<String, LinkStatus> {
        self.inner.links.lock().await.clone()
    }

    /// Latest [`LinkStatus::caught_up_ms`] across links, if any link has drained.
    pub async fn caught_up_ms(&self) -> Option<u64> {
        self.inner
            .links
            .lock()
            .await
            .values()
            .filter_map(|link| link.caught_up_ms)
            .max()
    }

    /// Signalled when an import adds at least one record.
    pub fn received(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.received)
    }

    /// Call `targets` and [`Self::tick`] once per interval until `shutdown` is set.
    pub async fn run<F>(&self, every: Duration, mut targets: F, mut shutdown: watch::Receiver<bool>)
    where
        F: FnMut() -> BTreeMap<String, forthebirds_net::Result<PeerTarget>> + Send,
    {
        let mut interval = tokio::time::interval(every);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            }
            if *shutdown.borrow() {
                return;
            }
            self.tick(targets(), &mut shutdown).await;
        }
    }

    async fn client_for(&self, key: &str, target: &PeerTarget) -> Result<PinnedClient, H::Error> {
        let mut clients = self.inner.clients.lock().await;
        if !clients.contains_key(key) {
            let built = pinned_client(&target.tls_fingerprint, self.inner.config.request_timeout)
                .map_err(|error| carry::<H::Error>(&error))?;
            clients.insert(key.to_owned(), built);
        }
        Ok(clients.get(key).expect("client inserted").clone())
    }

    /// `Ok(true)` when neither side had more pages. `Ok(false)` when the page
    /// cap stopped the attempt with more left.
    async fn catch_up(
        &self,
        target: &PeerTarget,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool, &'static str> {
        for _ in 0..CATCH_UP_PAGES {
            if *shutdown.borrow() {
                return Err("shutdown");
            }
            match self.sync_once(target).await {
                Ok(true) => {}
                Ok(false) => return Ok(true),
                Err(error) => return Err(static_code(H::code(&error))),
            }
        }
        Ok(false)
    }

    async fn summary(&self, failure_code: &'static str) -> TickSummary {
        let links = self.inner.links.lock().await;
        TickSummary {
            last_success_ms: links.values().filter_map(|link| link.last_success_ms).max(),
            failure_code,
        }
    }
}

pub(crate) async fn handle_exchange<H, R, X>(
    replicator: Replicator<H, R, X>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response
where
    H: Host<R, X>,
    R: LedgerRecord,
    X: Default + Send + 'static,
{
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if !authorized(&replicator.inner.config.token, presented) {
        return coded(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let incoming = match serde_json::from_slice::<Exchange<R>>(&body) {
        Ok(incoming) => incoming,
        Err(_) => return coded(StatusCode::BAD_REQUEST, "invalid"),
    };
    match replicator.exchange(incoming).await {
        Ok(reply) => (StatusCode::OK, Json(reply)).into_response(),
        Err(error) => {
            let code = static_code(H::code(&error));
            coded(http_status(code), code)
        }
    }
}

pub(crate) fn authorized(token: &str, candidate: Option<&str>) -> bool {
    let expected = format!("Bearer {token}");
    let value = candidate.unwrap_or("").as_bytes();
    let mut difference = value.len() ^ expected.len();
    for (left, right) in expected
        .bytes()
        .zip(value.iter().copied().chain(std::iter::repeat(0)))
    {
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

pub(crate) fn coded(status: StatusCode, code: &'static str) -> Response {
    (status, Json(serde_json::json!({"code": code}))).into_response()
}

fn http_status(code: &str) -> StatusCode {
    match code {
        "conflict" => StatusCode::CONFLICT,
        "upgrade_required" => StatusCode::GONE,
        "storage" => StatusCode::SERVICE_UNAVAILABLE,
        "limit" => StatusCode::PAYLOAD_TOO_LARGE,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        _ => StatusCode::BAD_REQUEST,
    }
}

fn fail<E>(code: &'static str, message: impl Into<String>) -> E
where
    E: From<StoreError>,
{
    StoreError::new(code, message).into()
}

fn carry<E>(error: &forthebirds_net::Error) -> E
where
    E: From<StoreError>,
{
    StoreError::new(error.code, error.message.clone()).into()
}

fn static_code(code: &str) -> &'static str {
    match code {
        "auth" => "auth",
        "exchange" => "exchange",
        "identity" => "identity",
        "tls" => "tls",
        "connect" => "connect",
        "conflict" => "conflict",
        "upgrade_required" => "upgrade_required",
        "invalid" => "invalid",
        "config" => "config",
        "storage" => "storage",
        "limit" => "limit",
        "capacity" => "capacity",
        "discovery" => "discovery",
        "discovery_invalid" => "discovery_invalid",
        "discovery_unavailable" => "discovery_unavailable",
        "unauthorized" => "unauthorized",
        "budget_exhausted" => "budget_exhausted",
        "missing_reference" => "missing_reference",
        "shutdown" => "shutdown",
        _ => "exchange",
    }
}

async fn read_limited(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, ()> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
