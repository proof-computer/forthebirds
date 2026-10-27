//! One exchange route that dispatches each request to the member for its run.

use crate::exchange::Exchange;
use crate::replicator::{Replicator, authorized, coded, handle_exchange};
use crate::{BODY_LIMIT, ROUTE};
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use forthebirds_store::{Error as StoreError, Host, LedgerRecord};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct Inner<H, R, X> {
    peer_id: String,
    token: String,
    members: Mutex<BTreeMap<String, Replicator<H, R, X>>>,
}

/// Several replicators behind one [`ROUTE`].
///
/// Clones share membership, so a member inserted after [`Self::router`] was
/// taken is served by that router, and a removed member is not.
pub struct Runs<H, R, X> {
    inner: Arc<Inner<H, R, X>>,
}

impl<H, R, X> Clone for Runs<H, R, X> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<H, R, X> Runs<H, R, X> {
    /// Refuse a `peer_id` that is not an identifier, or a token that is not
    /// 32–512 visible ASCII bytes. The refusal code is `config`.
    pub fn new(
        peer_id: impl Into<String>,
        token: impl Into<String>,
    ) -> forthebirds_store::Result<Self> {
        let peer_id = peer_id.into();
        let token = token.into();
        if forthebirds_store::identifier(&peer_id).is_err() {
            return Err(StoreError::new("config", "peer_id must be an identifier"));
        }
        if !(32..=512).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(StoreError::new(
                "config",
                "token must be 32–512 visible ASCII bytes",
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                peer_id,
                token,
                members: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Refuse a replicator whose `peer_id` or token differs from the set's, or
    /// whose `run_id` is already a member. The refusal code is `config`.
    pub async fn insert(&self, replicator: Replicator<H, R, X>) -> forthebirds_store::Result<()> {
        if replicator.peer_id() != self.inner.peer_id || replicator.token() != self.inner.token {
            return Err(StoreError::new(
                "config",
                "member peer_id and token must match the set",
            ));
        }
        let mut members = self.inner.members.lock().await;
        if members.contains_key(replicator.run_id()) {
            return Err(StoreError::new("config", "run_id is already a member"));
        }
        members.insert(replicator.run_id().to_owned(), replicator);
        Ok(())
    }

    /// Take a member out of the set. The router stops serving it.
    pub async fn remove(&self, run_id: &str) -> Option<Replicator<H, R, X>> {
        self.inner.members.lock().await.remove(run_id)
    }

    /// The member for `run_id`, if it is still in the set.
    pub async fn get(&self, run_id: &str) -> Option<Replicator<H, R, X>> {
        self.inner.members.lock().await.get(run_id).cloned()
    }

    /// Member run ids, in ascending order.
    pub async fn run_ids(&self) -> Vec<String> {
        self.inner.members.lock().await.keys().cloned().collect()
    }
}

impl<H, R, X> Runs<H, R, X>
where
    H: Host<R, X>,
    R: LedgerRecord,
    X: Default + Send + 'static,
{
    /// `POST` [`ROUTE`] with a body limit of [`BODY_LIMIT`].
    ///
    /// The bearer check against this set's token comes first, then the JSON
    /// parse, then dispatch. An unknown `run_id` answers `409` `conflict` and
    /// imports nothing. A member answers as [`Replicator::exchange`].
    pub fn router(&self) -> Router {
        let runs = self.clone();
        Router::new()
            .route(
                ROUTE,
                post(move |headers: HeaderMap, body: axum::body::Bytes| {
                    let runs = runs.clone();
                    async move { handle(runs, headers, body).await }
                }),
            )
            .layer(DefaultBodyLimit::max(BODY_LIMIT))
    }
}

async fn handle<H, R, X>(
    runs: Runs<H, R, X>,
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
    if !authorized(&runs.inner.token, presented) {
        return coded(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let incoming = match serde_json::from_slice::<Exchange<R>>(&body) {
        Ok(incoming) => incoming,
        Err(_) => return coded(StatusCode::BAD_REQUEST, "invalid"),
    };
    let member = {
        let members = runs.inner.members.lock().await;
        members.get(&incoming.run_id).cloned()
    };
    match member {
        Some(member) => handle_exchange(member, headers, body).await,
        None => coded(StatusCode::CONFLICT, "conflict"),
    }
}
