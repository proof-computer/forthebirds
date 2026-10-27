//! This crate is new code shaped after Planetarium `peer.rs` `exchange`/`sync_direct_once` at `375b696`, not a move, and it knows no record type.

mod exchange;
mod replicator;
mod runs;

pub use exchange::{Exchange, ExchangeReply};
pub use replicator::{LinkStatus, Replicator, SyncConfig, TickSummary};
pub use runs::Runs;

/// Wire version of this crate's exchange. It is not another product's protocol.
pub const PROTOCOL: u32 = 1;
/// `POST` path mounted by [`Replicator::router`] and [`Runs::router`].
pub const ROUTE: &str = "/api/sync/v1/exchange";
/// Pages one peer may exchange per tick while either side still has more.
pub const CATCH_UP_PAGES: usize = 64;
/// Largest accepted request or response body, in bytes.
pub const BODY_LIMIT: usize = 2 * 1024 * 1024;
