//! Pinned-TLS peer HTTP with hourly identity rotation and a local advertisement.

mod config;
mod error;
mod log;
pub mod peer_transport;
pub mod static_peers;

pub use config::TransportConfig;
pub use error::{Error, Result};
pub use peer_transport::{
    DirectStatus, PeerTransport, PinnedClient, TransportState, pinned_client, publish_summary,
};
pub use static_peers::{PeerTarget, StaticPeer, StaticPeers};
