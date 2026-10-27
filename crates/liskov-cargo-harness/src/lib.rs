//! Cargo launch harness copied from liskov-planetarium `c008afc37534f0b196eddca4ab09a06604534f4e`
//! under ADR-0262. Planetarium rebinds under ADR-0264
//! (BKLG-20261027-c0p1).
//!
//! The harness fetches one payload, supervises the inference server and an
//! optional named-tunnel connector, and starts the application peer. It does
//! not parse the peer's configuration: the deployment descriptor carries the
//! inference envelope, and [`HarnessApp`] turns every environment variable
//! name into `LISKOV_<APP>_…`. [`Descriptor`] supplies that envelope; the
//! default reads the harness JSON. [`directory`] reads the helper's direct-peer
//! directory into bounded peer entries and does not decode job references.

mod app;
pub mod cloudflared;
pub mod deployment;
mod deployment_download;
pub mod directory;
mod error;
pub mod launch;
#[doc(hidden)]
pub mod log;
pub mod readiness;

pub use app::{Descriptor, HarnessApp};
pub use error::{Error, Result};
