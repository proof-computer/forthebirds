//! Local llama-server model and embedding clients.

mod config;
pub mod embedding;
mod error;
pub mod http;
pub mod model;

pub use config::{DEFAULT_RENDER_TOKENS, InferenceConfig};
pub use error::{Error, Result};
