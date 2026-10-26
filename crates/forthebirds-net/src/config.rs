#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub listen: std::net::SocketAddr,
    /// Local advertisement path. `start` requires `Some`.
    pub advertisement_file: Option<std::path::PathBuf>,
    pub service: String,
    pub application: String,
}
