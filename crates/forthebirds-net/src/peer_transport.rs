//! Private IPv6-only transport for native peers. The website router never runs here.
use crate::{Error, Result, TransportConfig};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use serde::Serialize;
use sha2::{Digest, Sha256};
use socket2::{Domain, Protocol, Socket, Type};
use std::{io::Write, net::SocketAddr, path::PathBuf, time::Duration};

const CERT_VALIDITY: Duration = Duration::from_secs(2 * 60 * 60);
const ROTATE_AFTER: Duration = Duration::from_secs(60 * 60);
const ROTATE_RETRY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default, Serialize)]
pub struct DirectStatus {
    pub enabled: bool,
    pub listener_ready: bool,
    pub listener_port: Option<u16>,
    pub advertisement_error: Option<&'static str>,
    pub rotation_error: Option<&'static str>,
    pub tls_fingerprint: Option<String>,
}

#[derive(Clone, Default)]
pub struct TransportState {
    pub status: std::sync::Arc<tokio::sync::Mutex<DirectStatus>>,
    pub advertisement_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

pub struct PeerTransport {
    handle: Handle<SocketAddr>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    advertisement: PathBuf,
    tls: RustlsConfig,
    config: TransportConfig,
    state: TransportState,
    // The private key is owned by this lifetime and removed with the listener.
    _key_dir: std::sync::Arc<tokio::sync::Mutex<tempfile::TempDir>>,
    rotation_stop: tokio::sync::watch::Sender<bool>,
    rotation_task: Option<tokio::task::JoinHandle<()>>,
}

struct Identity {
    cert: Vec<u8>,
    key: Vec<u8>,
    fingerprint: String,
    key_dir: tempfile::TempDir,
}

fn generate_identity(application: &str) -> Result<Identity> {
    let key_dir = tempfile::Builder::new()
        .prefix(&format!("{application}-peer-key-"))
        .tempdir()
        .map_err(|_| Error::new("peer_key", "cannot create private peer key directory"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(key_dir.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|_| Error::new("peer_key", "cannot protect private peer key directory"))?;
    }
    let key_pair = rcgen::KeyPair::generate()
        .map_err(|_| Error::new("peer_key", "cannot generate peer key"))?;
    let mut params = rcgen::CertificateParams::new(vec![format!("{application}-peer")])
        .map_err(|_| Error::new("peer_key", "cannot configure peer certificate"))?;
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::minutes(1);
    params.not_after = params.not_before + time::Duration::hours(2);
    let cert = params
        .self_signed(&key_pair)
        .map_err(|_| Error::new("peer_key", "cannot sign peer certificate"))?
        .der()
        .to_vec();
    let key = key_pair.serialize_der();
    let fingerprint = format!("sha256:{}", hex::encode(Sha256::digest(&cert)));
    let key_path = key_dir.path().join("key.der");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&key_path)
        .map_err(|_| Error::new("peer_key", "cannot create private peer key"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| Error::new("peer_key", "cannot protect private peer key"))?;
    }
    file.write_all(&key)
        .and_then(|_| file.sync_all())
        .map_err(|_| Error::new("peer_key", "cannot persist private peer key"))?;
    Ok(Identity {
        cert,
        key,
        fingerprint,
        key_dir,
    })
}

impl PeerTransport {
    pub async fn start(
        config: &TransportConfig,
        state: &TransportState,
        router: axum::Router,
    ) -> Result<Self> {
        // axum-server builds its rustls config through the process provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let addr = config.listen;
        if !addr.is_ipv6() {
            return Err(Error::new("config", "direct peer listener requires IPv6"));
        }
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
            .map_err(|_| Error::new("peer_listener", "cannot create IPv6 socket"))?;
        socket
            .set_only_v6(true)
            .map_err(|_| Error::new("peer_listener", "cannot require IPv6-only socket"))?;
        socket
            .bind(&addr.into())
            .map_err(|_| Error::new("peer_listener", "direct peer port unavailable"))?;
        socket
            .listen(128)
            .map_err(|_| Error::new("peer_listener", "cannot listen for direct peers"))?;
        socket
            .set_nonblocking(true)
            .map_err(|_| Error::new("peer_listener", "cannot set nonblocking listener"))?;
        let listener: std::net::TcpListener = socket.into();
        let port = listener
            .local_addr()
            .map_err(|_| Error::new("peer_listener", "cannot read bound peer port"))?
            .port();

        let identity = generate_identity(&config.application)?;
        let tls = RustlsConfig::from_der(vec![identity.cert], identity.key)
            .await
            .map_err(|_| Error::new("peer_key", "cannot configure peer TLS"))?;
        let handle = Handle::new();
        let server = axum_server::from_tcp_rustls(listener, tls.clone())
            .map_err(|_| Error::new("peer_listener", "cannot prepare peer TLS listener"))?
            .handle(handle.clone());
        let task = tokio::spawn(async move { server.serve(router.into_make_service()).await });
        // A bound socket and prepared TLS identity are the readiness boundary.
        let path = config.advertisement_file.as_ref().unwrap().clone();
        let advertisement = serde_json::json!({
            "schema":"liskov.peer-advertisement.v2", "service":config.service,
            "port":port, "ready":true, "tlsFingerprint":identity.fingerprint
        });
        let _ad_guard = state.advertisement_lock.lock().await;
        let published = publish(&path, &advertisement);
        let mut status = state.status.lock().await;
        status.enabled = true;
        status.listener_ready = true;
        status.listener_port = Some(port);
        status.tls_fingerprint = Some(identity.fingerprint);
        status.advertisement_error = published.as_ref().err().map(|_| "publish_failed");
        drop(status);
        drop(_ad_guard);
        if let Err(error) = published {
            crate::log::log_line!(
                "{} direct-peer advertisement error={}",
                config.application,
                error.code
            );
        }
        let key_dir = std::sync::Arc::new(tokio::sync::Mutex::new(identity.key_dir));
        let (rotation_stop, mut rotation_rx) = tokio::sync::watch::channel(false);
        let rotation_config = config.clone();
        let rotation_state = state.clone();
        let rotation_tls = tls.clone();
        let rotation_dir = key_dir.clone();
        let rotation_handle = handle.clone();
        let rotation_task = tokio::spawn(async move {
            let mut born = tokio::time::Instant::now();
            let mut interval = tokio::time::interval(ROTATE_RETRY);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = interval.tick() => {}, _ = rotation_rx.changed() => return }
                if *rotation_rx.borrow() {
                    return;
                }
                if born.elapsed() < ROTATE_AFTER {
                    continue;
                }
                match rotate_identity(
                    &rotation_config,
                    &rotation_state,
                    &rotation_tls,
                    &rotation_dir,
                )
                .await
                {
                    Ok(()) => {
                        born = tokio::time::Instant::now();
                        rotation_state.status.lock().await.rotation_error = None;
                    }
                    Err(error) => {
                        crate::log::log_line!(
                            "{} direct-peer rotation error={}",
                            rotation_config.application,
                            error.code
                        );
                        rotation_state.status.lock().await.rotation_error = Some(error.code);
                        if born.elapsed() >= CERT_VALIDITY - Duration::from_secs(120) {
                            let _ad_guard = rotation_state.advertisement_lock.lock().await;
                            let _ = std::fs::remove_file(
                                rotation_config.advertisement_file.as_ref().unwrap(),
                            );
                            rotation_state.status.lock().await.listener_ready = false;
                            rotation_handle.shutdown();
                            return;
                        }
                    }
                }
            }
        });
        Ok(Self {
            handle,
            task: Some(task),
            advertisement: path,
            tls,
            config: config.clone(),
            state: state.clone(),
            _key_dir: key_dir,
            rotation_stop,
            rotation_task: Some(rotation_task),
        })
    }

    pub async fn finished(&mut self) -> Result<()> {
        let outcome = {
            let server = self.task.as_mut().expect("listener task");
            let rotation = self.rotation_task.as_mut().expect("rotation task");
            tokio::select! {
                result = server => (Some(result), None),
                result = rotation => (None, Some(result)),
            }
        };
        if let Some(completed) = outcome.0 {
            self.task = None;
            let result =
                completed.map_err(|_| Error::new("peer_listener", "peer TLS task failed"))?;
            result.map_err(|_| Error::new("peer_listener", "peer TLS server failed"))?;
            return Err(Error::new(
                "peer_listener",
                "peer TLS listener ended unexpectedly",
            ));
        }
        self.rotation_task = None;
        outcome
            .1
            .unwrap()
            .map_err(|_| Error::new("peer_key", "peer identity rotation task failed"))?;
        Err(Error::new(
            "peer_key",
            "peer identity rotation stopped unexpectedly",
        ))
    }
    pub async fn rotate_now(&self) -> Result<()> {
        rotate_identity(&self.config, &self.state, &self.tls, &self._key_dir).await
    }
    pub async fn stop(mut self) -> Result<()> {
        let _ = self.rotation_stop.send(true);
        if let Some(task) = self.rotation_task.take() {
            let _ = task.await;
        }
        let _ad_guard = self.state.advertisement_lock.lock().await;
        let withdrawal = match std::fs::remove_file(&self.advertisement) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Error::new(
                "peer_advertisement",
                "cannot withdraw peer advertisement",
            )),
        };
        {
            let mut status = self.state.status.lock().await;
            status.listener_ready = false;
            if withdrawal.is_err() {
                status.advertisement_error = Some("withdraw_failed");
            }
        }
        drop(_ad_guard);
        self.handle.graceful_shutdown(Some(Duration::from_secs(5)));
        if let Some(mut task) = self.task.take() {
            let result = (&mut task)
                .await
                .map_err(|_| Error::new("peer_listener", "peer TLS task failed"))?;
            result.map_err(|_| Error::new("peer_listener", "peer TLS server failed"))?;
        }
        withdrawal?;
        Ok(())
    }

    pub fn advertisement(&self) -> &std::path::Path {
        &self.advertisement
    }

    pub async fn key_dir(&self) -> PathBuf {
        self._key_dir.lock().await.path().to_owned()
    }
}

impl Drop for PeerTransport {
    fn drop(&mut self) {
        let _ = self.rotation_stop.send(true);
        if let Some(task) = self.rotation_task.take() {
            task.abort();
        }
        self.handle.shutdown();
        let _ = std::fs::remove_file(&self.advertisement);
    }
}

async fn rotate_identity(
    config: &TransportConfig,
    state: &TransportState,
    tls: &RustlsConfig,
    key_dir: &std::sync::Arc<tokio::sync::Mutex<tempfile::TempDir>>,
) -> Result<()> {
    let identity = generate_identity(&config.application)?;
    let _ad_guard = state.advertisement_lock.lock().await;
    let path = config.advertisement_file.as_ref().unwrap();
    // No new pin is advertised until the TLS acceptor presents that cert.
    // Existing snapshots may still carry the old pin, which fails closed.
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(Error::new(
                "peer_advertisement",
                "cannot withdraw old peer pin",
            ));
        }
    }
    tls.reload_from_der(vec![identity.cert], identity.key)
        .await
        .map_err(|_| Error::new("peer_key", "cannot rotate peer TLS identity"))?;
    let port = state.status.lock().await.listener_port.unwrap();
    let advertisement = serde_json::json!({
        "schema":"liskov.peer-advertisement.v2", "service":config.service,
        "port":port, "ready":true, "tlsFingerprint":identity.fingerprint
    });
    let published = publish(path, &advertisement);
    {
        let mut status = state.status.lock().await;
        status.tls_fingerprint = Some(identity.fingerprint);
        status.advertisement_error = published.as_ref().err().map(|_| "publish_failed");
    }
    *key_dir.lock().await = identity.key_dir;
    published
}

fn publish(path: &std::path::Path, advertisement: &serde_json::Value) -> Result<()> {
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| Error::new("peer_advertisement", "cannot create advertisement"))?;
    serde_json::to_writer(&mut temporary, advertisement)
        .map_err(|_| Error::new("peer_advertisement", "cannot encode advertisement"))?;
    temporary
        .write_all(b"\n")
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|_| Error::new("peer_advertisement", "cannot sync advertisement"))?;
    temporary
        .persist(path)
        .map_err(|_| Error::new("peer_advertisement", "cannot publish advertisement"))?;
    Ok(())
}

pub async fn publish_summary(
    config: &TransportConfig,
    state: &TransportState,
    last_success_ms: Option<u64>,
    failure_code: &str,
) {
    let _ad_guard = state.advertisement_lock.lock().await;
    let Some(path) = config.advertisement_file.as_ref() else {
        return;
    };
    let status = state.status.lock().await.clone();
    if !status.listener_ready {
        return;
    }
    let (Some(fingerprint), Some(port)) = (status.tls_fingerprint, status.listener_port) else {
        return;
    };
    let code = match failure_code {
        "none" | "connect" | "tls" | "auth" | "identity" | "exchange" | "discovery" => failure_code,
        _ => "exchange",
    };
    let mut advertisement = serde_json::json!({
        "schema":"liskov.peer-advertisement.v2", "service":config.service,
        "port":port, "ready":true,
        "tlsFingerprint":fingerprint,
        "peerSyncFailureCode":code,
    });
    if let Some(at) = last_success_ms {
        advertisement["peerSyncLastSuccessAtMs"] = serde_json::json!(at);
    }
    let failure = publish(path, &advertisement).err();
    state.status.lock().await.advertisement_error = failure.as_ref().map(|_| "publish_failed");
    if let Some(error) = failure {
        crate::log::log_line!(
            "{} direct-peer advertisement error={}",
            config.application,
            error.code
        );
    }
}

/// Build a client for exactly one advertised endpoint/incarnation/pin.
/// The verifier accepts only that leaf DER and delegates handshake signatures
/// to rustls. It never sends HTTP before the pinned handshake succeeds.
#[derive(Clone)]
pub struct PinnedClient {
    pub client: reqwest::Client,
    pin_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl PinnedClient {
    pub fn request_error(&self, error: &reqwest::Error) -> Error {
        use std::sync::atomic::Ordering;
        if self.pin_failed.swap(false, Ordering::SeqCst) {
            return Error::new("tls", "peer certificate pin mismatch");
        }
        if error.is_timeout() {
            return Error::new("exchange", "peer exchange timed out");
        }
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(err) = source {
            let name = err.to_string().to_ascii_lowercase();
            if name.contains("tls") || name.contains("certificate") || name.contains("handshake") {
                return Error::new("tls", "peer TLS handshake failed");
            }
            source = err.source();
        }
        Error::new("connect", "peer connection failed")
    }
}
pub fn pinned_client(fingerprint: &str, timeout: Duration) -> Result<PinnedClient> {
    let raw = fingerprint
        .strip_prefix("sha256:")
        .ok_or_else(|| Error::new("tls", "invalid advertised certificate pin"))?;
    let expected: [u8; 32] = hex::decode(raw)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| Error::new("tls", "invalid advertised certificate pin"))?;
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let pin_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let verifier = std::sync::Arc::new(PinnedVerifier {
        expected,
        algorithms: provider.signature_verification_algorithms,
        pin_failed: pin_failed.clone(),
    });
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| Error::new("tls", "cannot configure peer TLS version"))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    tls.resumption = rustls::client::Resumption::disabled();
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(timeout)
        .build()
        .map_err(|_| Error::new("tls", "cannot build pinned peer client"))?;
    Ok(PinnedClient { client, pin_failed })
}

#[derive(Debug)]
struct PinnedVerifier {
    expected: [u8; 32],
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
    pin_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl rustls::client::danger::ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        cert: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let actual = Sha256::digest(cert.as_ref());
        let mut difference = 0u8;
        for (a, b) in self.expected.iter().zip(actual.iter()) {
            difference |= a ^ b;
        }
        if difference != 0 || !intermediates.is_empty() || cert.len() > 16 * 1024 {
            self.pin_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ));
        }
        let (remaining, parsed) =
            x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|_| {
                rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
            })?;
        if !remaining.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::BadEncoding,
            ));
        }
        let before = parsed.validity().not_before.timestamp();
        let after = parsed.validity().not_after.timestamp();
        let at = now.as_secs() as i64;
        if at < before {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidYet,
            ));
        }
        if at > after
            || after
                .checked_sub(before)
                .is_none_or(|duration| !(1..=7200).contains(&duration))
        {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Expired,
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_verifier_rejects_expired_leaf_even_when_hash_matches() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["planetarium-peer".into()]).unwrap();
        params.not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(3);
        params.not_after = params.not_before + time::Duration::hours(2);
        let cert = params.self_signed(&key).unwrap();
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let verifier = PinnedVerifier {
            expected: Sha256::digest(cert.der().as_ref()).into(),
            algorithms: provider.signature_verification_algorithms,
            pin_failed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let result = rustls::client::danger::ServerCertVerifier::verify_server_cert(
            &verifier,
            cert.der(),
            &[],
            &rustls::pki_types::ServerName::try_from("planetarium-peer").unwrap(),
            &[],
            rustls::pki_types::UnixTime::now(),
        );
        assert!(matches!(
            result,
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Expired
            ))
        ));
    }
}
