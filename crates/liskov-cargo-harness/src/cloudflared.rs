//! One peer-owned connector for an operator-provisioned named tunnel.
//! Connector failure never restarts research or owns its persistence/allowance.
use crate::{
    Error, HarnessApp, Result,
    deployment::{Input, stop, verify},
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{Mutex, watch},
    time::{Instant, sleep, timeout},
};

fn fail(message: &str) -> Error {
    Error::new("cloudflared", message)
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Auto,
    Quic,
    Http2,
}
impl Protocol {
    fn argument(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Quic => "quic",
            Self::Http2 => "http2",
        }
    }
}
fn metrics_default() -> SocketAddr {
    "127.0.0.1:20241".parse().unwrap()
}
fn ca_default() -> PathBuf {
    "/etc/ssl/certs/ca-certificates.crt".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudflaredConfig {
    pub binary: Input,
    #[serde(default = "metrics_default")]
    pub metrics: SocketAddr,
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default = "ca_default")]
    pub ca_bundle: PathBuf,
}
impl CloudflaredConfig {
    pub fn resolve_paths(&mut self, root: &Path) {
        for path in [&mut self.binary.path, &mut self.ca_bundle] {
            if path.is_relative() {
                *path = root.join(&*path);
            }
        }
    }
    pub fn validate(&self, origin: SocketAddr, reserved_ports: &[u16]) -> Result<()> {
        if self.binary.url.is_some()
            || self.binary.path.as_os_str().is_empty()
            || self.binary.bytes == 0
            || self.binary.bytes > 256 * 1024 * 1024
            || self.binary.sha256.len() != 64
            || hex::decode(&self.binary.sha256).is_err()
            || !self.metrics.ip().is_loopback()
            || self.metrics.port() == 0
            || !origin.ip().is_loopback()
            || origin.port() == 0
            || self.metrics.port() == origin.port()
            || reserved_ports
                .iter()
                .any(|port| *port == 0 || *port == self.metrics.port())
            || self.ca_bundle.as_os_str().is_empty()
        {
            return Err(fail(
                "invalid binary, loopback listener or CA configuration",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub enabled: bool,
    pub phase: &'static str,
    pub pid: Option<u32>,
    /// Connector processes started after the first one, over the peer's life.
    pub restarts: u32,
    /// Delay before the next connector start, doubling per consecutive unstable
    /// attempt from one second to a sixty-second cap, reset by a stable connection.
    pub backoff_ms: u64,
    pub last_connected_at_ms: Option<u64>,
    pub last_transition_at_ms: u64,
    pub error_code: Option<&'static str>,
    pub exit_code: Option<i32>,
}
impl Status {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            phase: if enabled { "starting" } else { "disabled" },
            pid: None,
            restarts: 0,
            backoff_ms: 0,
            last_connected_at_ms: None,
            last_transition_at_ms: now(),
            error_code: None,
            exit_code: None,
        }
    }
}

pub struct Prepared {
    name: String,
    peer_token_env: String,
    tunnel_token_env: String,
    config: CloudflaredConfig,
    private: tempfile::TempDir,
    grace: Duration,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn command(binary: &Path, peer_token_env: &str, tunnel_token_env: &str) -> Command {
    let mut cmd = Command::new(binary);
    cmd.kill_on_drop(true).stdin(Stdio::null());
    for (key, _) in std::env::vars_os() {
        let text = key.to_string_lossy();
        if text.starts_with("TUNNEL_")
            || text.starts_with("LLAMA_")
            || text == tunnel_token_env
            || text == peer_token_env
        {
            cmd.env_remove(key);
        }
    }
    cmd
}

fn supported_version(text: &str) -> bool {
    text.split_whitespace().any(|part| {
        let mut parts = part.split('.');
        let version = (parts.next().and_then(|s| s.parse::<u32>().ok()),
            parts.next().and_then(|s| s.parse::<u32>().ok()), parts.next().and_then(|s| s.parse::<u32>().ok()));
        matches!(version, (Some(year), Some(month), Some(patch)) if parts.next().is_none() && (year,month,patch) >= (2026,10,2))
    })
}

impl Prepared {
    pub async fn from_env(
        app: &HarnessApp,
        config: &CloudflaredConfig,
        origin: SocketAddr,
        reserved_ports: &[u16],
    ) -> Result<Self> {
        let token = std::env::var(app.env("CLOUDFLARED_TOKEN"))
            .map_err(|_| fail("missing tunnel token"))?;
        let grace = match std::env::var(app.env("PARENT_SHUTDOWN_SECONDS")) {
            Ok(value) => {
                let seconds = value
                    .parse::<u64>()
                    .map_err(|_| fail("invalid parent shutdown deadline"))?;
                if !(1..=300).contains(&seconds) {
                    return Err(fail("invalid parent shutdown deadline"));
                }
                // Leave the outer launcher time to drain/reap the peer itself.
                Duration::from_secs(10).min(Duration::from_secs(seconds) / 2)
            }
            Err(std::env::VarError::NotPresent) => Duration::from_secs(10),
            Err(_) => return Err(fail("invalid parent shutdown deadline")),
        };
        Self::prepare(app, config, origin, reserved_ports, &token, grace).await
    }
    async fn prepare(
        app: &HarnessApp,
        config: &CloudflaredConfig,
        origin: SocketAddr,
        reserved_ports: &[u16],
        token: &str,
        grace: Duration,
    ) -> Result<Self> {
        config.validate(origin, reserved_ports)?;
        let token = token.trim();
        if token.is_empty() || token.len() > 16384 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(fail("invalid tunnel token"));
        }
        verify(&config.binary)?;
        let config = CloudflaredConfig {
            binary: Input {
                path: config
                    .binary
                    .path
                    .canonicalize()
                    .map_err(|_| fail("cannot resolve connector binary"))?,
                ..config.binary.clone()
            },
            ca_bundle: config
                .ca_bundle
                .canonicalize()
                .map_err(|_| fail("cannot resolve CA bundle"))?,
            ..config.clone()
        };
        let mut ca = String::new();
        std::fs::File::open(&config.ca_bundle)
            .map_err(|_| fail("cannot read CA bundle"))?
            .take(16 * 1024 * 1024 + 1)
            .read_to_string(&mut ca)
            .map_err(|_| fail("cannot read CA bundle"))?;
        if ca.len() > 16 * 1024 * 1024 || !ca.contains("-----BEGIN CERTIFICATE-----") {
            return Err(fail("CA bundle is empty or not PEM certificates"));
        }
        std::net::TcpListener::bind(config.metrics)
            .map_err(|_| fail("metrics listener already occupied"))?;
        let mut child = command(
            &config.binary.path,
            &app.env("PEER_TOKEN"),
            &app.env("CLOUDFLARED_TOKEN"),
        )
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| fail("cannot check connector version"))?;
        let mut data = Vec::new();
        let outcome = timeout(Duration::from_secs(5), async {
            child
                .stdout
                .take()
                .ok_or_else(|| fail("missing version output"))?
                .take(4097)
                .read_to_end(&mut data)
                .await
                .map_err(|_| fail("cannot read version output"))?;
            child
                .wait()
                .await
                .map_err(|_| fail("cannot reap version check"))
        })
        .await;
        let success = matches!(outcome, Ok(Ok(status)) if status.success());
        if !success || data.len() > 4096 || !supported_version(&String::from_utf8_lossy(&data)) {
            stop(&mut child, Duration::from_secs(1)).await?;
            return Err(fail("connector requires cloudflared 2026.10.2 or newer"));
        }
        let private = tempfile::Builder::new()
            .prefix(&format!("{}-cloudflared-", app.name()))
            .tempdir()
            .map_err(|_| fail("cannot create private connector directory"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(private.path(), std::fs::Permissions::from_mode(0o700))
                .map_err(|_| fail("cannot protect connector directory"))?;
        }
        let path = private.path().join("token");
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(path)
            .map_err(|_| fail("cannot create private token file"))?;
        file.write_all(token.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|_| fail("cannot write private token"))?;
        std::fs::write(private.path().join("config.yml"), "{}\n")
            .map_err(|_| fail("cannot write connector config"))?;
        Ok(Self {
            name: app.name().to_string(),
            peer_token_env: app.env("PEER_TOKEN"),
            tunnel_token_env: app.env("CLOUDFLARED_TOKEN"),
            config,
            private,
            grace,
        })
    }
    fn spawn(&self) -> Result<Child> {
        // Never adopt another process or use its readiness endpoint.
        std::net::TcpListener::bind(self.config.metrics)
            .map_err(|_| fail("metrics listener occupied before spawn"))?;
        command(
            &self.config.binary.path,
            &self.peer_token_env,
            &self.tunnel_token_env,
        )
        .args(["tunnel", "--no-autoupdate", "--config"])
        .arg(self.private.path().join("config.yml"))
        .args([
            "--metrics",
            &self.config.metrics.to_string(),
            "--protocol",
            self.config.protocol.argument(),
            "--grace-period",
            &format!("{}ms", self.grace.as_millis()),
            "run",
            "--token-file",
        ])
        .arg(self.private.path().join("token"))
        .env("SSL_CERT_FILE", &self.config.ca_bundle)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| fail("cannot spawn connector"))
    }
    pub async fn run(
        self,
        state: Arc<Mutex<Status>>,
        stop_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        self.supervise(state, stop_rx, Policy::default()).await
    }
    async fn supervise(
        self,
        state: Arc<Mutex<Status>>,
        mut stop_rx: watch::Receiver<bool>,
        policy: Policy,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(policy.request)
            .build()
            .map_err(|_| fail("cannot create readiness client"))?;
        // The connector is retried for the life of the peer: a window is ~50
        // minutes and a connector lost in its first minutes must not stay lost.
        // Only the delay grows, doubling per consecutive unstable attempt up to
        // the cap, so a dead credential costs at most one start a minute.
        let mut attempt: u32 = 0;
        let mut streak: u32 = 0;
        loop {
            if *stop_rx.borrow() {
                break;
            }
            transition(
                &self.name,
                &state,
                if attempt == 0 {
                    "starting"
                } else {
                    "restarting"
                },
                None,
                attempt,
                None,
            )
            .await;
            let (cause, stable) = match self.spawn() {
                Err(_) => ("spawn_failed", false),
                Ok(mut child) => {
                    state.lock().await.pid = child.id();
                    // Drain bounded chunks without relaying provider text: neither
                    // tokens nor private upstream diagnostics enter app/public logs.
                    let stdout = tokio::spawn(discard_output(child.stdout.take().unwrap()));
                    let stderr = tokio::spawn(discard_output(child.stderr.take().unwrap()));
                    let result = monitor(
                        &self.name,
                        &mut child,
                        &self.config,
                        &client,
                        &state,
                        &mut stop_rx,
                        &policy,
                    )
                    .await;
                    let cleanup = stop(&mut child, self.grace).await;
                    for mut task in [stdout, stderr] {
                        if timeout(Duration::from_secs(1), &mut task).await.is_err() {
                            task.abort();
                        }
                    }
                    cleanup?;
                    match result? {
                        Some(outcome) => outcome,
                        None => {
                            transition(&self.name, &state, "stopped", None, attempt, None).await;
                            return Ok(());
                        }
                    }
                }
            };
            streak = if stable { 1 } else { streak.saturating_add(1) };
            let delay = policy.delay(streak);
            state.lock().await.backoff_ms = delay.as_millis() as u64;
            transition(
                &self.name,
                &state,
                "reconnecting",
                None,
                attempt,
                Some(cause),
            )
            .await;
            attempt = attempt.saturating_add(1);
            tokio::select! { biased; _ = stopped(&mut stop_rx) => break,
            _ = sleep(delay) => {} }
        }
        let restarts = state.lock().await.restarts;
        transition(&self.name, &state, "stopped", None, restarts, None).await;
        Ok(())
    }
}

#[derive(Clone)]
struct Policy {
    /// Time a connector may stay non-ready before it is replaced; a connection
    /// that stayed ready at least this long counts as stable.
    ready: Duration,
    poll: Duration,
    request: Duration,
    /// Delay before the first retry; it doubles per consecutive unstable attempt.
    backoff: Duration,
    /// The longest delay between two connector starts.
    backoff_cap: Duration,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            ready: Duration::from_secs(60),
            poll: Duration::from_secs(2),
            request: Duration::from_secs(2),
            backoff: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
        }
    }
}
impl Policy {
    /// Delay before the next start after `streak` consecutive unstable attempts
    /// (`streak` is 1 right after a stable connection was lost).
    fn delay(&self, streak: u32) -> Duration {
        let doublings = streak.saturating_sub(1).min(31);
        self.backoff
            .checked_mul(1u32 << doublings)
            .unwrap_or(self.backoff_cap)
            .min(self.backoff_cap)
    }
}
async fn stopped(receiver: &mut watch::Receiver<bool>) {
    let _ = receiver.wait_for(|v| *v).await;
}
async fn discard_output(mut pipe: impl tokio::io::AsyncRead + Unpin) {
    let mut buffer = [0; 4096];
    while let Ok(count) = pipe.read(&mut buffer).await {
        if count == 0 {
            break;
        }
    }
}
async fn transition(
    name: &str,
    state: &Mutex<Status>,
    phase: &'static str,
    pid: Option<u32>,
    attempt: u32,
    cause: Option<&'static str>,
) {
    let mut state = state.lock().await;
    state.phase = phase;
    state.pid = pid;
    state.restarts = attempt;
    state.error_code = cause;
    state.last_transition_at_ms = now();
    crate::log_line!(
        "{name} cloudflared phase={phase} restarts={attempt} error={}",
        cause.unwrap_or("none")
    );
}
async fn monitor(
    name: &str,
    child: &mut Child,
    config: &CloudflaredConfig,
    client: &reqwest::Client,
    state: &Mutex<Status>,
    stop_rx: &mut watch::Receiver<bool>,
    policy: &Policy,
) -> Result<Option<(&'static str, bool)>> {
    let mut last_ready = Instant::now();
    let mut connected = false;
    // Set when the connector first became ready; the attempt counts as stable
    // once it has stayed ready for the readiness window.
    let mut first_ready: Option<Instant> = None;
    let stable = |first_ready: Option<Instant>, last_ready: Instant| {
        first_ready.is_some_and(|first| last_ready.duration_since(first) >= policy.ready)
    };
    loop {
        if let Some(exit) = child
            .try_wait()
            .map_err(|_| fail("cannot observe connector"))?
        {
            state.lock().await.exit_code = exit.code();
            return Ok(Some(("child_exited", stable(first_ready, last_ready))));
        }
        let ready = tokio::select! { biased; _ = stopped(stop_rx) => return Ok(None),
        response = client.get(format!("http://{}/ready", config.metrics)).send() => response.is_ok_and(|r| r.status() == reqwest::StatusCode::OK) };
        if ready {
            last_ready = Instant::now();
            first_ready.get_or_insert(last_ready);
            if !connected {
                let count = state.lock().await.restarts;
                transition(name, state, "connected", child.id(), count, None).await;
                connected = true;
            }
            state.lock().await.last_connected_at_ms = Some(now());
        } else {
            if connected {
                let count = state.lock().await.restarts;
                transition(
                    name,
                    state,
                    "reconnecting",
                    child.id(),
                    count,
                    Some("not_ready"),
                )
                .await;
                connected = false;
            }
            if last_ready.elapsed() >= policy.ready {
                return Ok(Some(("readiness_timeout", stable(first_ready, last_ready))));
            }
        }
        tokio::select! { biased; _ = stopped(stop_rx) => return Ok(None), _ = sleep(policy.poll) => {} }
    }
}

#[cfg(all(test, unix))]
#[path = "cloudflared_tests.rs"]
mod tests;
