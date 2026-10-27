use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;

// Every test below forks a connector, and a fork keeps a copy of each open
// descriptor until the child execs. A concurrent test's just-written connector
// script can stay held for that instant, which fails it with `ETXTBSY`.
// So these tests run one at a time.
static FORKS: Mutex<()> = Mutex::const_new(());

#[path = "../tests/support/ports.rs"]
mod ports;

fn fixture(
    mode: &str,
    version: &str,
) -> (
    tempfile::TempDir,
    CloudflaredConfig,
    std::net::SocketAddr,
    Vec<u16>,
) {
    let root = tempfile::tempdir().unwrap();
    let binary = root.path().join("cloudflared");
    let script = format!(
        r#"#!/usr/bin/env python3
import http.server,json,os,signal,sys,time
from pathlib import Path
root=Path(__file__).parent
if '--version' in sys.argv:
    print('cloudflared version {version}')
    sys.exit(0)
assert sys.argv[1]=='tunnel'
assert '--token' not in sys.argv
assert 'LISKOV_FIXTURE_CLOUDFLARED_TOKEN' not in os.environ
assert 'LISKOV_FIXTURE_PEER_TOKEN' not in os.environ
assert not any(k.startswith('TUNNEL_') for k in os.environ)
token_path=Path(sys.argv[sys.argv.index('--token-file')+1])
assert token_path.stat().st_mode & 0o777 == 0o600
assert token_path.parent.stat().st_mode & 0o777 == 0o700
token=token_path.read_text()
assert token not in sys.argv
with (root/'spawns').open('a') as f: f.write(json.dumps({{'pid':os.getpid(),'token_path':str(token_path)}})+'\n')
# Exceed pipe buffers and include credentials: the parent must drain without
# emitting provider text or blocking the process.
print(token+'x'*70000,flush=True)
print(token+'y'*70000,file=sys.stderr,flush=True)
mode='{mode}'
def stop(*_):
    (root/'terminated').write_text('term')
    sys.exit(0)
if mode=='hang':
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    time.sleep(60)
if mode=='exit': sys.exit(7)
signal.signal(signal.SIGTERM,stop)
host,port=sys.argv[sys.argv.index('--metrics')+1].rsplit(':',1)
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503 if (root/'unready').exists() else 200)
        self.end_headers();self.wfile.write(b'{{}}')
    def log_message(self,*_): pass
http.server.HTTPServer((host,int(port)),Handler).serve_forever()
"#
    );
    std::fs::write(&binary, script.as_bytes()).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let ca = root.path().join("ca.pem");
    std::fs::write(
        &ca,
        "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    let _app = crate::HarnessApp::new("fixture").unwrap();
    let metrics = ports::port();
    let listen = ports::port();
    let config = CloudflaredConfig {
        binary: Input {
            path: binary,
            bytes: script.len() as u64,
            sha256: hex::encode(Sha256::digest(script.as_bytes())),
            url: None,
        },
        metrics: format!("127.0.0.1:{metrics}").parse().unwrap(),
        protocol: Protocol::Auto,
        ca_bundle: ca,
    };
    let origin = format!("127.0.0.1:{listen}").parse().unwrap();
    // 8081 is the research port the old peer config contributed, so a metrics
    // listener on that port still fails validation.
    let reserved = vec![8081];
    (root, config, origin, reserved)
}

async fn prepare(
    config: &mut CloudflaredConfig,
    origin: std::net::SocketAddr,
    reserved: &[u16],
    token: &str,
    grace: Duration,
) -> crate::Result<Prepared> {
    let app = crate::HarnessApp::new("fixture").unwrap();
    // ports::port() only probes the bind, then drops it. A parallel test
    // process can take that port before prepare binds it for real.
    for _ in 0..8 {
        match Prepared::prepare(&app, config, origin, reserved, token, grace).await {
            Err(error) if error.message == "metrics listener already occupied" => {
                let metrics = ports::port();
                config.metrics = format!("127.0.0.1:{metrics}").parse().unwrap();
            }
            other => return other,
        }
    }
    Prepared::prepare(&app, config, origin, reserved, token, grace).await
}
fn quick() -> Policy {
    Policy {
        ready: Duration::from_secs(5),
        poll: Duration::from_millis(20),
        request: Duration::from_millis(100),
        backoff: Duration::from_millis(10),
        backoff_cap: Duration::from_millis(40),
    }
}
async fn until(state: &Mutex<Status>, predicate: impl Fn(&Status) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate(&*state.lock().await) {
        assert!(Instant::now() < deadline, "supervisor status deadline");
        sleep(Duration::from_millis(20)).await;
    }
}
#[test]
fn defaults_version_and_listener_validation() {
    let app = crate::HarnessApp::new("fixture").unwrap();
    assert_eq!(
        app.env("CLOUDFLARED_TOKEN"),
        "LISKOV_FIXTURE_CLOUDFLARED_TOKEN"
    );
    // The connector stays off unless the descriptor sets it, including when
    // the field is omitted and when it is round-tripped.
    let deployment: crate::deployment::Deployment = serde_json::from_str(
        r#"{
            "peer_config": "peer.json",
            "peer": {"path": "peer", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "bytes": 1},
            "server_files": [{"path": "llama-server", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "bytes": 1}],
            "server": "llama-server",
            "server_revision": "d2e54583c7452353eb35d40431281f6ee984332f",
            "model": {"path": "research.gguf", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "bytes": 1},
            "threads": 4,
            "min_available_memory_mib": 6144,
            "ready_seconds": 600,
            "download_seconds": 1800,
            "shutdown_seconds": 30,
            "model_port": 8081,
            "context_tokens": 4096
        }"#,
    )
    .unwrap();
    assert!(deployment.cloudflared.is_none());
    let parsed: crate::deployment::Deployment =
        serde_json::from_str(&serde_json::to_string(&deployment).unwrap()).unwrap();
    assert!(parsed.cloudflared.is_none());
    assert!(supported_version("cloudflared version 2026.10.2"));
    assert!(supported_version(
        "cloudflared version 2026.10.4 (built today)"
    ));
    assert!(!supported_version("cloudflared version 2026.10.1"));
    assert!(!supported_version("cloudflared version 2026.9.3"));
    assert!(!supported_version("cloudflared version 2025.4.0"));
    assert!(!supported_version("cloudflared version 2026.7.3"));
    let (_root, mut config, origin, reserved) = fixture("exit", "2026.10.4");
    config.validate(origin, &reserved).unwrap();
    for bad in ["0.0.0.0:20241", "127.0.0.1:0", "127.0.0.1:8081"] {
        config.metrics = bad.parse().unwrap();
        assert!(config.validate(origin, &reserved).is_err());
    }
}
#[tokio::test]
async fn preflight_rejects_bad_credentials_checksum_version_and_trust_store() {
    let _forks = FORKS.lock().await;
    let (_root, mut config, origin, reserved) = fixture("exit", "2024.1.0");
    assert!(
        prepare(&mut config, origin, &reserved, "", Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(
        prepare(
            &mut config,
            origin,
            &reserved,
            "synthetic-token",
            Duration::from_secs(1)
        )
        .await
        .is_err()
    );
    config.binary.sha256 = "0".repeat(64);
    assert!(
        prepare(
            &mut config,
            origin,
            &reserved,
            "synthetic-token",
            Duration::from_secs(1)
        )
        .await
        .is_err()
    );
    config.ca_bundle = "missing-ca.pem".into();
    assert!(
        prepare(
            &mut config,
            origin,
            &reserved,
            "synthetic-token",
            Duration::from_secs(1)
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn readiness_loss_restarts_only_connector_and_shutdown_cleans_private_files() {
    let _forks = FORKS.lock().await;
    let (root, mut config, origin, reserved) = fixture("healthy", "2026.10.4");
    let prepared = prepare(
        &mut config,
        origin,
        &reserved,
        "synthetic-token",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let private = prepared.private.path().to_path_buf();
    let state = Arc::new(Mutex::new(Status::new(true)));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(prepared.supervise(state.clone(), rx, quick()));
    until(&state, |s| s.phase == "connected").await;
    let first = state.lock().await.pid;
    std::fs::write(root.path().join("unready"), "1").unwrap();
    until(&state, |s| s.restarts >= 1).await;
    std::fs::remove_file(root.path().join("unready")).unwrap();
    until(&state, |s| s.phase == "connected").await;
    assert_ne!(state.lock().await.pid, first);
    tx.send(true).unwrap();
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.lock().await.phase, "stopped");
    assert!(!private.exists());
    assert!(root.path().join("terminated").exists());
    assert!(std::net::TcpListener::bind(config.metrics).is_ok());
}
#[test]
fn delay_doubles_per_unstable_attempt_up_to_the_cap() {
    let policy = Policy::default();
    let seconds: Vec<u64> = (1..=8)
        .map(|streak| policy.delay(streak).as_secs())
        .collect();
    assert_eq!(seconds, [1, 2, 4, 8, 16, 32, 60, 60]);
    assert_eq!(policy.delay(0), Duration::from_secs(1));
    assert_eq!(policy.delay(u32::MAX), Duration::from_secs(60));
    assert_eq!(quick().delay(3), Duration::from_millis(40));
}
#[tokio::test]
async fn exits_are_retried_past_six_attempts_with_a_capped_delay() {
    let _forks = FORKS.lock().await;
    let (root, mut config, origin, reserved) = fixture("exit", "2026.10.4");
    let prepared = prepare(
        &mut config,
        origin,
        &reserved,
        "synthetic-token",
        Duration::ZERO,
    )
    .await
    .unwrap();
    let state = Arc::new(Mutex::new(Status::new(true)));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(prepared.supervise(state.clone(), rx, quick()));
    until(&state, |s| s.restarts >= 8 && s.phase == "reconnecting").await;
    let snapshot = state.lock().await.clone();
    assert!(snapshot.pid.is_none());
    assert_eq!(snapshot.error_code, Some("child_exited"));
    assert_eq!(snapshot.exit_code, Some(7));
    assert_eq!(snapshot.backoff_ms, 40);
    assert!(snapshot.last_connected_at_ms.is_none());
    tx.send(true).unwrap();
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.lock().await.phase, "stopped");
    assert!(
        std::fs::read_to_string(root.path().join("spawns"))
            .unwrap()
            .lines()
            .count()
            >= 9
    );
}
#[tokio::test]
async fn a_stable_connection_resets_the_delay() {
    let _forks = FORKS.lock().await;
    let (root, mut config, origin, reserved) = fixture("healthy", "2026.10.4");
    let prepared = prepare(
        &mut config,
        origin,
        &reserved,
        "synthetic-token",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let state = Arc::new(Mutex::new(Status::new(true)));
    let (tx, rx) = watch::channel(false);
    let mut policy = quick();
    policy.ready = Duration::from_millis(200);
    std::fs::write(root.path().join("unready"), "1").unwrap();
    let task = tokio::spawn(prepared.supervise(state.clone(), rx, policy));
    until(&state, |s| s.restarts >= 2 && s.phase == "reconnecting").await;
    let snapshot = state.lock().await.clone();
    assert_eq!(snapshot.error_code, Some("readiness_timeout"));
    assert_eq!(snapshot.backoff_ms, 40);
    std::fs::remove_file(root.path().join("unready")).unwrap();
    until(&state, |s| s.phase == "connected").await;
    sleep(Duration::from_millis(400)).await;
    let before = state.lock().await.restarts;
    std::fs::write(root.path().join("unready"), "1").unwrap();
    until(&state, |s| s.restarts > before).await;
    assert_eq!(state.lock().await.backoff_ms, 10);
    std::fs::remove_file(root.path().join("unready")).unwrap();
    until(&state, |s| s.phase == "connected").await;
    tx.send(true).unwrap();
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.lock().await.phase, "stopped");
}
#[tokio::test]
async fn shutdown_during_startup_forces_and_reaps_uncooperative_connector() {
    let _forks = FORKS.lock().await;
    let (root, mut config, origin, reserved) = fixture("hang", "2026.10.4");
    let prepared = prepare(
        &mut config,
        origin,
        &reserved,
        "synthetic-token",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let private = prepared.private.path().to_path_buf();
    let state = Arc::new(Mutex::new(Status::new(true)));
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(prepared.supervise(state.clone(), rx, quick()));
    let deadline = Instant::now() + Duration::from_secs(4);
    while !root.path().join("spawns").exists() {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(20)).await;
    }
    tx.send(true).unwrap();
    timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!private.exists());
    assert_eq!(state.lock().await.phase, "stopped");
}
#[tokio::test]
async fn shutdown_during_backoff_cannot_start_another_child() {
    let _forks = FORKS.lock().await;
    let (root, mut config, origin, reserved) = fixture("exit", "2026.10.4");
    let prepared = prepare(
        &mut config,
        origin,
        &reserved,
        "synthetic-token",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let state = Arc::new(Mutex::new(Status::new(true)));
    let (tx, rx) = watch::channel(false);
    let mut policy = quick();
    policy.backoff = Duration::from_secs(5);
    let task = tokio::spawn(prepared.supervise(state.clone(), rx, policy));
    until(&state, |s| s.phase == "reconnecting").await;
    tx.send(true).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("spawns"))
            .unwrap()
            .lines()
            .count(),
        1
    );
}
