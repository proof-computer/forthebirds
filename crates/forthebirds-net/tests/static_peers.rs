use std::{
    collections::BTreeMap,
    net::Ipv6Addr,
    path::{Path, PathBuf},
    time::Duration,
};

use forthebirds_net::{
    PeerTarget, PeerTransport, StaticPeer, StaticPeers, TransportConfig, TransportState,
    pinned_client,
};

fn router() -> axum::Router {
    axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }))
}

fn config(path: PathBuf) -> TransportConfig {
    TransportConfig {
        listen: "[::1]:0".parse().unwrap(),
        advertisement_file: Some(path),
        service: "lab".into(),
        application: "lab".into(),
    }
}

fn peer(name: &str, path: impl AsRef<Path>) -> StaticPeer {
    StaticPeer {
        name: name.to_owned(),
        address: Ipv6Addr::LOCALHOST,
        advertisement_file: path.as_ref().to_path_buf(),
    }
}

fn code(map: &BTreeMap<String, forthebirds_net::Result<PeerTarget>>, name: &str) -> &'static str {
    map[name].as_ref().unwrap_err().code
}

fn pin() -> String {
    format!("sha256:{}", "0123456789abcdef".repeat(4))
}

fn advertisement(port: u16, ready: bool, fingerprint: &str, schema: &str) -> String {
    format!(
        r#"{{"schema":"{schema}","service":"lab","port":{port},"ready":{ready},"tlsFingerprint":"{fingerprint}","peerSyncFailureCode":"none"}}"#
    )
}

#[tokio::test]
async fn listed_transports_resolve_rotate_and_answer_healthz() {
    let root = tempfile::tempdir().unwrap();
    let transport_a = PeerTransport::start(
        &config(root.path().join("a.json")),
        &TransportState::default(),
        router(),
    )
    .await
    .unwrap();
    let transport_b = PeerTransport::start(
        &config(root.path().join("b.json")),
        &TransportState::default(),
        router(),
    )
    .await
    .unwrap();
    let peers = StaticPeers::new(vec![
        peer("a", transport_a.advertisement()),
        peer("b", transport_b.advertisement()),
    ])
    .unwrap();
    let found = peers.read();
    for (name, transport) in [("a", &transport_a), ("b", &transport_b)] {
        let target = found[name].as_ref().unwrap();
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(transport.advertisement()).unwrap()).unwrap();
        let port = raw["port"].as_u64().unwrap();
        let fingerprint = raw["tlsFingerprint"].as_str().unwrap();
        assert_eq!(target.url, format!("https://[::1]:{port}/"));
        assert_eq!(target.tls_fingerprint, fingerprint);
        assert_eq!(
            target.cursor_key(),
            format!("{name}|{fingerprint}|{}", target.url)
        );
        let client = pinned_client(&target.tls_fingerprint, Duration::from_secs(5)).unwrap();
        assert!(
            client
                .client
                .get(format!("{}healthz", target.url))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    let old_key = found["a"].as_ref().unwrap().cursor_key();
    let old_pin = found["a"].as_ref().unwrap().tls_fingerprint.clone();
    transport_a.rotate_now().await.unwrap();
    let after = peers.read();
    let rotated = after["a"].as_ref().unwrap().clone();
    assert_ne!(rotated.tls_fingerprint, old_pin);
    assert_ne!(rotated.cursor_key(), old_key);
    assert_eq!(
        rotated.cursor_key(),
        format!("a|{}|{}", rotated.tls_fingerprint, rotated.url)
    );
}

#[test]
fn read_classifies_bad_advertisements() {
    let root = tempfile::tempdir().unwrap();
    let fingerprint = pin();
    assert_eq!(fingerprint.len(), "sha256:".len() + 64);
    let cases = [
        (
            "ready-false",
            advertisement(1, false, &fingerprint, "liskov.peer-advertisement.v2"),
        ),
        (
            "schema",
            advertisement(1, true, &fingerprint, "liskov.peer-advertisement.v1"),
        ),
        (
            "port-zero",
            advertisement(0, true, &fingerprint, "liskov.peer-advertisement.v2"),
        ),
        (
            "upper-pin",
            advertisement(
                1,
                true,
                &format!(
                    "sha256:{}",
                    "0123456789abcdef".repeat(4).to_ascii_uppercase()
                ),
                "liskov.peer-advertisement.v2",
            ),
        ),
        (
            "short-pin",
            advertisement(1, true, "sha256:abcd", "liskov.peer-advertisement.v2"),
        ),
        ("json", "{".to_owned()),
    ];
    let mut peers = vec![peer("missing", root.path().join("missing.json"))];
    for (name, body) in &cases {
        let path = root.path().join(format!("{name}.json"));
        std::fs::write(&path, body).unwrap();
        peers.push(peer(name, path));
    }
    let mut exact =
        advertisement(1, true, &fingerprint, "liskov.peer-advertisement.v2").into_bytes();
    exact.resize(64 * 1024, b' ');
    let mut over = exact.clone();
    over.push(b' ');
    std::fs::write(root.path().join("exact.json"), &exact).unwrap();
    std::fs::write(root.path().join("over.json"), &over).unwrap();
    peers.push(peer("exact", root.path().join("exact.json")));
    peers.push(peer("over", root.path().join("over.json")));
    let found = StaticPeers::new(peers).unwrap().read();
    assert_eq!(code(&found, "missing"), "discovery_unavailable");
    for name in [
        "ready-false",
        "schema",
        "port-zero",
        "upper-pin",
        "short-pin",
        "json",
        "over",
    ] {
        assert_eq!(code(&found, name), "discovery_invalid", "{name}");
    }
    let exact_target = found["exact"].as_ref().unwrap();
    assert_eq!(exact_target.url, "https://[::1]:1/");
    assert_eq!(exact_target.tls_fingerprint, fingerprint);
    assert_unix_specials(root.path(), &fingerprint);
}

#[cfg(unix)]
fn assert_unix_specials(root: &Path, fingerprint: &str) {
    let valid = root.join("valid.json");
    std::fs::write(
        &valid,
        advertisement(9, true, fingerprint, "liskov.peer-advertisement.v2"),
    )
    .unwrap();
    let link = root.join("link.json");
    std::os::unix::fs::symlink(&valid, &link).unwrap();
    let fifo = root.join("fifo");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let peers = StaticPeers::new(vec![peer("link", &link), peer("fifo", &fifo)]).unwrap();
    let started = std::time::Instant::now();
    let found = peers.read();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(code(&found, "link"), "discovery_invalid");
    assert_eq!(code(&found, "fifo"), "discovery_invalid");
}

#[cfg(not(unix))]
fn assert_unix_specials(_root: &Path, _fingerprint: &str) {}

#[test]
fn new_checks_names_and_static_peer_round_trips() {
    let path = PathBuf::from("a.json");
    assert_eq!(
        StaticPeers::new(vec![peer("same", &path), peer("same", &path)])
            .unwrap_err()
            .code,
        "config"
    );
    assert_eq!(
        StaticPeers::new(vec![peer("", &path)]).unwrap_err().code,
        "config"
    );
    assert_eq!(
        StaticPeers::new(vec![peer("a b", &path)]).unwrap_err().code,
        "config"
    );
    assert_eq!(
        StaticPeers::new(vec![StaticPeer {
            name: "a".into(),
            address: Ipv6Addr::LOCALHOST,
            advertisement_file: PathBuf::new(),
        }])
        .unwrap_err()
        .code,
        "config"
    );
    let many: Vec<_> = (0..257)
        .map(|index| peer(&format!("n{index}"), &path))
        .collect();
    assert_eq!(StaticPeers::new(many).unwrap_err().code, "config");
    assert!(StaticPeers::new(vec![]).unwrap().read().is_empty());

    let peers = vec![peer("lab-a", "/tmp/a.json")];
    let encoded = serde_json::to_string(&peers).unwrap();
    let decoded: Vec<StaticPeer> = serde_json::from_str(&encoded).unwrap();
    assert_eq!(peers, decoded);
    let unknown = r#"[{"name":"a","address":"::1","advertisement_file":"a.json","extra":1}]"#;
    assert!(serde_json::from_str::<Vec<StaticPeer>>(unknown).is_err());
}
