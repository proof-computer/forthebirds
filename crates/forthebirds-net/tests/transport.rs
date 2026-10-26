use std::time::Duration;

use forthebirds_net::{
    PeerTransport, TransportConfig, TransportState, pinned_client, publish_summary,
};

fn router() -> axum::Router {
    axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }))
}

fn config(path: std::path::PathBuf, listen: &str) -> TransportConfig {
    TransportConfig {
        listen: listen.parse().unwrap(),
        advertisement_file: Some(path),
        service: "swarm".into(),
        application: "planetarium".into(),
    }
}

#[tokio::test]
async fn two_listeners_pin_rotate_summarise_and_stop() {
    let root = tempfile::tempdir().unwrap();
    let state_a = TransportState::default();
    let state_b = TransportState::default();
    let transport_a = PeerTransport::start(
        &config(root.path().join("a.json"), "[::1]:0"),
        &state_a,
        router(),
    )
    .await
    .unwrap();
    let transport_b = PeerTransport::start(
        &config(root.path().join("b.json"), "[::1]:0"),
        &state_b,
        router(),
    )
    .await
    .unwrap();

    let ad_a: serde_json::Value =
        serde_json::from_slice(&std::fs::read(transport_a.advertisement()).unwrap()).unwrap();
    let ad_b: serde_json::Value =
        serde_json::from_slice(&std::fs::read(transport_b.advertisement()).unwrap()).unwrap();
    assert_eq!(ad_a["schema"], "liskov.peer-advertisement.v2");
    assert_eq!(ad_b["schema"], "liskov.peer-advertisement.v2");
    assert_eq!(ad_a["ready"], true);
    assert_eq!(ad_b["ready"], true);
    let port_a = ad_a["port"].as_u64().unwrap() as u16;
    let port_b = ad_b["port"].as_u64().unwrap() as u16;
    let pin_a = ad_a["tlsFingerprint"].as_str().unwrap();
    let pin_b = ad_b["tlsFingerprint"].as_str().unwrap();
    assert_ne!(port_a, port_b);
    assert_ne!(pin_a, pin_b);
    assert!(pin_a.starts_with("sha256:"));
    assert!(pin_b.starts_with("sha256:"));

    let status_a = state_a.status.lock().await.clone();
    let status_b = state_b.status.lock().await.clone();
    assert!(status_a.listener_ready);
    assert!(status_b.listener_ready);
    assert_eq!(status_a.listener_port, Some(port_a));
    assert_eq!(status_b.listener_port, Some(port_b));
    assert_eq!(status_a.tls_fingerprint.as_deref(), Some(pin_a));
    assert_eq!(status_b.tls_fingerprint.as_deref(), Some(pin_b));

    let key_dir = transport_a.key_dir().await;
    assert!(
        key_dir
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("planetarium-peer-key-")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&key_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(key_dir.join("key.der"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    let client = pinned_client(pin_a, Duration::from_secs(5)).unwrap();
    let base = format!("https://[::1]:{port_a}");
    assert!(
        client
            .client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let wrong = pinned_client(pin_b, Duration::from_secs(5)).unwrap();
    let mismatched = wrong
        .client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(wrong.request_error(&mismatched).code, "tls");
    assert!(std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port_a)).is_err());

    let occupied = match PeerTransport::start(
        &config(root.path().join("c.json"), &format!("[::1]:{port_a}")),
        &TransportState::default(),
        router(),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("listener bound to an occupied port"),
    };
    assert_eq!(occupied.code, "peer_listener");
    let ipv4 = match PeerTransport::start(
        &config(root.path().join("v4.json"), "127.0.0.1:0"),
        &TransportState::default(),
        router(),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("IPv4 listener was accepted"),
    };
    assert_eq!(ipv4.code, "config");

    transport_a.rotate_now().await.unwrap();
    let rotated: serde_json::Value =
        serde_json::from_slice(&std::fs::read(transport_a.advertisement()).unwrap()).unwrap();
    let rotated_pin = rotated["tlsFingerprint"].as_str().unwrap();
    assert_ne!(pin_a, rotated_pin);
    assert_eq!(
        state_a.status.lock().await.tls_fingerprint.as_deref(),
        Some(rotated_pin)
    );
    assert!(!key_dir.exists(), "previous private key was withdrawn");
    let old_client = pinned_client(pin_a, Duration::from_secs(5)).unwrap();
    assert!(
        old_client
            .client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_err()
    );
    let fresh = pinned_client(rotated_pin, Duration::from_secs(5)).unwrap();
    assert!(
        fresh
            .client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let key_dir = transport_a.key_dir().await;
    assert!(
        key_dir
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("planetarium-peer-key-")
    );

    let summary_config = config(transport_a.advertisement().to_owned(), "[::1]:0");
    publish_summary(&summary_config, &state_a, Some(42), "unknown").await;
    let summarised: serde_json::Value =
        serde_json::from_slice(&std::fs::read(transport_a.advertisement()).unwrap()).unwrap();
    assert_eq!(summarised["peerSyncFailureCode"], "exchange");
    assert_eq!(summarised["peerSyncLastSuccessAtMs"], 42);
    publish_summary(&summary_config, &state_a, Some(42), "tls").await;
    let kept: serde_json::Value =
        serde_json::from_slice(&std::fs::read(transport_a.advertisement()).unwrap()).unwrap();
    assert_eq!(kept["peerSyncFailureCode"], "tls");

    let advertisement = transport_a.advertisement().to_owned();
    drop(client);
    drop(wrong);
    drop(old_client);
    drop(fresh);
    transport_a.stop().await.unwrap();
    transport_b.stop().await.unwrap();
    assert!(!advertisement.exists());
    assert!(!key_dir.exists());
    assert!(!state_a.status.lock().await.listener_ready);
    assert!(!root.path().join("b.json").exists());
}
