//! An application maps its own file into the harness deployment, and
//! `log_line!` expands from another crate.

use liskov_cargo_harness::{
    Error, HarnessApp, Result,
    deployment::{self, Deployment},
    log_line,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
struct Wrapped {
    inner: Deployment,
}

fn fixture_descriptor(path: &Path) -> Result<Deployment> {
    let bytes =
        std::fs::read(path).map_err(|_| Error::new("deployment", "cannot read deployment"))?;
    let wrapped: Wrapped = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("deployment", "invalid deployment JSON"))?;
    let root = path.parent().unwrap_or(Path::new("."));
    Deployment::resolve(wrapped.inner, root)
}

fn refuse(_path: &Path) -> Result<Deployment> {
    Err(Error::new("config", "fixture refused"))
}

fn pinned(path: &str) -> serde_json::Value {
    serde_json::json!({
        "path": path,
        "sha256": "0".repeat(64),
        "bytes": 1
    })
}

fn inner(absolute_extra: &str, absolute_embedding: &str) -> serde_json::Value {
    serde_json::json!({
        "peer_config": "peer.json",
        "peer": pinned("peer"),
        "server_files": [pinned("llama-server")],
        "extra_files": [pinned(absolute_extra)],
        "server": "llama-server",
        "server_revision": "d2e54583c7452353eb35d40431281f6ee984332f",
        "model": pinned("research.gguf"),
        "threads": 4,
        "min_available_memory_mib": 6144,
        "ready_seconds": 600,
        "download_seconds": 1800,
        "shutdown_seconds": 30,
        "model_port": 8081,
        "context_tokens": 4096,
        "embedding": {
            "model": pinned(absolute_embedding),
            "port": 8082
        },
        "reserved_ports": [8840]
    })
}

fn assert_resolved(
    deployment: &Deployment,
    root: &Path,
    absolute_extra: &Path,
    absolute_embedding: &Path,
) {
    assert_eq!(deployment.peer_config, root.join("peer.json"));
    assert_eq!(deployment.peer.path, root.join("peer"));
    assert_eq!(deployment.server, root.join("llama-server"));
    assert_eq!(deployment.server_files[0].path, root.join("llama-server"));
    assert_eq!(deployment.model.path, root.join("research.gguf"));
    assert_eq!(deployment.extra_files[0].path, absolute_extra);
    assert_eq!(
        deployment.embedding.as_ref().unwrap().model.path,
        absolute_embedding
    );
}

#[test]
fn descriptor_joins_relative_paths_and_leaves_absolute_paths() {
    let root = tempfile::tempdir().unwrap();
    let absolute_extra = PathBuf::from("/opt/descriptor-fixture/extra.bin");
    let absolute_embedding = PathBuf::from("/opt/descriptor-fixture/embedding.gguf");
    let path = root.path().join("wrapped.json");
    let value = inner(
        absolute_extra.to_str().unwrap(),
        absolute_embedding.to_str().unwrap(),
    );
    std::fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({ "inner": value })).unwrap(),
    )
    .unwrap();

    let deployment = fixture_descriptor(&path).unwrap();
    assert_resolved(
        &deployment,
        root.path(),
        &absolute_extra,
        &absolute_embedding,
    );
}

#[test]
fn resolve_matches_read_and_refuses_a_port_collision() {
    let root = tempfile::tempdir().unwrap();
    let absolute_extra = PathBuf::from("/opt/descriptor-fixture/extra.bin");
    let absolute_embedding = PathBuf::from("/opt/descriptor-fixture/embedding.gguf");
    let value = inner(
        absolute_extra.to_str().unwrap(),
        absolute_embedding.to_str().unwrap(),
    );
    let plain = root.path().join("deployment.json");
    std::fs::write(&plain, serde_json::to_vec(&value).unwrap()).unwrap();

    let parsed: Deployment = serde_json::from_value(value.clone()).unwrap();
    let resolved = Deployment::resolve(parsed, root.path()).unwrap();
    let read = deployment::read(&plain).unwrap();
    assert_resolved(&resolved, root.path(), &absolute_extra, &absolute_embedding);
    assert_resolved(&read, root.path(), &absolute_extra, &absolute_embedding);

    let mut collision = value;
    collision["embedding"]["port"] = serde_json::json!(8081);
    let colliding: Deployment = serde_json::from_value(collision).unwrap();
    let error = Deployment::resolve(colliding, root.path()).unwrap_err();
    assert_eq!(error.message, "peer/model port collision");
}

#[tokio::test]
async fn run_uses_the_app_descriptor_before_the_token() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wrapped.json");
    let value = inner(
        "/opt/descriptor-fixture/extra.bin",
        "/opt/descriptor-fixture/embedding.gguf",
    );
    std::fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({ "inner": value })).unwrap(),
    )
    .unwrap();

    let mut app = HarnessApp::new("descriptorfixture").unwrap();
    app.descriptor = fixture_descriptor;
    let missing = deployment::run(&app, &path, std::future::pending())
        .await
        .unwrap_err();
    assert_eq!(missing.message, "missing peer token");

    let defaults = HarnessApp::new("descriptorfixture").unwrap();
    let invalid = deployment::run(&defaults, &path, std::future::pending())
        .await
        .unwrap_err();
    assert_eq!(invalid.message, "invalid deployment JSON");

    app.descriptor = refuse;
    let refused = deployment::run(&app, &path, std::future::pending())
        .await
        .unwrap_err();
    let expected = Error::new("config", "fixture refused");
    assert_eq!(refused.code, expected.code);
    assert_eq!(refused.message, expected.message);
}

#[test]
fn log_line_expands_outside_the_crate() {
    log_line!("descriptor fixture");
}
