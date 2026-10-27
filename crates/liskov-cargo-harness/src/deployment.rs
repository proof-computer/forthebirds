//! Application-owned Cargo entrypoint. Inputs are supplied and verified before
//! any subprocess starts; models may be fetched through the bounded downloader.
//! No runtime build, tunnel provisioning or platform mutation happens here.
use crate::{Error, HarnessApp, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    time::{Instant, sleep, timeout},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

fn default_cache_ram_mib() -> u64 {
    1024
}

/// Inference server the descriptor used to point at through the peer config.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Embedding {
    pub model: Input,
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub peer_config: PathBuf,
    pub peer: Input,
    /// Includes llama-server and every adjacent shared library in its release.
    pub server_files: Vec<Input>,
    #[serde(default)]
    pub extra_files: Vec<Input>,
    pub server: PathBuf,
    /// Exact upstream commit used to build the CPU-only server.
    pub server_revision: String,
    pub model: Input,
    pub threads: u8,
    /// Admission floor chosen from the model/context peak, not GGUF file size.
    pub min_available_memory_mib: u64,
    pub ready_seconds: u64,
    pub download_seconds: u64,
    pub shutdown_seconds: u64,
    pub model_port: u16,
    /// Research-server context. The embedding server keeps its fixed 512.
    pub context_tokens: u32,
    /// Research-server `--cache-ram`, in MiB. The embedding server sets none.
    #[serde(default = "default_cache_ram_mib")]
    pub cache_ram_mib: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Embedding>,
    /// The peer's own listeners. At most eight. When a connector is configured,
    /// the first entry is the loopback port Planetarium read from `listen`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflared: Option<crate::cloudflared::CloudflaredConfig>,
}

fn fail(message: &str) -> Error {
    Error::new("deployment", message)
}

pub(crate) fn verify(input: &Input) -> Result<()> {
    if input.sha256.len() != 64
        || !input.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || input.bytes == 0
        || input.bytes > 32 * 1024 * 1024 * 1024
    {
        return Err(fail("invalid input digest/size"));
    }
    let mut file =
        std::fs::File::open(&input.path).map_err(|_| fail("cannot open pinned input"))?;
    let metadata = file
        .metadata()
        .map_err(|_| fail("cannot inspect pinned input"))?;
    if !metadata.is_file() || metadata.len() != input.bytes {
        return Err(fail("pinned input size mismatch"));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| fail("cannot hash pinned input"))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    if hex::encode(hasher.finalize()) != input.sha256.to_ascii_lowercase() {
        return Err(fail("pinned input checksum mismatch"));
    }
    Ok(())
}

#[cfg(test)]
fn local_port(value: &str) -> Result<u16> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| fail("supervised endpoints must be http://127.0.0.1:<port>/"))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.path() != "/"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(fail(
            "supervised endpoints must be http://127.0.0.1:<port>/",
        ));
    }
    url.port()
        .filter(|port| *port != 0)
        .ok_or_else(|| fail("supervised endpoint requires an explicit port"))
}

impl Deployment {
    fn load(path: &Path) -> Result<Self> {
        let root = path.parent().unwrap_or(Path::new("."));
        let mut deployment: Self = serde_json::from_slice(
            &std::fs::read(path).map_err(|_| fail("cannot read deployment"))?,
        )
        .map_err(|_| fail("invalid deployment JSON"))?;
        // Bundle-relative paths resolve beside the descriptor, independent of cwd.
        for path in std::iter::once(&mut deployment.peer_config)
            .chain(std::iter::once(&mut deployment.server))
            .chain(std::iter::once(&mut deployment.peer.path))
            .chain(std::iter::once(&mut deployment.model.path))
            .chain(
                deployment
                    .server_files
                    .iter_mut()
                    .chain(deployment.extra_files.iter_mut())
                    .map(|input| &mut input.path),
            )
            .chain(
                deployment
                    .embedding
                    .as_mut()
                    .map(|embedding| &mut embedding.model.path),
            )
        {
            if path.is_relative() {
                *path = root.join(&*path);
            }
        }
        if let Some(cloud) = &mut deployment.cloudflared {
            cloud.resolve_paths(root);
        }
        deployment.validate()?;
        Ok(deployment)
    }

    fn validate(&self) -> Result<()> {
        if !(512..=32768).contains(&self.min_available_memory_mib)
            || !(1..=8).contains(&self.threads)
            || !(1..=1800).contains(&self.ready_seconds)
            || !(1..=3600).contains(&self.download_seconds)
            || self.peer.url.is_some()
            || self
                .server_files
                .iter()
                .chain(self.extra_files.iter())
                .any(|input| input.url.is_some())
            || self.extra_files.len() > 64
            || !(1..=300).contains(&self.shutdown_seconds)
            || self.server_revision.len() != 40
            || !self.server_revision.bytes().all(|b| b.is_ascii_hexdigit())
            || self.server_files.is_empty()
            || self.server_files.len() > 64
            || !self
                .server_files
                .iter()
                .any(|input| input.path == self.server)
            || !(512..=65_536).contains(&self.context_tokens)
            || self.cache_ram_mib > 4096
            || self.reserved_ports.len() > 8
        {
            return Err(fail("invalid supervision limits or input inventory"));
        }
        let mut ports = vec![self.model_port];
        if let Some(embedding) = &self.embedding {
            ports.push(embedding.port);
        }
        ports.extend(self.reserved_ports.iter().copied());
        if let Some(cloud) = &self.cloudflared {
            ports.push(cloud.metrics.port());
        }
        if ports.contains(&0) {
            return Err(fail("supervised port must not be 0"));
        }
        let mut seen = ports.clone();
        seen.sort_unstable();
        seen.dedup();
        if seen.len() != ports.len() {
            return Err(fail("peer/model port collision"));
        }
        Ok(())
    }

    fn server_args(&self, embedding: bool) -> Result<Vec<String>> {
        let (input, port, context, batch, micro) = if embedding {
            let embedding = self
                .embedding
                .as_ref()
                .ok_or_else(|| fail("missing embedding input"))?;
            (&embedding.model, embedding.port, 512, 512, 512)
        } else {
            (&self.model, self.model_port, self.context_tokens, 512, 128)
        };
        let mut args = vec![
            "-m".into(),
            input.path.to_string_lossy().into_owned(),
            "--host".into(),
            "127.0.0.1".into(),
            "--port".into(),
            port.to_string(),
            "-t".into(),
            self.threads.to_string(),
            "-tb".into(),
            self.threads.to_string(),
            "-c".into(),
            context.to_string(),
            "-np".into(),
            "1".into(),
            "-ngl".into(),
            "0".into(),
            "-b".into(),
            batch.to_string(),
            "-ub".into(),
            micro.to_string(),
            "--threads-http".into(),
            "2".into(),
            "--no-webui".into(),
            // Keep the weights on the file-backed mmap path. A server built with
            // dotprod or i8mm otherwise repacks every Q4_K/Q4_0 tensor into
            // anonymous memory at load, and the phones kill the job for it
            // (job 285633 on 5DH3, 2026-10-12).
            "--no-repack".into(),
        ];
        if embedding {
            args.extend(["--embeddings".into(), "--pooling".into(), "cls".into()]);
        } else {
            args.extend(["--no-mmproj".into(), "--reasoning".into(), "off".into()]);
            // The RAM prompt cache defaults to 8 GiB, more than a phone has
            // free once the weights are mapped. An entry holds a slot's KV
            // (32 KiB a token for Qwen3.5-9B: 256 MiB at 8,192) plus ~48 MiB
            // of recurrent state per checkpoint, so this keeps a few saved
            // prompts — the system-prompt reuse after a code summary — and no
            // more. The descriptor sets the bound; 1024 is the default.
            args.extend(["--cache-ram".into(), self.cache_ram_mib.to_string()]);
        }
        Ok(args)
    }
}

async fn ready(child: &mut Child, url: &str, seconds: u64, client: &reqwest::Client) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if child
            .try_wait()
            .map_err(|_| fail("cannot observe inference process"))?
            .is_some()
        {
            return Err(fail("inference process exited before ready"));
        }
        if Instant::now() >= deadline {
            return Err(fail("inference readiness timeout"));
        }
        if client
            .get(format!("{url}health"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

pub(crate) async fn stop(child: &mut Child, grace: Duration) -> Result<()> {
    if child
        .try_wait()
        .map_err(|_| fail("cannot observe child shutdown"))?
        .is_some()
    {
        return Ok(());
    }
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        use nix::{
            sys::signal::{Signal, kill},
            unistd::Pid,
        };
        // This PID is owned and unreaped. Never signal a discovered/external process.
        match kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(_) => {
                child.start_kill().map_err(|_| fail("cannot stop child"))?;
            }
        }
    }
    #[cfg(not(unix))]
    child.start_kill().map_err(|_| fail("cannot stop child"))?;
    match timeout(grace, child.wait()).await {
        Ok(result) => {
            result.map_err(|_| fail("cannot reap child"))?;
        }
        Err(_) => {
            child
                .kill()
                .await
                .map_err(|_| fail("cannot kill/reap child after shutdown deadline"))?;
        }
    }
    Ok(())
}

fn hide_launch_secrets(command: &mut Command, app: &HarnessApp) {
    let peer_token = app.env("PEER_TOKEN");
    let tunnel_token = app.env("CLOUDFLARED_TOKEN");
    for (key, _) in std::env::vars_os() {
        let text = key.to_string_lossy();
        if text.starts_with("LLAMA_")
            || text.starts_with("TUNNEL_")
            || text == peer_token
            || text == tunnel_token
        {
            command.env_remove(key);
        }
    }
}

fn spawn_server(app: &HarnessApp, deployment: &Deployment, embedding: bool) -> Result<Child> {
    let mut command = Command::new(&deployment.server);
    command
        .env(
            "LD_LIBRARY_PATH",
            deployment
                .server
                .parent()
                .ok_or_else(|| fail("server has no release directory"))?,
        )
        .args(deployment.server_args(embedding)?)
        .kill_on_drop(true);
    // Avoid llama.cpp's inherited env silently overriding the pinned launch envelope.
    hide_launch_secrets(&mut command, app);
    command
        .spawn()
        .map_err(|_| fail("cannot start inference server"))
}

fn peer_command(
    app: &HarnessApp,
    peer: &Path,
    config: &Path,
    cwd: &Path,
    shutdown_seconds: u64,
) -> Result<Command> {
    let config = config
        .canonicalize()
        .map_err(|_| fail("cannot resolve peer config"))?;
    let peer = peer
        .canonicalize()
        .map_err(|_| fail("cannot resolve peer executable"))?;
    let mut command = Command::new(peer);
    command
        .args((app.peer_args)(&config))
        .current_dir(cwd)
        .env(
            app.env("PARENT_SHUTDOWN_SECONDS"),
            shutdown_seconds.to_string(),
        )
        .kill_on_drop(true);
    Ok(command)
}

/// `MemAvailable` from `/proc/meminfo` text, in KiB. Unreadable text and a
/// missing or unparseable value stay distinct errors.
fn available_memory_kib(meminfo: &str) -> Result<u64> {
    meminfo
        .lines()
        .find_map(|line| {
            let mut words = line.split_whitespace();
            (words.next()? == "MemAvailable:")
                .then(|| words.next()?.parse::<u64>().ok())
                .flatten()
        })
        .ok_or_else(|| fail("processor available memory is unknown"))
}

/// Admits the pinned envelope only when the processor has at least
/// `floor_mib` available. The reading is logged at every admission point, and a
/// refusal names both figures so the log says how far short the phone fell.
fn admit_memory(name: &str, meminfo: &str, floor_mib: u64, point: &str) -> Result<()> {
    let available_mib = available_memory_kib(meminfo)? / 1024;
    crate::log_line!(
        "{name} deployment stage=memory-admission point={point} available_mib={available_mib} floor_mib={floor_mib}"
    );
    if available_mib < floor_mib {
        return Err(fail(&format!(
            "insufficient available memory for pinned deployment envelope: available {available_mib} MiB, floor {floor_mib} MiB"
        )));
    }
    Ok(())
}

fn admit_processor_memory(name: &str, floor_mib: u64, point: &str) -> Result<()> {
    let memory = std::fs::read_to_string("/proc/meminfo")
        .map_err(|_| fail("cannot read processor memory admission"))?;
    admit_memory(name, &memory, floor_mib, point)
}

pub async fn run(app: &HarnessApp, path: &Path, shutdown: impl Future<Output = ()>) -> Result<()> {
    let deployment = Deployment::load(path)?;
    let token = std::env::var(app.env("PEER_TOKEN")).map_err(|_| fail("missing peer token"))?;
    if !(24..=256).contains(&token.len()) || !token.bytes().all(|byte| (33..=126).contains(&byte)) {
        return Err(fail("invalid peer token"));
    }
    tokio::pin!(shutdown);
    let inputs = async {
        if let Some(cloud) = &deployment.cloudflared {
            // The harness does not parse the peer config. The descriptor's first
            // reserved port is the listener Planetarium took from `config.listen`,
            // and the connector origin stays on loopback.
            let origin = std::net::SocketAddr::from((
                std::net::Ipv4Addr::LOCALHOST,
                deployment.reserved_ports.first().copied().unwrap_or(0),
            ));
            let _checked = crate::cloudflared::Prepared::from_env(
                app,
                cloud,
                origin,
                &deployment.reserved_ports,
            )
            .await?;
        }
        for input in deployment
            .server_files
            .iter()
            .chain(deployment.extra_files.iter())
            .chain(std::iter::once(&deployment.peer))
        {
            verify(input)?;
        }
        for input in std::iter::once(&deployment.model).chain(
            deployment
                .embedding
                .as_ref()
                .map(|embedding| &embedding.model),
        ) {
            // Validate identity before trusting it as a transfer bound.
            if input.bytes == 0
                || input.bytes > 32 * 1024 * 1024 * 1024
                || input.sha256.len() != 64
                || hex::decode(&input.sha256).is_err()
            {
                return Err(fail("invalid model download identity/size"));
            }
        }
        // Refuse a phone that is already short before spending minutes and
        // gigabytes on a model it will not be allowed to serve.
        admit_processor_memory(
            app.name(),
            deployment.min_available_memory_mib,
            "before-model-download",
        )?;
        for input in std::iter::once(&deployment.model).chain(
            deployment
                .embedding
                .as_ref()
                .map(|embedding| &embedding.model),
        ) {
            crate::deployment_download::populate(input, app.name(), deployment.download_seconds)
                .await?;
        }
        Ok(())
    };
    // Register shutdown handling before a cached-input fast path can finish
    // synchronously and start children. Polling inputs first could leave a
    // narrow default-SIGTERM window with an owned server already running.
    tokio::select! { biased; _ = &mut shutdown => return Ok(()), result = inputs => result? }
    // Re-read after the transfer: the model is on disk now and the phone's
    // other residents may have grown while it downloaded.
    admit_processor_memory(
        app.name(),
        deployment.min_available_memory_mib,
        "after-model-download",
    )?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|_| fail("cannot create readiness client"))?;
    // Refuse an occupied listener rather than adopting someone else's model.
    for port in std::iter::once(deployment.model_port).chain(
        deployment
            .embedding
            .as_ref()
            .map(|embedding| embedding.port),
    ) {
        std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .map_err(|_| fail("inference port already occupied"))?;
    }
    let mut children = Vec::new();
    let session = async {
        children.push(spawn_server(app, &deployment, false)?);
        let model_url = format!("http://127.0.0.1:{}/", deployment.model_port);
        ready(
            children.last_mut().unwrap(),
            &model_url,
            deployment.ready_seconds,
            &client,
        )
        .await?;
        crate::log_line!("{} deployment stage=model-ready", app.name());
        if let Some(embedding) = &deployment.embedding {
            children.push(spawn_server(app, &deployment, true)?);
            let embedding_url = format!("http://127.0.0.1:{}/", embedding.port);
            ready(
                children.last_mut().unwrap(),
                &embedding_url,
                deployment.ready_seconds,
                &client,
            )
            .await?;
            crate::log_line!("{} deployment stage=embedding-ready", app.name());
        }
        // Pass absolute paths using the child working directory, without generating a config.
        // Relative storage paths in the original config resolve to this same directory.
        let root = path
            .canonicalize()
            .map_err(|_| fail("cannot resolve deployment path"))?;
        let root = root.parent().unwrap();
        children.push(
            peer_command(
                app,
                &deployment.peer.path,
                &deployment.peer_config,
                root,
                deployment.shutdown_seconds,
            )?
            .spawn()
            .map_err(|_| fail("cannot start peer"))?,
        );
        crate::log_line!("{} deployment stage=peer-started", app.name());
        loop {
            for child in &mut children {
                if child
                    .try_wait()
                    .map_err(|_| fail("cannot observe supervised child"))?
                    .is_some()
                {
                    return Err(fail("supervised child exited; stopping deployment"));
                }
            }
            sleep(Duration::from_millis(250)).await;
        }
    };
    let outcome = tokio::select! { result = session => result, _ = &mut shutdown => Ok(()) };
    // Peer first: it needs its inference servers during graceful drain.
    let mut cleanup_error = None;
    for child in children.iter_mut().rev() {
        if let Err(error) = stop(child, Duration::from_secs(deployment.shutdown_seconds)).await {
            cleanup_error = Some(error);
        }
    }
    crate::log_line!("{} deployment stage=stopped", app.name());
    outcome.and(cleanup_error.map_or(Ok(()), Err))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned(name: &str) -> Input {
        Input {
            path: PathBuf::from(name),
            sha256: "0".repeat(64),
            bytes: 1,
            url: None,
        }
    }

    fn supervised() -> Deployment {
        Deployment {
            peer_config: PathBuf::from("peer.json"),
            peer: pinned("planetarium"),
            server_files: vec![pinned("llama-server")],
            extra_files: vec![],
            server: PathBuf::from("llama-server"),
            server_revision: "d2e54583c7452353eb35d40431281f6ee984332f".into(),
            model: pinned("research.gguf"),
            threads: 4,
            min_available_memory_mib: 6144,
            ready_seconds: 600,
            download_seconds: 1800,
            shutdown_seconds: 30,
            model_port: 8081,
            context_tokens: 4096,
            cache_ram_mib: 1024,
            embedding: Some(Embedding {
                model: pinned("embedding.gguf"),
                port: 8082,
            }),
            reserved_ports: vec![8840],
            cloudflared: None,
        }
    }

    fn flag<'a>(args: &'a [String], name: &str) -> &'a str {
        let index = args.iter().position(|arg| arg == name).unwrap();
        args[index + 1].as_str()
    }

    #[test]
    fn both_servers_keep_weights_on_the_mmap_path() {
        let deployment = supervised();
        for embedding in [false, true] {
            let args = deployment.server_args(embedding).unwrap();
            assert!(args.iter().any(|arg| arg == "--no-repack"), "{args:?}");
            assert!(!args.iter().any(|arg| arg == "--no-mmap"), "{args:?}");
            let cache = args.iter().position(|arg| arg == "--cache-ram");
            if embedding {
                assert!(cache.is_none(), "{args:?}");
            } else {
                assert_eq!(args[cache.expect("bounded prompt cache") + 1], "1024");
            }
        }
    }

    #[test]
    fn server_args_come_from_the_descriptor() {
        let mut deployment = supervised();
        deployment.context_tokens = 2048;
        deployment.model_port = 9001;
        deployment.cache_ram_mib = 256;
        deployment.embedding = Some(Embedding {
            model: pinned("embedding.gguf"),
            port: 9002,
        });
        let research = deployment.server_args(false).unwrap();
        assert_eq!(flag(&research, "-c"), "2048");
        assert_eq!(flag(&research, "--port"), "9001");
        assert_eq!(flag(&research, "--cache-ram"), "256");
        assert!(research.iter().any(|arg| arg == "--no-repack"));
        assert!(!research.iter().any(|arg| arg == "--no-mmap"));
        assert!(research.iter().any(|arg| arg == "--no-mmproj"));
        let embedding = deployment.server_args(true).unwrap();
        assert_eq!(flag(&embedding, "-c"), "512");
        assert_eq!(flag(&embedding, "--port"), "9002");
        assert!(embedding.iter().any(|arg| arg == "--no-repack"));
        assert!(!embedding.iter().any(|arg| arg == "--no-mmap"));
        assert!(embedding.iter().all(|arg| arg != "--cache-ram"));
    }

    #[test]
    fn port_collisions_are_refused() {
        let mut deployment = supervised();
        deployment.embedding.as_mut().unwrap().port = deployment.model_port;
        assert!(deployment.validate().is_err());

        let mut deployment = supervised();
        deployment.reserved_ports = vec![deployment.model_port];
        assert!(deployment.validate().is_err());

        let mut deployment = supervised();
        deployment.model_port = 0;
        assert!(deployment.validate().is_err());
    }

    #[tokio::test]
    async fn peer_command_is_the_applications() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("record");
        let script_path = root.path().join("peer");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s\\n' \"$LISKOV_FIXTURE_PARENT_SHUTDOWN_SECONDS\" >> '{}'\n",
            record.display(),
            record.display()
        );
        std::fs::write(&script_path, script).unwrap();
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = root.path().join("peer.json");
        std::fs::write(&config, b"{}\n").unwrap();
        let mut app = crate::HarnessApp::new("fixture").unwrap();
        app.peer_args = |path| vec!["peer".into(), path.as_os_str().to_os_string()];
        let status = peer_command(&app, &script_path, &config, root.path(), 30)
            .unwrap()
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(status.success());
        let absolute = config.canonicalize().unwrap();
        assert_eq!(
            std::fs::read_to_string(record).unwrap(),
            format!("peer\n{}\n30\n", absolute.display())
        );
    }

    #[test]
    fn verify_size_digest_and_missing_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input");
        std::fs::write(&path, b"pinned").unwrap();
        let mut input = Input {
            path,
            bytes: 6,
            url: None,
            sha256: hex::encode(Sha256::digest(b"pinned")),
        };
        verify(&input).unwrap();
        input.bytes = 5;
        assert!(verify(&input).is_err());
        input.bytes = 6;
        input.sha256 = "0".repeat(64);
        assert!(verify(&input).is_err());
        std::fs::remove_file(&input.path).unwrap();
        assert!(verify(&input).is_err());
    }

    #[test]
    fn memory_admission_names_both_figures() {
        let meminfo = "MemTotal:       11743512 kB\nMemFree:         512000 kB\nMemAvailable:    7765468 kB\nSwapTotal:       6291452 kB\n";
        assert_eq!(available_memory_kib(meminfo).unwrap(), 7_765_468);
        admit_memory("fixture", meminfo, 7168, "test").unwrap();
        admit_memory("fixture", meminfo, 7583, "test").unwrap();
        let refused = admit_memory("fixture", meminfo, 7584, "test")
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("insufficient available memory"),
            "{refused}"
        );
        assert!(
            refused.contains("available 7583 MiB, floor 7584 MiB"),
            "{refused}"
        );
        for broken in [
            "MemTotal: 1 kB\n",
            "MemAvailable: lots kB\n",
            "MemAvailable:\n",
            "",
        ] {
            let unknown = admit_memory("fixture", broken, 512, "test")
                .unwrap_err()
                .to_string();
            assert!(unknown.contains("available memory is unknown"), "{unknown}");
        }
    }

    #[test]
    fn only_explicit_loopback_endpoints() {
        assert_eq!(local_port("http://127.0.0.1:8081/").unwrap(), 8081);
        for url in [
            "http://example.com:8081/",
            "http://127.0.0.1/",
            "http://127.0.0.1:0/",
            "http://127.0.0.1:8081/model/",
            "https://127.0.0.1:8081/",
        ] {
            assert!(local_port(url).is_err());
        }
    }

    #[tokio::test]
    async fn shutdown_deadline_kills_and_reaps_uncooperative_child() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "trap '' TERM; exec sleep 60"])
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        sleep(Duration::from_millis(100)).await;
        stop(&mut child, Duration::from_secs(1)).await.unwrap();
        assert!(child.try_wait().unwrap().is_some());
    }
}
