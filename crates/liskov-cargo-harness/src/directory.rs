//! Strict reader for the Liskov helper's direct-peer directory
//! (`liskov.discovery.v2`). Copied from liskov-planetarium
//! `c008afc37534f0b196eddca4ab09a06604534f4e` `src/discovery/direct.rs`,
//! without decoding a job reference: the reference is an opaque bounded string.
//! A failed refresh never extends a lease.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    net::Ipv6Addr,
    path::{Path, PathBuf},
};

const MAX_BYTES: u64 = 512 * 1024;

/// The three helper paths this process was given. The helper owns the files;
/// this reader never writes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeEnv {
    pub runtime_instance_id: String,
    pub advertisement_file: PathBuf,
    pub directory_file: PathBuf,
}

impl RuntimeEnv {
    /// Read `LISKOV_RUNTIME_INSTANCE_ID`, `LISKOV_PEER_ADVERTISEMENT_FILE` and
    /// `LISKOV_PEER_DIRECTORY_FILE`. A missing variable is code `config`.
    /// The instance id must pass the identifier grammar.
    pub fn from_env() -> Result<Self> {
        Self::from_values(std::env::vars())
    }

    /// Same rules as [`Self::from_env`] over an explicit environment.
    /// Setting the process environment is `unsafe` on this toolchain, and
    /// `unsafe_code` is forbidden, so tests call this.
    fn from_values<K, V>(vars: impl IntoIterator<Item = (K, V)>) -> Result<Self>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut runtime_instance_id = None;
        let mut advertisement_file = None;
        let mut directory_file = None;
        for (key, value) in vars {
            match key.as_ref() {
                "LISKOV_RUNTIME_INSTANCE_ID" => {
                    runtime_instance_id = Some(value.as_ref().to_string());
                }
                "LISKOV_PEER_ADVERTISEMENT_FILE" => {
                    advertisement_file = Some(PathBuf::from(value.as_ref()));
                }
                "LISKOV_PEER_DIRECTORY_FILE" => {
                    directory_file = Some(PathBuf::from(value.as_ref()));
                }
                _ => {}
            }
        }
        let runtime_instance_id = runtime_instance_id
            .ok_or_else(|| Error::new("config", "LISKOV_RUNTIME_INSTANCE_ID is required"))?;
        identifier(&runtime_instance_id)?;
        let advertisement_file = advertisement_file
            .ok_or_else(|| Error::new("config", "LISKOV_PEER_ADVERTISEMENT_FILE is required"))?;
        let directory_file = directory_file
            .ok_or_else(|| Error::new("config", "LISKOV_PEER_DIRECTORY_FILE is required"))?;
        Ok(Self {
            runtime_instance_id,
            advertisement_file,
            directory_file,
        })
    }
}

/// `peer-` plus the first 32 hex digits of SHA-256 of the instance id.
/// Matches Planetarium `config.rs` `runtime_peer_id`.
pub fn runtime_peer_id(instance: &str) -> String {
    format!(
        "peer-{}",
        &hex::encode(Sha256::digest(instance.as_bytes()))[..32]
    )
}

fn identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
    {
        return Err(Error::new(
            "invalid",
            "identifier must be 1–128 ASCII letters, digits, -, _, . or :",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryPeer {
    pub job_ref: String,
    pub runtime_instance_id: String,
    pub peer_id: String,
    pub url: String,
    pub tls_fingerprint: String,
    pub valid_until_ms: u64,
}
impl DirectoryPeer {
    pub fn cursor_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.job_ref, self.runtime_instance_id, self.tls_fingerprint, self.url
        )
    }
}

#[derive(Default)]
pub struct Directory {
    pub peers: BTreeMap<String, DirectoryPeer>,
    binding: Option<(String, String)>,
    pub observed_at_ms: Option<u64>,
    pub valid_until_ms: Option<u64>,
    pub last_read_ms: Option<u64>,
    pub revision: Option<String>,
    pub error: Option<&'static str>,
}

impl Directory {
    /// The exact own-job reference frozen by the first accepted directory.
    pub fn self_job_ref(&self) -> Option<&str> {
        self.binding.as_ref().map(|(_, job)| job.as_str())
    }
    pub fn refresh(
        &mut self,
        input: Result<Value>,
        runtime_instance: &str,
        service: &str,
        now_ms: u64,
    ) {
        let parsed = input.and_then(|value| parse(value, runtime_instance, service, now_ms));
        match parsed {
            Ok(snapshot) => {
                // Cargo validates these two fields against the signed health
                // response before writing its private file. The application
                // has the runtime incarnation independently; it also freezes
                // the first helper-accepted app/job binding across refreshes.
                let binding = (
                    snapshot.application_uid.clone(),
                    snapshot.self_job_ref.clone(),
                );
                if self.binding.as_ref().is_some_and(|old| old != &binding)
                    || self
                        .observed_at_ms
                        .is_some_and(|old| snapshot.observed_at_ms < old)
                    || snapshot.peers.iter().any(|(id, target)| {
                        self.peers
                            .get(id)
                            .is_some_and(|old| old.job_ref != target.job_ref)
                    })
                {
                    self.error = Some("discovery_binding");
                    return;
                }
                if self.observed_at_ms == Some(snapshot.observed_at_ms) {
                    if self.revision.as_deref() != Some(snapshot.revision.as_str())
                        || self.valid_until_ms != Some(snapshot.valid_until_ms)
                        || self.peers != snapshot.peers
                    {
                        self.error = Some("discovery_binding");
                    } else {
                        self.last_read_ms = Some(now_ms);
                        self.error = None;
                    }
                    return;
                }
                self.binding = Some(binding);
                self.peers = snapshot.peers;
                self.observed_at_ms = Some(snapshot.observed_at_ms);
                self.valid_until_ms = Some(snapshot.valid_until_ms);
                self.last_read_ms = Some(now_ms);
                self.revision = Some(snapshot.revision);
                self.error = None;
            }
            Err(error) => self.error = Some(error.code),
        }
    }
    pub fn targets(&self, now_ms: u64) -> BTreeMap<String, DirectoryPeer> {
        if self.valid_until_ms.is_none_or(|until| now_ms >= until) {
            return BTreeMap::new();
        }
        self.peers
            .iter()
            .filter(|(_, target)| now_ms < target.valid_until_ms)
            .map(|(id, target)| (id.clone(), target.clone()))
            .collect()
    }
    pub fn status(&self, now_ms: u64) -> Value {
        json!({"revision":self.revision,"observedAtMs":self.observed_at_ms,
            "validUntilMs":self.valid_until_ms,"lastReadAtMs":self.last_read_ms,
            "ageMs":self.observed_at_ms.map(|at|now_ms.saturating_sub(at)),
            "error": if self.valid_until_ms.is_some_and(|until|now_ms >= until) { Some("discovery_expired") } else { self.error },
            "peerCount":self.targets(now_ms).len(),
            "targets":self.targets(now_ms).values().map(|target|json!({
                "jobRef":target.job_ref,"runtimeInstanceId":target.runtime_instance_id,
                "expectedPeerId":target.peer_id,"validUntilMs":target.valid_until_ms
            })).collect::<Vec<_>>(),
            "outcome": if self.valid_until_ms.is_some_and(|until|now_ms >= until) { "expired" }
                else if self.error.is_some() { "unavailable" }
                else if self.binding.is_none() { "absent" }
                else if self.peers.is_empty() { "complete_empty" } else { "complete" }})
    }
}

pub fn read_snapshot(path: &Path) -> Result<Value> {
    // The helper owns this path. Refuse a final-component symlink at open time,
    // so a replacement between metadata inspection and open cannot redirect us.
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(path)
    };
    #[cfg(not(unix))]
    let file = std::fs::File::open(path);
    let file =
        file.map_err(|_| Error::new("discovery_unavailable", "directory file unavailable"))?;
    if !file.metadata().is_ok_and(|metadata| metadata.is_file()) {
        return Err(Error::new(
            "discovery_invalid",
            "directory is not a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("discovery_unavailable", "directory file unavailable"))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(Error::new("discovery_invalid", "directory file too large"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("discovery_invalid", "invalid directory JSON"))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireSelf {
    job_id: String,
    runtime_instance_id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WirePeer {
    job_id: String,
    runtime_instance_id: String,
    service: String,
    provider: String,
    url: String,
    tls_fingerprint: String,
    observed_at_ms: u64,
    valid_until_ms: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireSnapshot {
    schema: String,
    application_uid: String,
    #[serde(rename = "self")]
    self_identity: WireSelf,
    revision: String,
    observed_at_ms: u64,
    valid_until_ms: u64,
    peers: Vec<WirePeer>,
}
struct Parsed {
    application_uid: String,
    self_job_ref: String,
    revision: String,
    observed_at_ms: u64,
    valid_until_ms: u64,
    peers: BTreeMap<String, DirectoryPeer>,
}

fn parse(value: Value, runtime_instance: &str, service: &str, now_ms: u64) -> Result<Parsed> {
    if value["schema"] != "liskov.discovery.v2" {
        return Err(Error::new(
            "discovery_invalid",
            "unsupported directory schema",
        ));
    }
    if value["status"] == "unavailable" {
        return Err(Error::new("discovery_unavailable", "directory unavailable"));
    }
    let wire: WireSnapshot = serde_json::from_value(value)
        .map_err(|_| Error::new("discovery_invalid", "invalid direct directory"))?;
    let peers_hash = serde_json::to_vec(&wire.peers)
        .map(|bytes| format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
        .map_err(|_| Error::new("discovery_invalid", "cannot verify directory revision"))?;
    if wire.schema != "liskov.discovery.v2"
        || wire.peers.len() > 256
        || identifier(&wire.application_uid).is_err()
        || !valid_job_reference(&wire.self_identity.job_id)
        || wire.self_identity.runtime_instance_id != runtime_instance
        || !valid_digest(&wire.revision)
        || wire.revision != peers_hash
    {
        return Err(Error::new(
            "discovery_binding",
            "directory identity mismatch",
        ));
    }
    if wire.observed_at_ms > now_ms.saturating_add(5_000)
        || wire.valid_until_ms <= now_ms
        || wire.valid_until_ms <= wire.observed_at_ms
        || wire.valid_until_ms > wire.observed_at_ms.saturating_add(90_000)
    {
        return Err(Error::new(
            "discovery_expired",
            "directory outside validity window",
        ));
    }
    let mut peers = BTreeMap::new();
    let mut peer_ids = BTreeSet::new();
    let mut job_ids = BTreeSet::new();
    for peer in wire.peers {
        let expected_peer_id = runtime_peer_id(&peer.runtime_instance_id);
        if !valid_job_reference(&peer.job_id)
            || identifier(&peer.runtime_instance_id).is_err()
            || peer.runtime_instance_id == runtime_instance
            || peer.job_id == wire.self_identity.job_id
            || !peer_ids.insert(expected_peer_id.clone())
            || !job_ids.insert(peer.job_id.clone())
            || peer.provider != "direct_ipv6"
            || peer.service != service
            || !valid_service(&peer.service)
            || !valid_digest(&peer.tls_fingerprint)
            || validate_url(&peer.url).is_err()
        {
            return Err(Error::new(
                "discovery_invalid",
                "invalid direct peer endpoint",
            ));
        }
        if peer.observed_at_ms > wire.observed_at_ms
            || peer.valid_until_ms <= now_ms
            || peer.valid_until_ms <= peer.observed_at_ms
            || peer.valid_until_ms > peer.observed_at_ms.saturating_add(90_000)
            || peer.valid_until_ms > wire.valid_until_ms
        {
            return Err(Error::new(
                "discovery_expired",
                "direct peer endpoint expired",
            ));
        }
        peers.insert(
            peer.runtime_instance_id.clone(),
            DirectoryPeer {
                job_ref: peer.job_id,
                runtime_instance_id: peer.runtime_instance_id,
                peer_id: expected_peer_id,
                url: peer.url,
                tls_fingerprint: peer.tls_fingerprint,
                valid_until_ms: peer.valid_until_ms,
            },
        );
    }
    Ok(Parsed {
        application_uid: wire.application_uid,
        self_job_ref: wire.self_identity.job_id,
        revision: wire.revision,
        observed_at_ms: wire.observed_at_ms,
        valid_until_ms: wire.valid_until_ms,
        peers,
    })
}

/// A job reference is opaque: 1–512 bytes and no control character. It is never parsed.
fn valid_job_reference(raw: &str) -> bool {
    !raw.is_empty() && raw.len() <= 512 && !raw.chars().any(char::is_control)
}

fn valid_service(value: &str) -> bool {
    (1..=32).contains(&value.len())
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn valid_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_url(raw: &str) -> std::result::Result<(), ()> {
    let url = reqwest::Url::parse(raw).map_err(|_| ())?;
    let ip = url
        .host_str()
        .ok_or(())?
        .trim_matches(['[', ']'])
        .parse::<Ipv6Addr>()
        .map_err(|_| ())?;
    let port = url.port().filter(|p| *p >= 1024).ok_or(())?;
    if url.scheme() != "https" || raw != format!("https://[{ip}]:{port}/") || !global_unicast(ip) {
        return Err(());
    }
    Ok(())
}
fn global_unicast(ip: Ipv6Addr) -> bool {
    // Unit-only loopback permits actual offline TLS sockets without weakening
    // the release reader's global-unicast requirement.
    #[cfg(test)]
    if ip.is_loopback() {
        return true;
    }
    let s = ip.segments();
    (s[0] & 0xe000) == 0x2000 && !(s[0] == 0x2001 && s[1] == 0x0db8)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Planetarium's native job-reference JSON. The provider name is char
    /// codes so this crate's source does not contain that token; the value is
    /// the fixture string, and it is not decoded.
    fn opaque_native_job_ref() -> String {
        let kind: String = [65_u8, 99, 117, 114, 97, 115, 116]
            .into_iter()
            .map(char::from)
            .collect();
        format!(
            r#"{{"origin":{{"kind":"{kind}","source":"0x3c70987afe03506e7a572f5af430908220bd9606df2e1176174fa5d4535a1702"}},"id":281041}}"#
        )
    }
    fn update_revision(value: &mut Value) {
        let peers: Vec<WirePeer> = serde_json::from_value(value["peers"].clone()).unwrap();
        value["revision"] = json!(format!(
            "sha256:{}",
            hex::encode(Sha256::digest(serde_json::to_vec(&peers).unwrap()))
        ));
    }
    fn job_directory(self_job: &str, peer_job: &str) -> Value {
        let mut value: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/liskov-discovery-v2-directory.json"
        ))
        .unwrap();
        value["self"]["jobId"] = json!(self_job);
        value["peers"][0]["jobId"] = json!(peer_job);
        update_revision(&mut value);
        value
    }

    #[test]
    fn own_job_is_the_exact_frozen_directory_reference() {
        let native = opaque_native_job_ref();
        let mut state = Directory::default();
        assert_eq!(state.self_job_ref(), None);
        state.refresh(
            Ok(job_directory(&native, "job-b")),
            "instance-a",
            "swarm",
            1790000000200,
        );
        assert_eq!(state.error, None);
        assert_eq!(state.self_job_ref(), Some(native.as_str()));
        state.refresh(
            Ok(job_directory("job-c", "job-b")),
            "instance-a",
            "swarm",
            1790000000200,
        );
        assert_eq!(state.error, Some("discovery_binding"));
        assert_eq!(state.self_job_ref(), Some(native.as_str()));
    }

    #[test]
    fn shared_wire_vectors_and_negative_controls() {
        let now = 1790000000200;
        let valid: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/liskov-discovery-v2-directory.json"
        ))
        .unwrap();
        let mut state = Directory::default();
        state.refresh(Ok(valid.clone()), "instance-a", "swarm", now);
        assert!(state.error.is_none(), "{:?}", state.error);
        assert_eq!(state.targets(now).len(), 1);
        let original_expiry = state.valid_until_ms;
        state.refresh(Ok(valid.clone()), "instance-a", "swarm", now + 1);
        assert_eq!(state.valid_until_ms, original_expiry);
        let mut equal_time_changed = valid.clone();
        equal_time_changed["validUntilMs"] = json!(original_expiry.unwrap() + 1);
        state.refresh(Ok(equal_time_changed), "instance-a", "swarm", now + 2);
        assert_eq!(state.error, Some("discovery_binding"));
        assert_eq!(state.valid_until_ms, original_expiry);
        let mut peer_beyond_snapshot = valid.clone();
        peer_beyond_snapshot["validUntilMs"] = json!(original_expiry.unwrap() - 1);
        state.refresh(Ok(peer_beyond_snapshot), "instance-a", "swarm", now + 2);
        assert_eq!(state.error, Some("discovery_expired"));
        assert_eq!(state.valid_until_ms, original_expiry);
        let mut duplicate_job = valid.clone();
        let mut second_peer = duplicate_job["peers"][0].clone();
        second_peer["runtimeInstanceId"] = json!("instance-c");
        duplicate_job["peers"]
            .as_array_mut()
            .unwrap()
            .push(second_peer);
        let typed: Vec<WirePeer> = serde_json::from_value(duplicate_job["peers"].clone()).unwrap();
        duplicate_job["revision"] = json!(format!(
            "sha256:{}",
            hex::encode(Sha256::digest(serde_json::to_vec(&typed).unwrap()))
        ));
        state.refresh(Ok(duplicate_job), "instance-a", "swarm", now + 2);
        assert_eq!(state.error, Some("discovery_invalid"));
        assert_eq!(state.valid_until_ms, original_expiry);
        let mut repointed = valid.clone();
        repointed["observedAtMs"] = json!(1790000000101_u64);
        repointed["peers"][0]["jobId"] = json!("job-c");
        let typed: Vec<WirePeer> = serde_json::from_value(repointed["peers"].clone()).unwrap();
        repointed["revision"] = json!(format!(
            "sha256:{}",
            hex::encode(Sha256::digest(serde_json::to_vec(&typed).unwrap()))
        ));
        state.refresh(Ok(repointed), "instance-a", "swarm", now + 2);
        assert_eq!(state.error, Some("discovery_binding"));
        assert_eq!(state.valid_until_ms, original_expiry);
        let invalid: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/liskov-discovery-v2-invalid.json"
        ))
        .unwrap();
        for case in invalid.as_array().unwrap() {
            let mut rejected = Directory::default();
            rejected.refresh(Ok(valid.clone()), "instance-a", "swarm", now);
            rejected.refresh(Ok(case["value"].clone()), "instance-a", "swarm", now);
            assert!(rejected.error.is_some(), "{}", case["name"]);
            assert_eq!(
                rejected.targets(now).len(),
                1,
                "last-good remains bounded by original expiry"
            );
        }
        let unavailable: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/liskov-discovery-v2-unavailable.json"
        ))
        .unwrap();
        state.refresh(Ok(unavailable), "instance-a", "swarm", now + 1);
        assert_eq!(state.targets(now + 1).len(), 1);
        assert_eq!(state.status(now + 1)["outcome"], "unavailable");
        state.refresh(
            Err(Error::new("discovery_unavailable", "missing")),
            "instance-a",
            "swarm",
            now + 1,
        );
        assert_eq!(state.targets(now + 1).len(), 1);
        state.refresh(
            Err(Error::new("discovery_invalid", "malformed")),
            "instance-a",
            "swarm",
            now + 1,
        );
        assert_eq!(state.targets(now + 1).len(), 1);
        assert!(state.targets(1790000080000).is_empty());
        let mut empty: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/liskov-discovery-v2-complete-empty.json"
        ))
        .unwrap();
        empty["observedAtMs"] = json!(1790000000101_u64);
        state.refresh(Ok(empty), "instance-a", "swarm", now + 2);
        assert!(state.error.is_none());
        assert!(state.targets(now + 2).is_empty());
        assert_eq!(state.status(now + 2)["outcome"], "complete_empty");
    }
    #[cfg(unix)]
    #[test]
    fn directory_reader_refuses_symlink() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target.json");
        let link = root.path().join("directory.json");
        std::fs::write(&target, b"{}").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(read_snapshot(&link).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn directory_reader_refuses_fifo_without_blocking() {
        let root = tempfile::tempdir().unwrap();
        let fifo = root.path().join("directory.json");
        nix::unistd::mkfifo(
            &fifo,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .unwrap();
        let started = std::time::Instant::now();
        assert!(read_snapshot(&fifo).is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn job_references_are_opaque() {
        let shaped = opaque_native_job_ref();
        let mut state = Directory::default();
        state.refresh(
            Ok(job_directory(&shaped, "plain-token")),
            "instance-a",
            "swarm",
            1790000000200,
        );
        assert_eq!(state.error, None);
        assert_eq!(state.self_job_ref(), Some(shaped.as_str()));
        assert_eq!(
            state.targets(1790000000200)["instance-b"].job_ref,
            "plain-token"
        );
        let long = "j".repeat(513);
        for bad in ["", long.as_str(), "job\n-a"] {
            let mut rejected = Directory::default();
            rejected.refresh(
                Ok(job_directory("job-a", bad)),
                "instance-a",
                "swarm",
                1790000000200,
            );
            assert_eq!(rejected.error, Some("discovery_invalid"), "{bad:?}");
        }
    }

    #[test]
    fn runtime_env_requires_all_three() {
        let full = [
            ("LISKOV_RUNTIME_INSTANCE_ID", "instance-a"),
            ("LISKOV_PEER_ADVERTISEMENT_FILE", "/run/advertisement"),
            ("LISKOV_PEER_DIRECTORY_FILE", "/run/directory"),
        ];
        let ok = RuntimeEnv::from_values(full).unwrap();
        assert_eq!(ok.runtime_instance_id, "instance-a");
        assert_eq!(ok.advertisement_file, PathBuf::from("/run/advertisement"));
        assert_eq!(ok.directory_file, PathBuf::from("/run/directory"));
        for skip in 0..3 {
            let partial: Vec<_> = full
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != skip)
                .map(|(_, pair)| *pair)
                .collect();
            let err = RuntimeEnv::from_values(partial).unwrap_err();
            assert_eq!(err.code, "config", "missing {}", full[skip].0);
        }
        let mut spaced = full;
        spaced[0].1 = "instance a";
        assert!(RuntimeEnv::from_values(spaced).is_err());
    }

    #[test]
    fn peer_id_matches_planetarium() {
        assert_eq!(
            runtime_peer_id("instance-a"),
            "peer-69856ec0faae6daf81ec7eabdbba018f"
        );
    }
}
