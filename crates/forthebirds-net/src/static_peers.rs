//! Static peer list for a local lab.
//!
//! Each peer starts a `PeerTransport` on `[::1]:0` with its own advertisement
//! file. Its `StaticPeers` lists the others. On each sync tick it calls
//! `read`, builds one `pinned_client` per new `cursor_key`, and retries an
//! entry that errs on the next tick. An advertisement is briefly absent while
//! the transport rotates its identity (`rotate_identity`).

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    net::Ipv6Addr,
    path::{Path, PathBuf},
};

const MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticPeer {
    pub name: String,
    pub address: Ipv6Addr,
    pub advertisement_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerTarget {
    pub name: String,
    pub url: String,
    pub tls_fingerprint: String,
}
impl PeerTarget {
    pub fn cursor_key(&self) -> String {
        format!("{}|{}|{}", self.name, self.tls_fingerprint, self.url)
    }
}

#[derive(Debug)]
pub struct StaticPeers {
    peers: Vec<StaticPeer>,
}
impl StaticPeers {
    pub fn new(peers: Vec<StaticPeer>) -> Result<Self> {
        if peers.len() > 256 {
            return Err(Error::new(
                "config",
                "static peer list has more than 256 entries",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for peer in &peers {
            if peer.name.is_empty()
                || peer.name.len() > 128
                || !peer
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
            {
                return Err(Error::new(
                    "config",
                    "peer name must be 1-128 ASCII letters, digits, -, _, . or :",
                ));
            }
            if !names.insert(peer.name.as_str()) {
                return Err(Error::new("config", "duplicate peer name"));
            }
            if peer.advertisement_file.as_os_str().is_empty() {
                return Err(Error::new("config", "advertisement file is empty"));
            }
        }
        Ok(Self { peers })
    }

    pub fn read(&self) -> BTreeMap<String, Result<PeerTarget>> {
        self.peers
            .iter()
            .map(|peer| {
                (
                    peer.name.clone(),
                    read_advertisement(&peer.advertisement_file, peer.address, &peer.name),
                )
            })
            .collect()
    }
}

fn read_advertisement(path: &Path, address: Ipv6Addr, name: &str) -> Result<PeerTarget> {
    // O_NOFOLLOW returns ELOOP when the final component is a symlink. That is
    // an invalid advertisement, distinct from a missing or unreadable file.
    #[cfg(unix)]
    let opened = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(path)
    };
    #[cfg(not(unix))]
    let opened = std::fs::File::open(path);
    let file = match opened {
        Ok(file) => file,
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(nix::libc::ELOOP) => {
            return Err(invalid("advertisement is a symlink"));
        }
        Err(_) => return Err(unavailable("advertisement file unavailable")),
    };
    if !file.metadata().is_ok_and(|metadata| metadata.is_file()) {
        return Err(invalid("advertisement is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable("advertisement file unavailable"))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("advertisement is larger than 64 KiB"));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid advertisement JSON"))?;
    if value.get("schema").and_then(|item| item.as_str()) != Some("liskov.peer-advertisement.v2") {
        return Err(invalid("unexpected advertisement schema"));
    }
    if value.get("ready") != Some(&serde_json::Value::Bool(true)) {
        return Err(invalid("advertisement is not ready"));
    }
    let Some(port) = value.get("port").and_then(json_port) else {
        return Err(invalid("advertisement port is not an integer in 1-65535"));
    };
    let Some(fingerprint) = value
        .get("tlsFingerprint")
        .and_then(|item| item.as_str())
        .filter(|pin| valid_pin(pin))
    else {
        return Err(invalid(
            "advertisement pin is not sha256 and 64 lowercase hex digits",
        ));
    };
    Ok(PeerTarget {
        name: name.to_owned(),
        url: format!("https://[{address}]:{port}/"),
        tls_fingerprint: fingerprint.to_owned(),
    })
}

fn json_port(value: &serde_json::Value) -> Option<u16> {
    let port = value.as_u64()?;
    u16::try_from(port).ok().filter(|port| *port >= 1)
}

fn valid_pin(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("discovery_invalid", message)
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::new("discovery_unavailable", message)
}
