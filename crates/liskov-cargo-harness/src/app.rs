//! Application seam. The harness names one native Liskov application and never
//! reads that application's own configuration.

use crate::{Error, Result};
use std::{ffi::OsString, path::Path};

/// How the supervised peer process is started. Planetarium passes
/// `peer --config <path>`; plumage passes `peer <path>`.
pub type PeerArgs = fn(&Path) -> Vec<OsString>;

fn peer_config_args(path: &Path) -> Vec<OsString> {
    vec![
        OsString::from("peer"),
        OsString::from("--config"),
        path.as_os_str().to_os_string(),
    ]
}

/// One native Liskov application launched by this harness.
#[derive(Clone, Debug)]
pub struct HarnessApp {
    name: String,
    /// Arguments after the peer executable. The path is absolute.
    pub peer_args: PeerArgs,
}

impl HarnessApp {
    /// Accept `^[a-z][a-z0-9]{0,31}$` and refuse everything else.
    pub fn new(name: &str) -> Result<Self> {
        let mut bytes = name.bytes();
        let first_ok = bytes.next().is_some_and(|b| b.is_ascii_lowercase());
        if !first_ok
            || name.len() > 32
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(Error::new(
                "config",
                "application name must match ^[a-z][a-z0-9]{0,31}$",
            ));
        }
        Ok(Self {
            name: name.to_string(),
            peer_args: peer_config_args,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `LISKOV_<NAME>_<suffix>`, with `NAME` the application name in capitals.
    pub fn env(&self, suffix: &str) -> String {
        format!("LISKOV_{}_{suffix}", self.name.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_derive_from_the_application() {
        let app = HarnessApp::new("plumage").unwrap();
        assert_eq!(app.env("PAYLOAD_URL"), "LISKOV_PLUMAGE_PAYLOAD_URL");
        assert_eq!(
            app.env("PAYLOAD_PART_SHA256"),
            "LISKOV_PLUMAGE_PAYLOAD_PART_SHA256"
        );
        assert_eq!(app.env("HOME"), "LISKOV_PLUMAGE_HOME");
        assert_eq!(app.env("PEER_TOKEN"), "LISKOV_PLUMAGE_PEER_TOKEN");
        assert_eq!(
            app.env("CLOUDFLARED_TOKEN"),
            "LISKOV_PLUMAGE_CLOUDFLARED_TOKEN"
        );
        assert_eq!(
            app.env("PARENT_SHUTDOWN_SECONDS"),
            "LISKOV_PLUMAGE_PARENT_SHUTDOWN_SECONDS"
        );
        for name in ["", "Plumage", "9lives", "a-b", &"a".repeat(33)] {
            let error = HarnessApp::new(name).unwrap_err();
            assert_eq!(error.code, "config", "{name}");
        }
    }
}
