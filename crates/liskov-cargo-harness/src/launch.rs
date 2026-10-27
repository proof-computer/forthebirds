//! Cargo entrypoint for one native Liskov application. The runtime image bakes
//! one static binary and nothing else, so the peer, the pinned inference
//! server release and the deployment descriptor arrive as one payload archive
//! named by the application environment. This module fetches that archive
//! through the bounded downloader, verifies its exact bytes, unpacks it beside
//! the job's state and hands over to [`crate::deployment::run`]. It builds
//! nothing, provisions nothing and never trusts an archive path it did not sanitise.
use crate::{
    Error, HarnessApp, Result,
    deployment::{self, Input},
    deployment_download,
};
use std::{
    future::Future,
    path::{Component, Path, PathBuf},
};

/// Payload home when the application does not set `HOME`.
pub const DEFAULT_HOME: &str = "/app";
/// Per-attempt transfer deadline for one payload part, which carries no model
/// bytes and is at most a few megabytes.
const DOWNLOAD_SECONDS: u64 = 600;
const MAX_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_PARTS: usize = 64;
const MAX_ENTRIES: usize = 256;
const MAX_UNPACKED_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub url: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    /// Ordered archive parts; concatenated they are the archive.
    pub parts: Vec<Part>,
    pub sha256: String,
    pub bytes: u64,
    pub home: PathBuf,
}

fn fail(message: &str) -> Error {
    Error::new("launch", message)
}

fn digest(value: &str) -> Result<String> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(fail("payload digest must be 64 hex digits"));
    }
    Ok(value.to_ascii_lowercase())
}

fn size(value: &str) -> Result<u64> {
    value
        .parse()
        .ok()
        .filter(|n| (1..=MAX_ARCHIVE_BYTES).contains(n))
        .ok_or_else(|| fail("payload size must be 1..=1 GiB bytes"))
}

impl Source {
    /// Read the payload identity from the application environment. Every value
    /// is a non-secret policy variable; the peer bearer stays in its own secret.
    pub fn from_env(app: &HarnessApp) -> Result<Self> {
        let read = |suffix: &str| {
            std::env::var(app.env(suffix)).map_err(|_| fail("missing payload variable"))
        };
        let home = std::env::var(app.env("HOME")).unwrap_or_else(|_| DEFAULT_HOME.to_string());
        Self::from_values(
            &read("PAYLOAD_URL")?,
            &read("PAYLOAD_SHA256")?,
            &read("PAYLOAD_BYTES")?,
            std::env::var(app.env("PAYLOAD_PART_SHA256"))
                .ok()
                .as_deref(),
            std::env::var(app.env("PAYLOAD_PART_BYTES")).ok().as_deref(),
            &home,
        )
    }

    /// `urls` holds one or more whitespace-separated HTTPS URLs. With one URL
    /// and no part lists, that URL is the whole archive. Otherwise the part
    /// digest and size lists must align with the URLs one to one.
    pub fn from_values(
        urls: &str,
        sha256: &str,
        bytes: &str,
        part_sha256: Option<&str>,
        part_bytes: Option<&str>,
        home: &str,
    ) -> Result<Self> {
        let sha256 = digest(sha256)?;
        let bytes = size(bytes)?;
        let urls: Vec<&str> = urls.split_whitespace().collect();
        if urls.is_empty() || urls.len() > MAX_PARTS {
            return Err(fail("payload needs 1..=64 part URLs"));
        }
        for url in &urls {
            deployment_download::source_url(url)?;
        }
        let parts = match (part_sha256, part_bytes) {
            (None, None) if urls.len() == 1 => vec![Part {
                url: urls[0].to_string(),
                sha256: sha256.clone(),
                bytes,
            }],
            (Some(digests), Some(sizes)) => {
                let digests: Vec<&str> = digests.split_whitespace().collect();
                let sizes: Vec<&str> = sizes.split_whitespace().collect();
                if digests.len() != urls.len() || sizes.len() != urls.len() {
                    return Err(fail("payload part lists must align with the URL list"));
                }
                let parts = urls
                    .iter()
                    .zip(digests)
                    .zip(sizes)
                    .map(|((url, digest_value), size_value)| {
                        Ok(Part {
                            url: url.to_string(),
                            sha256: digest(digest_value)?,
                            bytes: size(size_value)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if parts.iter().map(|p| p.bytes).sum::<u64>() != bytes {
                    return Err(fail("payload part sizes do not sum to the archive size"));
                }
                parts
            }
            _ => return Err(fail("payload part lists must both be present")),
        };
        let home = PathBuf::from(home);
        if !home.is_absolute() || home.components().any(|c| c == Component::ParentDir) {
            return Err(fail("payload home must be an absolute path"));
        }
        Ok(Self {
            parts,
            sha256,
            bytes,
            home,
        })
    }

    fn archive(&self) -> PathBuf {
        self.home.join("payload.tar.gz")
    }
    pub fn root(&self) -> PathBuf {
        self.home.join("payload")
    }
    fn marker(&self) -> PathBuf {
        self.home.join("payload.sha256")
    }
    fn part_path(&self, index: usize) -> PathBuf {
        self.home.join(format!("payload.part-{index:02}"))
    }

    /// Make the verified whole archive present at `archive()`: reuse it when
    /// its bytes already verify, otherwise fetch every part through the
    /// bounded downloader and assemble them under a temporary name.
    async fn obtain_archive(&self, name: &str) -> Result<()> {
        let whole = Input {
            path: self.archive(),
            sha256: self.sha256.clone(),
            bytes: self.bytes,
            url: (self.parts.len() == 1).then(|| self.parts[0].url.clone()),
        };
        if self.parts.len() == 1 || whole.path.exists() {
            // One part is the archive; an existing archive only needs verifying.
            return deployment_download::populate(&whole, name, DOWNLOAD_SECONDS).await;
        }
        let mut inputs = Vec::new();
        for (index, part) in self.parts.iter().enumerate() {
            let input = Input {
                path: self.part_path(index),
                sha256: part.sha256.clone(),
                bytes: part.bytes,
                url: Some(part.url.clone()),
            };
            crate::log_line!(
                "{name} launch stage=payload-part index={} of={}",
                index + 1,
                self.parts.len()
            );
            deployment_download::populate(&input, name, DOWNLOAD_SECONDS).await?;
            inputs.push(input);
        }
        let assembling = self.home.join(".payload.assembling");
        let result = assemble(&inputs, &assembling, &whole);
        if result.is_err() {
            let _ = std::fs::remove_file(&assembling);
            return result;
        }
        std::fs::rename(&assembling, &whole.path)
            .map_err(|_| fail("cannot publish assembled payload"))?;
        for input in &inputs {
            let _ = std::fs::remove_file(&input.path);
        }
        Ok(())
    }
}

/// Concatenate verified parts, checking the whole digest and size before the
/// archive is published under its final name.
fn assemble(parts: &[Input], destination: &Path, whole: &Input) -> Result<()> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(destination)
        .map_err(|_| fail("cannot create assembled payload"))?;
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    for part in parts {
        let mut file =
            std::fs::File::open(&part.path).map_err(|_| fail("cannot read payload part"))?;
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|_| fail("cannot read payload part"))?;
            if count == 0 {
                break;
            }
            total += count as u64;
            if total > whole.bytes {
                return Err(fail("assembled payload exceeds the archive size"));
            }
            hasher.update(&buffer[..count]);
            out.write_all(&buffer[..count])
                .map_err(|_| fail("cannot write assembled payload"))?;
        }
    }
    if total != whole.bytes || hex::encode(hasher.finalize()) != whole.sha256 {
        return Err(fail("assembled payload digest/size mismatch"));
    }
    out.sync_all()
        .map_err(|_| fail("cannot sync assembled payload"))
}

/// Fetch, verify and unpack the payload, then supervise it until shutdown.
pub async fn launch(
    app: &HarnessApp,
    source: Source,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let name = app.name().to_string();
    std::fs::create_dir_all(&source.home).map_err(|_| fail("cannot create payload home"))?;
    crate::log_line!("{name} launch stage=payload-download");
    source.obtain_archive(&name).await?;
    let unpacked = std::fs::read_to_string(source.marker())
        .map(|m| m.trim() == source.sha256)
        .unwrap_or(false);
    if unpacked && source.root().join("deployment.json").is_file() {
        crate::log_line!("{name} launch stage=payload-cached");
    } else {
        // A different payload is a different deployment: its state does not carry over.
        let root = source.root();
        if root.exists() {
            std::fs::remove_dir_all(&root).map_err(|_| fail("cannot clear stale payload"))?;
        }
        let _ = std::fs::remove_file(source.marker());
        let archive = source.archive();
        let unpack_name = name.clone();
        tokio::task::spawn_blocking(move || unpack(&archive, &root, &unpack_name))
            .await
            .map_err(|_| fail("payload unpack task failed"))??;
        std::fs::write(source.marker(), format!("{}\n", source.sha256))
            .map_err(|_| fail("cannot record unpacked payload"))?;
        crate::log_line!("{name} launch stage=payload-unpacked");
    }
    deployment::run(app, &source.root().join("deployment.json"), shutdown).await
}

/// Unpack a gzip tar archive of regular files and directories into a fresh
/// directory. Links, devices, absolute paths and parent segments are refused;
/// entry count and unpacked size are bounded before any byte is written.
pub(crate) fn unpack(archive: &Path, destination: &Path, name: &str) -> Result<()> {
    let staging = destination.with_extension("unpacking");
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(|_| fail("cannot clear unpack staging"))?;
    }
    std::fs::create_dir_all(&staging).map_err(|_| fail("cannot create unpack staging"))?;
    let result = unpack_into(archive, &staging, name);
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
        return result;
    }
    std::fs::rename(&staging, destination).map_err(|_| fail("cannot publish unpacked payload"))
}

fn unpack_into(archive: &Path, staging: &Path, name: &str) -> Result<()> {
    let file = std::fs::File::open(archive).map_err(|_| fail("cannot open payload archive"))?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(false);
    archive.set_unpack_xattrs(false);
    let mut entries = 0usize;
    let mut total = 0u64;
    for entry in archive
        .entries()
        .map_err(|_| fail("payload is not a tar archive"))?
    {
        let mut entry = entry.map_err(|_| fail("payload archive is corrupt"))?;
        entries += 1;
        if entries > MAX_ENTRIES {
            return Err(fail("payload has too many entries"));
        }
        let kind = entry.header().entry_type();
        if !(kind.is_file() || kind.is_dir()) {
            return Err(fail("payload entry type is not allowed"));
        }
        let path = entry
            .path()
            .map_err(|_| fail("payload entry path is not valid"))?;
        if path.as_os_str().is_empty()
            || !path
                .components()
                .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(fail("payload entry path escapes the payload"));
        }
        let size = entry
            .header()
            .size()
            .map_err(|_| fail("payload entry size is not valid"))?;
        total = total
            .checked_add(size)
            .filter(|n| *n <= MAX_UNPACKED_BYTES)
            .ok_or_else(|| fail("payload unpacks beyond the size bound"))?;
        if !entry.unpack_in(staging).map_err(|error| {
            crate::log_line!("{name} launch unpack error={error}");
            fail("cannot unpack payload entry")
        })? {
            return Err(fail("payload entry escapes the payload"));
        }
    }
    if entries == 0 {
        return Err(fail("payload archive is empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn archive(dir: &Path, entries: &[(&str, tar::EntryType, &[u8])]) -> PathBuf {
        let path = dir.join("payload.tar.gz");
        let file = std::fs::File::create(&path).unwrap();
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            file,
            flate2::Compression::fast(),
        ));
        for (name, kind, data) in entries {
            let mut header = tar::Header::new_ustar();
            header.set_entry_type(*kind);
            header.set_mode(if kind.is_dir() { 0o755 } else { 0o644 });
            header.set_size(data.len() as u64);
            if kind.is_symlink() {
                header.set_link_name("/etc/passwd").unwrap();
            }
            // Raw name bytes: `set_path` refuses the escapes this test must build.
            let raw = header.as_ustar_mut().unwrap();
            raw.name[..name.len()].copy_from_slice(name.as_bytes());
            header.set_cksum();
            builder.append(&header, *data).unwrap();
        }
        builder
            .into_inner()
            .unwrap()
            .finish()
            .unwrap()
            .flush()
            .unwrap();
        path
    }

    #[test]
    fn source_values_are_bounded() {
        let sha = "a".repeat(64);
        let ok = Source::from_values(
            "https://example.org/p.tar.gz",
            &sha,
            "12",
            None,
            None,
            "/app",
        )
        .unwrap();
        assert_eq!(ok.bytes, 12);
        assert_eq!(ok.parts.len(), 1);
        assert_eq!(ok.parts[0].sha256, sha);
        assert_eq!(ok.root(), PathBuf::from("/app/payload"));
        let two = Source::from_values(
            "https://example.org/p.00 https://example.org/p.01",
            &sha,
            "12",
            Some(&format!("{} {}", "b".repeat(64), "C".repeat(64))),
            Some("5 7"),
            "/app",
        )
        .unwrap();
        assert_eq!(two.parts.len(), 2);
        assert_eq!(two.parts[1].sha256, "c".repeat(64));
        assert_eq!(two.parts[1].bytes, 7);
        let b = "b".repeat(64);
        type Case<'a> = (
            &'a str,
            &'a str,
            &'a str,
            Option<String>,
            Option<&'a str>,
            &'a str,
        );
        let cases: Vec<Case> = vec![
            (
                "http://example.org/p.tar.gz",
                &sha,
                "12",
                None,
                None,
                "/app",
            ),
            (
                "https://u:p@example.org/p.tar.gz",
                &sha,
                "12",
                None,
                None,
                "/app",
            ),
            (
                "https://example.org/p.tar.gz",
                "abc",
                "12",
                None,
                None,
                "/app",
            ),
            (
                "https://example.org/p.tar.gz",
                &sha,
                "0",
                None,
                None,
                "/app",
            ),
            (
                "https://example.org/p.tar.gz",
                &sha,
                "x",
                None,
                None,
                "/app",
            ),
            (
                "https://example.org/p.tar.gz",
                &sha,
                "12",
                None,
                None,
                "app",
            ),
            (
                "https://example.org/p.tar.gz",
                &sha,
                "12",
                None,
                None,
                "/app/../x",
            ),
            ("", &sha, "12", None, None, "/app"),
            // Two URLs need aligned part lists whose sizes sum to the whole.
            (
                "https://a.org/0 https://a.org/1",
                &sha,
                "12",
                None,
                None,
                "/app",
            ),
            (
                "https://a.org/0 https://a.org/1",
                &sha,
                "12",
                Some(b.clone()),
                None,
                "/app",
            ),
            (
                "https://a.org/0 https://a.org/1",
                &sha,
                "12",
                Some(format!("{b} {b}")),
                Some("5"),
                "/app",
            ),
            (
                "https://a.org/0 https://a.org/1",
                &sha,
                "12",
                Some(format!("{b} {b}")),
                Some("5 8"),
                "/app",
            ),
            (
                "https://a.org/0 https://a.org/1",
                &sha,
                "12",
                Some(format!("{b} zz")),
                Some("5 7"),
                "/app",
            ),
        ];
        for (url, sha, bytes, psha, pbytes, home) in cases {
            assert!(
                Source::from_values(url, sha, bytes, psha.as_deref(), pbytes, home).is_err(),
                "{url} {sha} {bytes} {home}"
            );
        }
    }

    #[test]
    fn assemble_checks_the_whole_digest() {
        use sha2::{Digest, Sha256};
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"hello ").unwrap();
        std::fs::write(&b, b"world").unwrap();
        let part = |path: &Path| Input {
            path: path.to_path_buf(),
            sha256: String::new(),
            bytes: 0,
            url: None,
        };
        let whole = |sha: String, bytes: u64| Input {
            path: dir.path().join("whole"),
            sha256: sha,
            bytes,
            url: None,
        };
        let good = hex::encode(Sha256::digest(b"hello world"));
        let out = dir.path().join("out");
        assemble(&[part(&a), part(&b)], &out, &whole(good.clone(), 11)).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"hello world");
        assert!(assemble(&[part(&a), part(&b)], &out, &whole(good.clone(), 10)).is_err());
        assert!(assemble(&[part(&b), part(&a)], &out, &whole(good, 11)).is_err());
    }

    #[test]
    fn unpack_refuses_escapes_links_and_empty_archives() {
        let dir = tempfile::tempdir().unwrap();
        let cases: [(&str, tar::EntryType); 3] = [
            ("../outside", tar::EntryType::Regular),
            ("/abs", tar::EntryType::Regular),
            ("link", tar::EntryType::Symlink),
        ];
        for (name, kind) in cases {
            let path = archive(dir.path(), &[(name, kind, b"x")]);
            let error = unpack(&path, &dir.path().join("out"), "fixture").unwrap_err();
            assert_eq!(error.code, "launch", "{name}");
            assert!(!dir.path().join("out").exists());
            assert!(!dir.path().join("out.unpacking").exists());
        }
        let path = archive(dir.path(), &[]);
        assert!(unpack(&path, &dir.path().join("out"), "fixture").is_err());
    }

    #[test]
    fn unpack_publishes_regular_files_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = archive(
            dir.path(),
            &[
                ("bin/", tar::EntryType::Directory, b""),
                ("bin/planetarium", tar::EntryType::Regular, b"#!/bin/sh\n"),
                ("deployment.json", tar::EntryType::Regular, b"{}"),
            ],
        );
        let out = dir.path().join("out");
        unpack(&path, &out, "fixture").unwrap();
        assert_eq!(std::fs::read(out.join("deployment.json")).unwrap(), b"{}");
        assert!(out.join("bin/planetarium").is_file());
        assert!(!dir.path().join("out.unpacking").exists());
    }
}
