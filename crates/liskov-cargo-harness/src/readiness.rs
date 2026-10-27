//! One application-readiness signal for the runtime helper, after listener bind.

use std::{io, path::PathBuf, time::Duration};
use tokio::sync::Mutex;

use crate::{HarnessApp, cloudflared::Status};

pub const READY_FILE_ENV: &str = "LISKOV_APPLICATION_READY_FILE";

/// Call only after binding the peer's HTTP listener. The helper owns the path
/// and removes its previous signal before starting a child.
pub async fn report_from_env(app: &HarnessApp, cloud: &Mutex<Status>) -> io::Result<()> {
    report(
        app.name(),
        std::env::var_os(READY_FILE_ENV).map(PathBuf::from),
        cloud,
    )
    .await
}

async fn report(name: &str, path: Option<PathBuf>, cloud: &Mutex<Status>) -> io::Result<()> {
    let Some(path) = path else { return Ok(()) };
    loop {
        let ready = {
            let status = cloud.lock().await;
            // The timestamp retains the first connection even if the tunnel
            // reconnects before this future is polled again. Zero is valid.
            status.phase == "disabled"
                || status.phase == "connected"
                || status.last_connected_at_ms.is_some()
        };
        if ready {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            crate::log_line!("{name} readiness stage=written");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_path_does_not_wait_for_a_tunnel() {
        report("fixture", None, &Mutex::new(Status::new(true)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn remembers_a_connection_even_after_reconnecting_at_timestamp_zero() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("ready");
        let mut status = Status::new(true);
        status.phase = "reconnecting";
        status.last_connected_at_ms = Some(0);
        report("fixture", Some(path.clone()), &Mutex::new(status))
            .await
            .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"");
    }

    #[tokio::test]
    async fn create_is_exclusive_and_write_errors_are_returned() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("ready");
        std::fs::write(&path, b"existing").unwrap();
        let cloud = Mutex::new(Status::new(false));
        let error = report("fixture", Some(path.clone()), &cloud)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(path).unwrap(), b"existing");
        let error = report("fixture", Some(root.path().join("missing/ready")), &cloud)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
