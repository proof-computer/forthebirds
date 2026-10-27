//! Bounded, atomic, resumable population of application-owned model files.
use crate::{Error, Result, deployment::Input};
use reqwest::{
    StatusCode,
    header::{CONTENT_RANGE, HeaderValue, RANGE},
};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout};

/// Consecutive attempts that add no bytes before the transfer is abandoned.
/// An attempt that extends the incomplete file earns another.
const FRUITLESS_ATTEMPTS: u32 = 8;

/// How long a transfer may deliver nothing, and how often it reports.
#[derive(Clone, Copy)]
struct Pace {
    stall: Duration,
    progress: Duration,
}
const PACE: Pace = Pace {
    stall: Duration::from_secs(60),
    progress: Duration::from_secs(60),
};

enum Failure {
    /// The source or the link failed; bytes on disk remain a valid prefix.
    Retry(Error),
    /// The bytes on disk cannot become the pinned file; start over, once.
    Discard(Error),
    /// Another attempt cannot help.
    Fatal(Error),
}

fn fail(message: &str) -> Error {
    Error::new("deployment", message)
}

fn stage(name: &str, line: String) {
    crate::log_line!("{name} deployment stage={line}");
}

/// Incomplete bytes live beside the model under a name bound to the pinned
/// digest, so a later attempt or launch only ever extends bytes meant for this
/// exact file. They are never accepted as a model before the whole file
/// verifies. Only this process owns this job's model directory; never share it
/// across concurrent peers.
fn partial_path(input: &Input, name: &str) -> Result<PathBuf> {
    let parent = input
        .path
        .parent()
        .ok_or_else(|| fail("model has no storage directory"))?;
    let digest = input
        .sha256
        .get(..16)
        .filter(|d| d.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| fail("invalid model download identity/size"))?;
    Ok(parent.join(format!(
        ".{name}-model-{}.part",
        digest.to_ascii_lowercase()
    )))
}

fn held(partial: &Path) -> u64 {
    std::fs::metadata(partial).map(|m| m.len()).unwrap_or(0)
}

pub(crate) fn source_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).map_err(|_| fail("invalid model download URL"))?;
    let local = url
        .host_str()
        .is_some_and(|h| h == "127.0.0.1" || h == "[::1]");
    if (url.scheme() != "https" && !(url.scheme() == "http" && local))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(fail(
            "model source must be HTTPS without credentials/query/fragment",
        ));
    }
    Ok(url)
}

pub(crate) async fn populate(input: &Input, name: &str, seconds: u64) -> Result<()> {
    populate_paced(input, name, seconds, PACE).await
}

/// `seconds` bounds one attempt and twice that bounds the whole transfer. An
/// attempt that ends early resumes from the bytes already on disk.
async fn populate_paced(input: &Input, name: &str, seconds: u64, pace: Pace) -> Result<()> {
    if input.path.exists() {
        return super::deployment::verify(input);
    }
    let source = source_url(
        input
            .url
            .as_deref()
            .ok_or_else(|| fail("missing pinned model file/source"))?,
    )?;
    let partial = partial_path(input, name)?;
    let allow_http = source.scheme() == "http";
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            let local = attempt
                .url()
                .host_str()
                .is_some_and(|h| h == "127.0.0.1" || h == "[::1]");
            let scheme_ok = attempt.url().scheme() == "https"
                || (allow_http && attempt.url().scheme() == "http" && local);
            if attempt.previous().len() >= 8 || !scheme_ok {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(|_| fail("cannot create model transfer client"))?;
    let attempt_limit = Duration::from_secs(seconds);
    let budget = attempt_limit.saturating_mul(2);
    let started = Instant::now();
    let (mut attempt, mut fruitless, mut discarded) = (0u32, 0u32, false);
    let mut last = fail("model download deadline exceeded");
    loop {
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(last);
        }
        attempt += 1;
        stage(name, format!("model-download attempt={attempt}"));
        let before = held(&partial);
        let failure = match timeout(
            remaining.min(attempt_limit),
            transfer(&client, &source, input, &partial, name, pace),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(failure)) => failure,
            Err(_) => Failure::Retry(fail("model download deadline exceeded")),
        };
        let (error, stop) = match failure {
            Failure::Retry(error) => (error, false),
            Failure::Discard(error) => {
                let _ = std::fs::remove_file(&partial);
                (error, std::mem::replace(&mut discarded, true))
            }
            Failure::Fatal(error) => (error, true),
        };
        let now = held(&partial);
        if now == 0 {
            let _ = std::fs::remove_file(&partial);
        }
        fruitless = if now > before { 0 } else { fruitless + 1 };
        if stop || fruitless == FRUITLESS_ATTEMPTS {
            return Err(error);
        }
        stage(
            name,
            format!(
                "model-retry reason=\"{}\" bytes={now} of={}",
                error.message, input.bytes
            ),
        );
        last = error;
        let pause = Duration::from_secs((1 << fruitless.saturating_sub(1)).min(30));
        sleep(pause.min(budget.saturating_sub(started.elapsed()))).await;
    }
}

/// Hash the bytes an earlier attempt left, so the stream continues one digest.
async fn prefix_digest(partial: PathBuf, bytes: u64) -> std::result::Result<Sha256, Failure> {
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(partial).ok()?.take(bytes);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; 1024 * 1024];
        let mut seen = 0u64;
        loop {
            let count = file.read(&mut buffer).ok()?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            seen += count as u64;
        }
        (seen == bytes).then_some(hasher)
    })
    .await
    .ok()
    .flatten()
    .ok_or_else(|| Failure::Fatal(fail("cannot read incomplete model file")))
}

/// Whether `Content-Range` is `bytes <offset>-<last>/<total>` running exactly
/// from `offset` to the pinned end.
fn continues(value: Option<&HeaderValue>, offset: u64, bytes: u64) -> bool {
    let parsed = || {
        let (range, total) = value?
            .to_str()
            .ok()?
            .trim()
            .strip_prefix("bytes")?
            .trim()
            .split_once('/')?;
        let (first, last) = range.split_once('-')?;
        Some((
            first.trim().parse::<u64>().ok()?,
            last.trim().parse::<u64>().ok()?,
            total.trim().to_owned(),
        ))
    };
    parsed().is_some_and(|(first, last, total)| {
        first == offset
            && last.checked_add(1) == Some(bytes)
            && (total == "*" || total.parse() == Ok(bytes))
    })
}

async fn transfer(
    client: &reqwest::Client,
    source: &reqwest::Url,
    input: &Input,
    partial: &Path,
    name: &str,
    pace: Pace,
) -> std::result::Result<(), Failure> {
    let fatal = |message| Failure::Fatal(fail(message));
    let mismatch = "model source status/size mismatch";
    if let Some(parent) = partial.parent() {
        std::fs::create_dir_all(parent).map_err(|_| fatal("cannot create model directory"))?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(partial)
        .map_err(|_| fatal("cannot create incomplete model file"))?;
    let mut size = file
        .metadata()
        .map_err(|_| fatal("cannot create incomplete model file"))?
        .len();
    if size > input.bytes {
        return Err(Failure::Discard(fail(
            "model transfer exceeded pinned size",
        )));
    }
    let mut hasher = Sha256::new();
    if size > 0 {
        stage(
            name,
            format!("model-resume bytes={size} of={}", input.bytes),
        );
        hasher = prefix_digest(partial.to_path_buf(), size).await?;
    }
    if size < input.bytes {
        let mut request = client.get(source.clone());
        if size > 0 {
            request = request.header(RANGE, format!("bytes={size}-"));
        }
        let mut response = timeout(pace.stall, request.send())
            .await
            .map_err(|_| Failure::Retry(fail("model transfer stalled")))?
            .map_err(|_| Failure::Retry(fail("model transfer failed")))?;
        match response.status() {
            StatusCode::PARTIAL_CONTENT
                if size > 0
                    && continues(response.headers().get(CONTENT_RANGE), size, input.bytes) => {}
            StatusCode::OK => {
                if size > 0 {
                    // The source ignored the range and is sending the whole file.
                    file.set_len(0)
                        .map_err(|_| fatal("cannot write model bytes"))?;
                    size = 0;
                    hasher = Sha256::new();
                }
            }
            StatusCode::RANGE_NOT_SATISFIABLE => return Err(Failure::Discard(fail(mismatch))),
            status
                if status.is_server_error()
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS =>
            {
                return Err(Failure::Retry(fail(mismatch)));
            }
            _ => return Err(fatal(mismatch)),
        }
        if response
            .content_length()
            .is_some_and(|n| n != input.bytes - size)
        {
            return Err(fatal(mismatch));
        }
        let (mut reported, mut reported_size) = (Instant::now(), size);
        loop {
            let chunk = match timeout(pace.stall, response.chunk()).await {
                Err(_) => return Err(Failure::Retry(fail("model transfer stalled"))),
                Ok(Err(_)) => return Err(Failure::Retry(fail("model transfer interrupted"))),
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => chunk,
            };
            size = size
                .checked_add(chunk.len() as u64)
                .filter(|size| *size <= input.bytes)
                .ok_or_else(|| Failure::Discard(fail("model transfer exceeded pinned size")))?;
            file.write_all(&chunk)
                .map_err(|_| fatal("cannot write model bytes"))?;
            hasher.update(&chunk);
            if reported.elapsed() >= pace.progress {
                let millis = reported.elapsed().as_millis().max(1);
                stage(
                    name,
                    format!(
                        "model-progress bytes={size} of={} kib_per_second={}",
                        input.bytes,
                        u128::from(size - reported_size) * 1000 / 1024 / millis
                    ),
                );
                (reported, reported_size) = (Instant::now(), size);
            }
        }
        if size < input.bytes {
            return Err(Failure::Retry(fail("model transfer interrupted")));
        }
    }
    if hex::encode(hasher.finalize()) != input.sha256.to_ascii_lowercase() {
        return Err(Failure::Discard(fail(
            "downloaded model digest/size mismatch",
        )));
    }
    file.sync_all()
        .map_err(|_| fatal("cannot sync downloaded model"))?;
    drop(file);
    // Publication happens only after verified complete bytes.
    std::fs::rename(partial, &input.path).map_err(|_| fatal("cannot publish verified model"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, Bytes},
        extract::State,
        http::{HeaderMap, Response, header},
        routing::get,
    };
    use futures_util::{StreamExt, stream};
    use std::sync::{Arc, Mutex};

    /// A source that sends `cut` bytes of its first response and then either
    /// breaks the connection or goes silent. Later responses are whole.
    #[derive(Clone)]
    struct Flaky {
        body: Arc<Vec<u8>>,
        cut: Option<usize>,
        silent: bool,
        ranges: bool,
        seen: Arc<Mutex<Vec<Option<String>>>>,
    }

    async fn flaky(State(source): State<Flaky>, headers: HeaderMap) -> Response<Body> {
        let range = headers
            .get(header::RANGE)
            .map(|v| v.to_str().unwrap().to_owned());
        let first = {
            let mut seen = source.seen.lock().unwrap();
            seen.push(range.clone());
            seen.len() == 1
        };
        let offset = range.filter(|_| source.ranges).map(|r| {
            let first = r.strip_prefix("bytes=").unwrap().strip_suffix('-').unwrap();
            first.parse::<usize>().unwrap()
        });
        let rest = source.body[offset.unwrap_or(0)..].to_vec();
        let mut response = Response::builder().header(header::CONTENT_LENGTH, rest.len());
        if let Some(offset) = offset {
            let total = source.body.len();
            response = response.status(206).header(
                header::CONTENT_RANGE,
                format!("bytes {offset}-{}/{total}", total - 1),
            );
        }
        let body = match source.cut.filter(|_| first) {
            Some(cut) => {
                let silent = source.silent;
                let tail = stream::once(async move {
                    if silent {
                        std::future::pending::<()>().await;
                    }
                    sleep(Duration::from_millis(200)).await;
                    Err::<Bytes, _>(std::io::Error::other("cut"))
                });
                let head = Bytes::from(rest[..cut].to_vec());
                Body::from_stream(stream::iter([Ok(head)]).chain(tail))
            }
            None => Body::from(rest),
        };
        response.body(body).unwrap()
    }

    async fn serve(
        cut: Option<usize>,
        silent: bool,
        ranges: bool,
    ) -> (Flaky, Input, tempfile::TempDir) {
        let source = Flaky {
            body: Arc::new((0..200_000u32).map(|i| (i * 31 % 251) as u8).collect()),
            cut,
            silent,
            ranges,
            seen: Arc::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://127.0.0.1:{}/model",
            listener.local_addr().unwrap().port()
        );
        let router = Router::new()
            .route("/model", get(flaky))
            .with_state(source.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let root = tempfile::tempdir().unwrap();
        let input = Input {
            path: root.path().join("models/research.gguf"),
            bytes: source.body.len() as u64,
            sha256: hex::encode(Sha256::digest(&*source.body)),
            url: Some(url),
        };
        (source, input, root)
    }

    fn resumed_from(range: &Option<String>) -> u64 {
        let range = range.as_deref().expect("the retry must ask for a range");
        let first = range.strip_prefix("bytes=").unwrap().strip_suffix('-');
        first.unwrap().parse().unwrap()
    }

    fn published(source: &Flaky, input: &Input) {
        assert_eq!(std::fs::read(&input.path).unwrap(), *source.body);
        assert_eq!(
            std::fs::read_dir(input.path.parent().unwrap())
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn broken_and_silent_transfers_resume_from_the_bytes_already_held() {
        for silent in [false, true] {
            let (source, input, _root) = serve(Some(70_000), silent, true).await;
            let pace = Pace {
                stall: Duration::from_millis(300),
                progress: Duration::ZERO,
            };
            populate_paced(&input, "fixture", 20, pace).await.unwrap();
            let seen = source.seen.lock().unwrap().clone();
            assert_eq!(seen.len(), 2, "silent={silent}");
            assert_eq!(seen[0], None);
            assert!((1..=70_000).contains(&resumed_from(&seen[1])));
            published(&source, &input);
        }
    }

    #[tokio::test]
    async fn source_without_ranges_restarts_from_the_first_byte() {
        let (source, input, _root) = serve(Some(70_000), false, false).await;
        populate_paced(&input, "fixture", 20, PACE).await.unwrap();
        assert_eq!(source.seen.lock().unwrap().len(), 2);
        published(&source, &input);
    }

    #[tokio::test]
    async fn held_bytes_that_do_not_verify_are_discarded_once() {
        let (source, input, _root) = serve(None, false, true).await;
        let partial = partial_path(&input, "fixture").unwrap();
        std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
        std::fs::write(&partial, vec![7; 1000]).unwrap();
        populate_paced(&input, "fixture", 20, PACE).await.unwrap();
        assert_eq!(
            *source.seen.lock().unwrap(),
            [Some("bytes=1000-".to_owned()), None]
        );
        published(&source, &input);
    }

    #[tokio::test]
    async fn complete_held_bytes_publish_without_a_request() {
        let (source, input, _root) = serve(None, false, true).await;
        let partial = partial_path(&input, "fixture").unwrap();
        std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
        std::fs::write(&partial, &*source.body).unwrap();
        populate_paced(&input, "fixture", 20, PACE).await.unwrap();
        assert!(source.seen.lock().unwrap().is_empty());
        published(&source, &input);
    }

    #[test]
    fn content_range_must_continue_the_held_bytes_to_the_pinned_end() {
        let ok = |value: &'static str| continues(Some(&HeaderValue::from_static(value)), 5, 12);
        assert!(ok("bytes 5-11/12") && ok("bytes 5-11/*"));
        for value in ["bytes 4-11/12", "bytes 5-10/12", "bytes 5-11/13", "5-11/12"] {
            assert!(!ok(value), "{value}");
        }
        assert!(!continues(None, 5, 12));
    }

    #[tokio::test]
    async fn transfer_verifies_before_publish_and_cache_needs_no_origin() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://127.0.0.1:{}/model",
            listener.local_addr().unwrap().port()
        );
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/model", get(|| async { "GGUF-fixture" })),
            )
            .await
            .unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let mut input = Input {
            path: root.path().join("models/research.gguf"),
            bytes: 13,
            sha256: hex::encode(Sha256::digest(b"GGUF-fixture")),
            url: Some(url),
        };
        // Fixture is deliberately sized wrongly first: no final file accepted.
        assert!(populate(&input, "fixture", 2).await.is_err());
        assert!(!input.path.exists());
        input.bytes = 12;
        populate(&input, "fixture", 2).await.unwrap();
        task.abort();
        populate(&input, "fixture", 2).await.unwrap();
        std::fs::write(&input.path, b"changed").unwrap();
        assert!(populate(&input, "fixture", 2).await.is_err());
        assert!(
            std::fs::read_dir(input.path.parent().unwrap())
                .unwrap()
                .all(|p| !p.unwrap().file_name().to_string_lossy().ends_with(".part"))
        );
    }

    #[tokio::test]
    async fn bad_digest_and_transfer_deadline_never_publish_model() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let router = Router::new()
            .route("/bad", get(|| async { "GGUF-fixture" }))
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    "GGUF-fixture"
                }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let mut input = Input {
            path: root.path().join("models/research.gguf"),
            bytes: 12,
            sha256: "0".repeat(64),
            url: Some(format!("{base}/bad")),
        };
        assert_eq!(
            populate(&input, "fixture", 1).await.unwrap_err().message,
            "downloaded model digest/size mismatch"
        );
        assert!(!input.path.exists());
        assert_eq!(
            std::fs::read_dir(input.path.parent().unwrap())
                .unwrap()
                .count(),
            0
        );
        input.sha256 = hex::encode(Sha256::digest(b"GGUF-fixture"));
        input.url = Some(format!("{base}/slow"));
        assert_eq!(
            populate(&input, "fixture", 1).await.unwrap_err().message,
            "model download deadline exceeded"
        );
        assert!(!input.path.exists());
        task.abort();
    }
    #[test]
    fn download_sources_do_not_allow_unbound_cleartext_or_credentials() {
        source_url("https://example.org/revision/model.gguf").unwrap();
        for value in [
            "http://example.org/model",
            "https://user:secret@example.org/model",
            "https://example.org/model?token=value",
        ] {
            assert!(source_url(value).is_err());
        }
    }
}
