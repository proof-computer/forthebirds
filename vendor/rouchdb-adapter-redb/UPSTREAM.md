# RouchDB redb adapter patch

Vendored from rubylab-app/rouchdb commit
c9268c00db01c2fcdc3411f030e006ecca60efaa (workspace 0.5.1), MIT.
Only this adapter is patched; the other RouchDB crates are pinned to 0.5.1.
Cargo metadata is standalone so the crate can be used by [patch.crates-io].

Local change: OpenOptions::cache_size(bytes) configures ordinary create/open
and is retained when an upgraded file is reopened. None preserves redb's
upstream default; Some(0) explicitly selects a zero-size cache.
No on-disk format or query/replication semantics are changed.
