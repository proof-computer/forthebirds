# forthebirds development

`forthebirds` is the shared library behind the pointless swarms (`plumage`
first) and, once extraction completes, behind Planetarium. The workspace root
AGENTS.md of the `liskov` orchestrator governs worktrees, documentation and
delivery; this file governs local conventions.

## Layout

- `Cargo.toml`: a workspace; every crate lives under `crates/<name>` and shares
  version, edition, toolchain and licence through `[workspace.package]`.
- `rust-toolchain.toml`: the pinned development and CI toolchain.
- `.github/workflows/ci.yml`: the local checks repeated on GitHub, on x86_64
  and native AArch64.

## Extraction rules

- Move, do not rewrite. A module comes from `liskov-planetarium` at a named
  commit, in a rename-preserving move, with its offline tests. Record the
  source commit in the pull request.
- The move must not change what Planetarium does. Planetarium's offline tests
  pin that; a behaviour change is a separate pull request with its own reason.
- No crate here knows about Acurast, IPFS, jobs or artifacts. Domain actions,
  brief modes, tool sets and templates are the consumer's.
- Nothing fetched from a model or a peer is trusted. Validate every action
  against its grammar and every reference against known records before it is
  replicated.
- `unsafe_code` is forbidden workspace-wide.

## Validation

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo doc --locked --workspace --no-deps
```

## Landing on `main`

Push the agent branch, open the pull request, arm
`gh pr merge <n> --merge --auto`, and move on; do not poll the checks.
