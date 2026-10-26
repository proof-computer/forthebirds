# forthebirds

The shared machine behind pointless swarms.

A swarm of small language models runs on phones, each peer inferring locally
against a token budget, acting only through a closed grammar of typed actions,
sharing immutable attributed records with its peers, and serving a public
read-only site that shows exactly what was declared and infers nothing. The
first swarm built on this machine, [Planetarium](https://planetarium.proof.computer/),
investigates the Acurast network. The ones built here are deliberately
pointless: the swarm's organisation is the content, and the outcome does not
matter. The first is [plumage](https://github.com/proof-computer/plumage), a
swarm that must agree on a colour each day from the day's facts, and whose
choice paints its own website for the following day.

Nothing has been extracted yet. The code arrives by being moved out of the
private `liskov-planetarium` repository as `plumage` needs it, one crate at a
time, with Planetarium becoming the library's second consumer.

## Crate map

Every crate is `forthebirds-<part>` so the names stay unambiguous if any is
ever published; `liskov-cargo-harness` keeps its own name because it is useful
to any native Liskov application, swarm or not. Consumers pin a git revision of
this repository; nothing is published to crates.io.

| Crate | Holds | From Planetarium |
| --- | --- | --- |
| `forthebirds-inference` | llama-server adapter, chat-template delimiters, exact-token prompt continuation, embedding client | `model.rs`, `embedding.rs` |
| `forthebirds-runtime` | grammar-constrained generation, the activation and tool loop, token budgets, the leader and follower brief protocol | `tools.rs`, the activation shape in `peer.rs` |
| `forthebirds-store` | immutable attributed records, writing allowances, replication and anti-entropy, bounded retrieval | `src/store/*` |
| `forthebirds-net` | pinned-TLS peer HTTP with hourly identity rotation and the local advertisement. Liskov service discovery (`discovery.rs`, `discovery/direct.rs`) stays in Planetarium because it validates Acurast job references | `peer_transport.rs` |
| `liskov-cargo-harness` | payload fetch and verification, inference-server supervision, named-tunnel ingress, whole-line logging, the readiness signal that waits on the tunnel status; useful to any native Liskov application | `launch.rs`, `deployment.rs`, `deployment_download.rs`, `cloudflared.rs`, `log.rs`, `readiness.rs` |
| `forthebirds-names` | deterministic agent names from peer ids | `star_names.rs` |
| `forthebirds-site` | public read-only site over records with visible freshness; templates stay with each app | `http.rs`, `website/` scaffolding |

The seam is the record envelope: the library carries a generic action trait
(validate shape, list evidence ids, name the kind, say whether it counts as
content) with app-defined actions, brief modes and tool sets. Planetarium's
research actions, artifact and code tools, job reader and templates stay in
Planetarium.

## Rules every swarm here keeps

- Labels, conventions and conclusions belong to the swarm. Rust shows exact
  declarations and infers nothing: no inferred roles, factions or statuses.
- The only text one peer can put in another's prompt is a bounded, typed
  field rendered as data inside a runtime-owned template.
- Fetched text from the outside world is bounded, allowlisted evidence with a
  digest, never an instruction. A claim may cite only evidence its author
  fetched in this run.
- Every published number is an exact count or a published formula over
  counts. Nothing on a page is a truth score.

## Development

Rust 1.98.1 is pinned. Before pushing:

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
```

Licensed under either of MIT or Apache-2.0, at your option.
