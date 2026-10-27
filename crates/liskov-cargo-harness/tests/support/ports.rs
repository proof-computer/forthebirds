//! Ports for children that must bind their own listeners. Kernel-selected
//! ports stay in the ephemeral range; these explicit ports stay outside it.
use std::{
    net::TcpListener,
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
};

const SYSCTL: &str = "/proc/sys/net/ipv4/ip_local_port_range";
static PORTS: OnceLock<Allocator> = OnceLock::new();

pub fn port() -> u16 {
    PORTS
        .get_or_init(|| {
            let text = std::fs::read_to_string(SYSCTL)
                .unwrap_or_else(|error| panic!("cannot read {SYSCTL}: {error}"));
            let range: Vec<u16> = text
                .split_whitespace()
                .map(|part| {
                    part.parse()
                        .unwrap_or_else(|error| panic!("invalid {SYSCTL}: {error}"))
                })
                .collect();
            assert_eq!(range.len(), 2, "invalid {SYSCTL}: {text:?}");
            // Consecutive test processes begin at different parts of the range.
            Allocator::new((range[0], range[1]), std::process::id().wrapping_mul(127))
        })
        .next(|port| TcpListener::bind(("127.0.0.1", port)).is_ok())
}

/// The range, starting offset and bind predicate are injected so allocation
/// can be exercised independently of the host sysctl and network state.
pub struct Allocator {
    below: u32,
    above_start: u32,
    count: u32,
    start: u32,
    cursor: AtomicU32,
}

impl Allocator {
    pub fn new((low, high): (u16, u16), start: u32) -> Self {
        assert!(low <= high, "invalid {SYSCTL}: {low} {high}");
        let below = u32::from(low).saturating_sub(1024);
        let above_start = (u32::from(high) + 1).max(1024);
        let count = below + 65536 - above_start;
        assert!(count > 0, "{SYSCTL} leaves no test ports outside its range");
        Self {
            below,
            above_start,
            count,
            start: start % count,
            cursor: AtomicU32::new(0),
        }
    }

    pub fn next(&self, mut can_bind: impl FnMut(u16) -> bool) -> u16 {
        loop {
            // Saturate at exhaustion, including when many threads arrive then.
            let cursor = self
                .cursor
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cursor| {
                    (cursor < self.count).then_some(cursor + 1)
                })
                .unwrap_or_else(|_| panic!("test ports outside {SYSCTL} exhausted"));
            let index = (self.start + cursor) % self.count;
            let port = if index < self.below {
                1024 + index
            } else {
                self.above_start + index - self.below
            } as u16;
            if can_bind(port) {
                return port;
            }
        }
    }
}
