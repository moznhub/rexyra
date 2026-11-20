//! Exact latency statistics over every recorded sample.

use serde::Serialize;

/// One completed request. Times are nanoseconds since the run's origin
/// (`Instant` taken just before the traffic started).
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// When the full reply had been read.
    pub end_ns: u64,
    /// From just before the request was written to `end_ns`.
    pub lat_ns: u64,
    /// Foreground: 0 = GET, 1 = SET. Toucher: index of the touched key.
    pub target: u32,
}

/// Half-open interval `[start_ns, end_ns)` of completion times.
#[derive(Clone, Copy, Debug)]
pub struct Window {
    pub start_ns: u64,
    pub end_ns: u64,
}

impl Window {
    pub fn contains(&self, t: u64) -> bool {
        t >= self.start_ns && t < self.end_ns
    }

    pub fn secs(&self) -> f64 {
        self.end_ns.saturating_sub(self.start_ns) as f64 / 1e9
    }
}

pub fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

/// `t` in milliseconds relative to `zero` (negative before it).
pub fn rel_ms(t: u64, zero: u64) -> f64 {
    (t as f64 - zero as f64) / 1e6
}

#[derive(Serialize, Debug, Default)]
pub struct LatStats {
    pub count: usize,
    pub throughput_per_s: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub p9999_ms: f64,
    pub max_ms: f64,
    /// Completion time of the slowest request, ms relative to the BGSAVE
    /// acknowledgement.
    pub max_completed_at_ms: Option<f64>,
}

/// Nearest-rank percentiles over the exact latencies of the samples that
/// completed in `window` and pass `keep`. All zero when there are none.
pub fn latency(
    samples: &[Sample],
    window: Window,
    keep: impl Fn(&Sample) -> bool,
    zero_ns: u64,
) -> LatStats {
    let mut lats = Vec::new();
    let mut sum: u128 = 0;
    let mut slowest: Option<Sample> = None;
    for s in samples
        .iter()
        .filter(|s| window.contains(s.end_ns) && keep(s))
    {
        lats.push(s.lat_ns);
        sum += u128::from(s.lat_ns);
        if slowest.is_none_or(|m| s.lat_ns > m.lat_ns) {
            slowest = Some(*s);
        }
    }
    if lats.is_empty() {
        return LatStats::default();
    }
    lats.sort_unstable();
    let n = lats.len();
    let pct = |p: f64| {
        let rank = ((p * n as f64).ceil() as usize).clamp(1, n);
        ms(lats[rank - 1])
    };
    LatStats {
        count: n,
        throughput_per_s: n as f64 / window.secs(),
        mean_ms: sum as f64 / n as f64 / 1e6,
        p50_ms: pct(0.50),
        p90_ms: pct(0.90),
        p95_ms: pct(0.95),
        p99_ms: pct(0.99),
        p999_ms: pct(0.999),
        p9999_ms: pct(0.9999),
        max_ms: ms(lats[n - 1]),
        max_completed_at_ms: slowest.map(|s| rel_ms(s.end_ns, zero_ns)),
    }
}

#[derive(Serialize, Debug)]
pub struct Gap {
    /// Longest interval inside the window in which no request completed on
    /// any connection; the window's edges count as boundaries.
    pub longest_ms: f64,
    /// Where that interval began, ms relative to the BGSAVE acknowledgement.
    pub starts_at_ms: f64,
}

/// `ends` must be sorted.
pub fn longest_gap(ends: &[u64], window: Window, zero_ns: u64) -> Gap {
    let lo = ends.partition_point(|&e| e < window.start_ns);
    let hi = ends.partition_point(|&e| e < window.end_ns);
    let mut prev = window.start_ns;
    let mut best = (0u64, window.start_ns);
    for &e in &ends[lo..hi] {
        if e - prev > best.0 {
            best = (e - prev, prev);
        }
        prev = e;
    }
    if window.end_ns.saturating_sub(prev) > best.0 {
        best = (window.end_ns - prev, prev);
    }
    Gap {
        longest_ms: ms(best.0),
        starts_at_ms: rel_ms(best.1, zero_ns),
    }
}
