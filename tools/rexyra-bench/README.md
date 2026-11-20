# rexyra-bench

Load generator for measuring what a `BGSAVE` does to rexyra-server's command
latency, throughput and memory. Plain threads and blocking loopback TCP
(RESP2), no Docker, no redis-benchmark. Results and the report are in
`docs/benchmarks/bgsave/`.

## Build

```
cargo build --release -p rexyra-server -p rexyra-bench
```

Run everything from the workspace root. By default the tool starts
`target\release\rexyra-server.exe` (next to its own executable), keeps data under
`target\bench-data\` (each run's directory is deleted afterwards unless
`--keep-data`), uses port 6400 and writes one JSON file per run to
`docs\benchmarks\bgsave\raw\`.

## What one run does

1. Starts a fresh server process (`--port`, `--dir <fresh dir>`, `--save ""`)
   and loads the scenario's dataset over 8 pipelined connections; checks
   `DBSIZE`; sets `rdb-key-save-delay` (0 unless `--save-delay-us`).
2. Starts the traffic: `--clients` (50) closed-loop connections doing GET or
   SET (`--get-percent`, 50) of a uniformly random existing small key with a
   value of the same size; for scenarios with large values, one *toucher*
   connection; and one *monitor* connection that sends `INFO memory` +
   `INFO persistence` (pipelined) every `--monitor-ms` (10).
3. After `--warmup-secs` (1) and a `--baseline-secs` (5) baseline, takes
   `DBSIZE` (and `LLEN` of the lists) with no toucher command in flight,
   then sends `BGSAVE`. The moment its reply is read is time zero.
4. The save is over at the first monitor reply, to a poll sent after time
   zero, that says `rdb_bgsave_in_progress:0`. Traffic continues for
   `--after-secs` (3) more, then stops.
5. Checks `rdb_last_bgsave_status`, `LASTSAVE` and the size of `dump.rdb`,
   stops the server (`SHUTDOWN NOSAVE`, then waits on the process handle),
   starts a new server on the same directory and compares its `DBSIZE` (and
   `LLEN`s) with the values taken at the `BGSAVE`. The traffic never creates
   or deletes keys, so these must match exactly.

Latency is measured per request with `Instant` (QueryPerformanceCounter)
from just before the request is written to after the whole reply is read.
Every sample is kept in memory; percentiles are exact (nearest rank). A
sample belongs to the window its *completion* time falls in: baseline
`[warm-up end, BGSAVE reply)`, during `[BGSAVE reply, save end seen)`,
after `[save end seen, traffic stop)`. "Longest gap" is the longest interval
in a window in which no foreground request completed on any connection (the
window edges count as boundaries).

## Scenarios (`rexyra-bench list`)

| name | dataset | toucher |
|---|---|---|
| `small` | 1,000,000 × 1,000-byte strings | none |
| `str1m` | 1,000 × 1 MiB strings + 100,000 × 1,000-byte strings | SETs the large strings round-robin, back to back |
| `str10m` | 100 × 10 MiB + 100,000 × 1,000 B | same |
| `str100m` | 10 × 100 MiB + 100,000 × 1,000 B | same |
| `coll` | 2 lists × 2,000,000 elements, 2 hashes × 1,000,000 fields, 100,000 × 1,000 B | every 10 ms (and at once on the BGSAVE reply) one `RPUSH` per list and one `HSET` of an existing field per hash |
| `slow-small` | as `small` | none; needs `--save-delay-us` |
| `slow-str10m` | as `str10m` | as `str10m`; needs `--save-delay-us` |
| `idle` | as `small` | no traffic: `BGSAVE` (monitor polling only), then blocking `SAVE` |

Foreground traffic always targets the 1,000-byte `s:<n>` keys.

## Reproducing each scenario (one command each)

```
target\release\rexyra-bench.exe run --scenario small   --runs 3
target\release\rexyra-bench.exe run --scenario str1m   --runs 3
target\release\rexyra-bench.exe run --scenario str10m  --runs 3
target\release\rexyra-bench.exe run --scenario str100m --runs 3
target\release\rexyra-bench.exe run --scenario str1m   --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario str10m  --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario str100m --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario coll    --runs 3
target\release\rexyra-bench.exe run --scenario slow-small  --runs 3 --save-delay-us 28
target\release\rexyra-bench.exe run --scenario slow-str10m --runs 3 --save-delay-us 170
target\release\rexyra-bench.exe run --scenario idle    --runs 3
target\release\rexyra-bench.exe summarize > summary.md
```

(The delays above are the values used for the published report; see the
report for how they were calibrated. Calibration runs were written with
`--label calib-<delay> --out docs\benchmarks\bgsave\raw\calibration`.)

`--no-toucher` runs a scenario without its toucher (to separate what the
save does to small-key traffic from what overwriting large values does).

Each ordinary run takes about 25 s and each slow-disk run 55–75 s (mostly
loading and reloading); the whole set takes about 25 minutes. Run nothing else heavy at the same time,
and one scenario at a time.

## Limits

- Closed-loop clients: each connection waits for its reply before sending
  the next request, so when the server stalls, fewer requests are issued and
  the stall shows up in fewer samples than an open-loop load would give
  (coordinated omission). The longest-gap figure and the per-50-ms timeline
  in each JSON file are the direct stall measures.
- `used_memory` is sampled, not tracked: a peak between two samples is
  missed. The JSON records the longest interval between samples.
- The save's end is seen by polling, so the save duration is known to within
  one poll interval plus an INFO round trip (about 10–12 ms; longer while
  the command thread is stalled).
- Client and server share one machine and its CPUs.
