# BGSAVE under load: latency, throughput and memory

> **This is the first round, on the build that had the two defects it describes.** Both have since been fixed and everything was measured again: see [after-fix.md](after-fix.md) for the results on the fixed build and the before/after comparison.

Measured 2026-11-20 with `tools/rexyra-bench` on one machine. Every figure comes from the JSON files in `raw/` (one per run); the tables under "Results per scenario" are generated from them by `rexyra-bench summarize`. What was **measured** and what is **explanation** are kept apart.

The measurements were then checked independently of the tool that produced them: see "How these results were checked".

## Summary

1. **Small values (1 KB): fine.** Throughput −7 % during a save, p99 0.78 → 0.80 ms, one stall of about 10 ms per save.
2. **Large strings: a defect.** With 10 MB or 100 MB strings in the keyspace, a save cut throughput by 71–86 % and raised p99 from under 1 ms to 16–30 ms, whether or not anything was overwriting those strings. Cause, confirmed by experiment: when the save's queue is full, the command thread waits for the writer with a 1 ms timeout that Windows stretches to about 15 ms, and arriving commands do not end the wait.
3. **Slow disk: the same defect.** With the writer slowed about tenfold, throughput fell 68 % and p50 rose from 0.45 to 1.5 ms for the whole 43 s save. The claim "a slow disk slows the save down and nothing else" did not hold as built.
4. **With that wait removed (experimental build), both regressions disappear**: throughput and p99 during a save are the same as with no save, for 10 MB strings, 100 MB strings and the slow disk. The slow-disk run then shows the memory cost the design accepts: +294 MiB of old versions queued, where the throttled server showed +33 MiB.
5. **Large collections stall as predicted**: the first write to each during a save took about 17–23 ms for a 2,000,000-element list and 87–100 ms for a 1,000,000-field hash; four collections touched together stalled the server for about 225 ms in total.
6. **A save leaves memory behind**: the first save after a key and its value are written costs 24 bytes for each, permanently (48 MB per million keys). Later saves add nothing.
7. Every one of the 39 runs ended with a good snapshot that reloaded to exactly the key count at the moment `BGSAVE` was issued.

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 9 7900X, 12 cores / 24 logical CPUs |
| RAM | 63.2 GB |
| OS | Windows 11 Pro 10.0.26300, power plan "Balanced" |
| Disk | Samsung SSD 990 PRO 1 TB NVMe, NTFS |
| Toolchain | rustc/cargo 1.100.0, release profile (`lto = "thin"`, `codegen-units = 1`) |
| Server | `target\release\rexyra-server.exe`, built 2026-11-20 19:07, not rebuilt during the measurements |
| Load generator | `target\release\rexyra-bench.exe`, same machine, loopback TCP, no Docker |

Scenarios ran one after another with nothing else heavy running. Processes were not pinned to CPUs.

## Method

For every run:

1. A fresh server process on a fresh data directory (`--save ""`). The dataset is loaded and `DBSIZE` checked; `rdb-key-save-delay` is set where the scenario calls for it.
2. Traffic: 50 closed-loop connections doing 50/50 `GET`/`SET` (1,000-byte values) on uniformly random existing small keys; an optional "toucher" connection (per scenario); a monitor connection sending `INFO memory` and `INFO persistence` every 10 ms.
3. 1 s warm-up (discarded), 5 s baseline, then `BGSAVE`. **Time zero is when the `BGSAVE` reply is read.**
4. The "during" window ends when the monitor reads the first reply saying the save is no longer in progress. The "after" window is the next 3 s.
5. Checks: `rdb_last_bgsave_status`, the size of `dump.rdb`, then `SHUTDOWN NOSAVE` and a new server on the same directory, whose `DBSIZE` (and list lengths) must equal the values at time zero. The traffic never creates or deletes keys. Done for every run.

**Latency** is per request, from just before the request is written to after the full reply is read, on a monotonic clock. Every sample is kept and percentiles are exact (nearest rank). A sample belongs to a window by its completion time. **Longest gap** is the longest interval in a window in which no foreground request completed on any connection.

### Limits

- **Closed loop.** During a stall the clients stop sending, so a stall produces at most 50 slow samples and the percentiles under-report it. The longest gap, the 50 ms timeline in each file and the throughput are the measures to trust for stalls.
- **Thin tails.** p99.9 rests on 455–465 samples above it for small values, but only 40–46 for 100 MB strings and collections. p99.99 is in the files but should not be quoted for those.
- **Window edges.** The "during" window, and so the save duration, can run up to 12.5 ms past the moment the server finished (one poll interval plus a round trip). Requests in flight when `BGSAVE` was sent (at most about 170 of 580,000) may be on the wrong side of time zero.
- **The monitor is answered by the thread that stalls**, so memory samples during a stall are late: up to 101 ms apart in the collections scenario, 41–47 ms with 100 MB strings, 30 ms or less elsewhere.
- **Client side not profiled.** The load generator's own CPU use was not measured and samples carry no connection id. That the slow-down is in the server is shown instead by the monitor connection's `INFO` round trips (0.4 ms in baseline, up to 16–18 ms during a save with large strings) and by the experiment below.
- **Three builds of the tool** produced the runs (main scenarios; slow-disk, calibration and idle; no-toucher). The differences were an idle-run sleep, a command-line flag, a column label and formatting; none in the latency, window or loading code. The re-runs used the final build and agree.
- 3 runs per configuration; medians are shown next to every run.
- `SAVE` was always timed after `BGSAVE` on the same server, so "BGSAVE is not slower than SAVE when idle" is as far as that comparison goes.

## Headline (medians of 3; baseline → during the save)

| scenario | ops/s | p50 ms | p99 ms | max ms | longest gap ms | save | used_memory peak − before |
|---|---|---|---|---|---|---|---|
| small, 1M × 1 KB | 116,347 → 107,651 (−7 %) | 0.416 → 0.452 | 0.779 → 0.803 | 3.35 → 10.73 | 0.81 → 10.00 | 4.30 s | +37.8 MiB |
| 1 MiB strings + toucher | 98,535 → 38,018 (−61 %) | 0.481 → 0.878 | 0.94 → 2.94 | 3.69 → 6.18 | 0.90 → 4.76 | 2.77 s | +18.9 MiB |
| 1 MiB strings, no toucher | 118,134 → 38,981 (−67 %) | 0.404 → 0.786 | 0.83 → 2.88 | 3.57 → 10.02 | 2.02 → 9.43 | 2.74 s | +3.3 MiB |
| 10 MiB strings + toucher | 89,262 → 25,743 (−71 %) | 0.455 → 0.529 | 2.10 → 16.09 | 3.19 → 18.13 | 2.37 → 17.55 | 2.59 s | +161 MiB |
| 10 MiB strings, no toucher | 118,561 → 28,662 (−76 %) | 0.403 → 0.480 | 0.80 → 15.45 | 3.21 → 17.52 | 0.76 → 16.40 | 2.50 s | +3.6 MiB |
| 100 MiB strings + toucher | 86,403 → 15,888 (−82 %) | 0.446 → 0.518 | 1.22 → 30.30 | 20.36 → 35.28 | 19.68 → 34.56 | 2.60 s | +801 MiB |
| 100 MiB strings, no toucher | 119,061 → 16,576 (−86 %) | 0.402 → 0.552 | 0.81 → 16.70 | 3.41 → 18.19 | 0.82 → 17.58 | 2.49 s | +4.5 MiB |
| collections (2 lists × 2M, 2 hashes × 1M) | 117,938 → 70,117 | 0.408 → 0.429 | 0.79 → 0.92 | 3.51 → 99.96 | 0.72 → 99.25 | 0.66 s | +64.2 MiB |
| slow disk, small (28 µs/key) | 114,810 → 34,111 (−70 %) | 0.416 → 1.524 | 0.83 → 2.05 | 3.37 → 4.75 | 0.84 → 3.21 | 43.29 s | +33.3 MiB |
| slow disk, 10 MiB (170 µs/key) | 88,939 → 32,692 (−63 %) | 0.454 → 1.507 | 2.12 → 4.20 | 3.66 → 18.37 | 2.19 → 17.86 | 27.76 s | +826 MiB |
| idle | – | – | – | – | – | BGSAVE 2.59 s, SAVE 2.89 s | +47.3 MiB |

## Findings

**Small values.** Throughput fell 7 % and the percentiles barely moved. Each run had one foreground stall of 9–10 ms at a different point in the save. The save took 4.2–4.4 s under load against 2.6 s idle. Of the +37.8 MiB at the peak, +33.7 MiB was still there after the save (see "Memory a save leaves allocated").

**Large strings.** The largest regressions, and they are as large with no toucher as with one, so they are not caused by overwriting large values. In the 10 MiB no-toucher runs, 84 % of the 50 ms buckets during the save contain a request slower than 10 ms and 64 % one slower than 14 ms; with 100 MiB strings, 92 % have one slower than 14 ms. This fails the design note's own threshold (p99 more than five times the idle p99) for 10 MiB and 100 MiB strings.

Overwriting a large string (the toucher), baseline → during: 1 MiB p50 1.32 → 2.04 ms; 10 MiB p50 8.47 → 10.32 ms, p99 9.85 → 31.2 ms; 100 MiB p50 75.8 → 49.7 ms. An overwrite costs time in proportion to its size with or without a save: in baseline a 100 MiB `SET` already stalls other clients for 15–20 ms.

Memory: with the toucher the peak rose by the old versions kept until written (19 MiB, 131–191 MiB and 801–902 MiB for the three sizes) and came back afterwards; without it, 3–5 MiB.

**Collections.** The toucher's first command to each collection after `BGSAVE`, median of 3: `RPUSH` to the two lists 16.7 and 22.8 ms, `HSET` to the two hashes 99.8 and 87.3 ms; later ones were sub-millisecond again. Other clients saw these as four stalls back to back, about 225 ms in total, the longest 97–101 ms. p99 hardly moved (0.79 → 0.92 ms); p99.9 went to about 100 ms. This is the stall the design note describes ("once per collection per save"), at about the size it gives; one run in three was just above the note's 100 ms line. Not measured: whether the walk reaching a collection by itself stalls the same way (expected, since it encodes on the same thread).

**Slow disk.** Delays of 28 µs and 170 µs per key made the saves 10.1 and 10.7 times longer (43.3 s and 27.7 s). Latency did get worse for small values: p50 1.52 ms against 0.45 ms in the un-slowed save, p99 2.05 against 0.80 ms, throughput 34,111 against 107,651 ops/s, for the whole save. For 10 MiB strings p50 was worse (1.51 against 0.53 ms) and p99 better (4.2 against 16.1 ms). The memory claim held: +785 to +846 MiB with the toucher overwriting 10 MiB strings throughout, under the ceiling of 1,000 MiB (every large string overwritten once before being written) plus 16 MB, and back to +2.3 MiB afterwards.

**Idle.** `BGSAVE` took 2.55–2.60 s with only the monitor's polls as traffic; a blocking `SAVE` right after took 2.87–2.95 s. Both files were 1,011,888,967 bytes.

**Memory a save leaves allocated.** On an idle server `used_memory` rose by exactly 48,000,384 bytes over 1,000,000 keys and was unchanged to the byte 30 s later. On 200,000 keys: the first `BGSAVE` adds 48.00 bytes per key, the second and third add nothing, and after every key is `SET` again the next one adds 24.00 bytes per key. So it is a one-time 24 bytes for a key and 24 bytes for a value, the first time a save hands each to the writer; a `SET` of an existing key replaces the value and not the stored key, hence the half. Explanation (from how the `bytes` crate works, not traced in a debugger): keys and string values are stored as buffers that have never been shared; the first time one is shared, here with the writer thread, the library allocates a 24-byte reference-count block for it, which lives as long as the buffer. This is about 4 % of memory with 1 KB values and more with smaller ones, and it is not covered by the documented "16 MB plus old versions".

## How these results were checked

After the runs, independently of the tool's own summary:

- **The tool's measuring code was read**: the request timing (`run.rs`, `worker`), the window test and the percentile rule (`stats.rs`). No fault found.
- **The headline table was recomputed from the raw files with an independent script**; every figure matched. Each file was tested for internal consistency (throughput against count and window length, percentile order, save duration against event times, peak memory against the memory samples, the timeline against the request counts, status and reload flags): no discrepancy in 33 files.
- **Three scenarios were run again** with the final tool build (`raw/verification/rerun/`). Small values: 112,893 → 107,155 ops/s, p99 0.88 → 0.84 ms, one 7.9 ms gap, save 4.27 s. 10 MiB strings without toucher: 116,099 → 28,752 ops/s, p99 0.83 → 15.64 ms. Collections: longest gap 92.6 ms. All in line with the reported runs, except that the collections run peaked at +32 MiB where the reported median is +64 MiB.
- **The method was reviewed** for window attribution, the sample counts behind the percentiles, what had been ruled out on the client side, the timer test, the evidence for the 24 bytes and the build history. That review led to the corrections now in "Limits" and to more careful wording in "Large strings".
- **The explanation for the regressions was tested by experiment.** The measurements alone only suggested that the cause was the wait in `bgsave_wait_for_room`. A server built with that wait's timeout set to zero (`raw/verification/no-wait-experiment/`, one run each) gave, baseline → during:

| scenario | ops/s | p99 ms | longest gap ms | save | memory peak − before |
|---|---|---|---|---|---|
| small | 114,217 → 115,553 | 0.83 → 0.85 | 0.75 → 0.45 | 4.14 s | +37.7 MiB |
| 10 MiB strings, no toucher | 116,932 → 118,188 | 0.81 → 0.85 | 0.80 → 0.53 | 3.30 s | +5.5 MiB |
| 100 MiB strings, no toucher | 118,619 → 115,443 | 0.79 → 0.85 | 0.81 → 0.57 | 3.48 s | +18.1 MiB |
| slow disk, small (28 µs/key) | 114,987 → 116,372 | 0.82 → 0.84 | 0.86 → 0.54 | 44.47 s | +293.6 MiB |

  The regressions are gone, as is the 10 ms stall in the small-value runs, so the explanation is confirmed: the wait is the cause. Saves over large strings took about 0.8 s longer. The experimental build is not a fix (it spins a processor while the queue is full and no command is waiting); it only removes the wait to show what the wait costs.

  The slow-disk row also shows what the wait had been hiding: with commands running at full speed for 44 s, the old versions of overwritten keys accumulate faster than the slowed writer takes them (+294 MiB here). That is the cost the design note states and accepts ("16 MB plus the old versions of what commands change"; unbounded with a stalled disk and a write-heavy load), now measured.

## What contradicts the design note and README

1. "Commands never wait for the disk" and "a slow disk slows the save down and nothing else" were not true of the build measured: commands waited for the writer through the idle wait. Fixing that wait makes them true (experiment), at the memory cost in the last row above.
2. "A save in progress uses up to 16 MB ... plus the old versions" omits the permanent 24 bytes per key and per value.
3. "Capturing a string costs nothing whatever its size" is consistent with the toucher data, but a save over large strings was not free for other commands, for reason 1.
4. The collection stall is as described; the note does not say that several collections touched together stall one after another.

## Results per scenario

Every run and the median, generated by `rexyra-bench summarize` from `raw/*.json`. Latencies in ms, memory in MiB (2^20 bytes).

### small

1,000,000 strings of 1,000 bytes; foreground GET/SET on them.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 584356 | 116813 | 0.413 | 0.644 | 0.786 | 1.041 | 3.28 | 0.81 |
| 1 | during | 4.30 | 465521 | 108356 | 0.449 | 0.673 | 0.798 | 1.035 | 10.73 | 10.00 |
| 1 | after | 3.00 | 348484 | 116138 | 0.415 | 0.646 | 0.791 | 1.052 | 3.31 | 0.44 |
| 2 | baseline | 5.00 | 582054 | 116347 | 0.416 | 0.639 | 0.774 | 1.012 | 3.35 | 1.05 |
| 2 | during | 4.43 | 455434 | 102808 | 0.472 | 0.714 | 0.854 | 1.141 | 10.59 | 9.14 |
| 2 | after | 3.00 | 329551 | 109828 | 0.439 | 0.683 | 0.835 | 1.168 | 3.52 | 0.48 |
| 3 | baseline | 5.00 | 581284 | 116200 | 0.416 | 0.642 | 0.779 | 1.016 | 3.41 | 0.73 |
| 3 | during | 4.24 | 456747 | 107651 | 0.452 | 0.676 | 0.803 | 1.108 | 10.74 | 10.18 |
| 3 | after | 3.00 | 342932 | 114230 | 0.422 | 0.658 | 0.806 | 1.094 | 3.19 | 0.40 |
| **median** | baseline | 5.00 | 582054 | 116347 | 0.416 | 0.642 | 0.779 | 1.016 | 3.35 | 0.81 |
| **median** | during | 4.30 | 456747 | 107651 | 0.452 | 0.676 | 0.803 | 1.108 | 10.73 | 10.00 |
| **median** | after | 3.00 | 342932 | 114230 | 0.422 | 0.658 | 0.806 | 1.094 | 3.31 | 0.44 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4.296 | 965.0 | 1221.0 | 1221.0 | 1258.7 | 37.7 | 37.7 | 1538.8 | 1539.7 | 1254.6 | 20.0 |
| 2 | 4.430 | 965.0 | 1221.0 | 1221.0 | 1258.7 | 37.8 | 37.8 | 1514.7 | 1532.7 | 1254.7 | 11.8 |
| 3 | 4.243 | 965.0 | 1220.9 | 1220.9 | 1258.7 | 37.9 | 37.8 | 1515.1 | 1530.6 | 1254.7 | 20.4 |
| **median** | 4.296 | 965.0 | 1221.0 | 1221.0 | 1258.7 | 37.8 | 37.8 | 1515.1 | 1532.7 | 1254.7 | 20.0 |

### str1m

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 493116 | 98535 | 0.481 | 0.786 | 0.928 | 1.149 | 3.69 | 0.90 |
| 1 | during | 2.80 | 106597 | 38018 | 0.861 | 2.531 | 2.943 | 3.195 | 6.18 | 2.81 |
| 1 | after | 3.00 | 294288 | 98074 | 0.485 | 0.786 | 0.925 | 1.125 | 3.24 | 0.79 |
| 2 | baseline | 5.00 | 487259 | 97379 | 0.486 | 0.804 | 0.957 | 1.199 | 3.60 | 1.01 |
| 2 | during | 2.77 | 104736 | 37770 | 0.891 | 2.539 | 2.976 | 3.379 | 5.33 | 4.76 |
| 2 | after | 3.00 | 293332 | 97681 | 0.485 | 0.796 | 0.944 | 1.183 | 3.42 | 0.84 |
| 3 | baseline | 5.00 | 495185 | 98960 | 0.478 | 0.789 | 0.937 | 1.177 | 3.85 | 0.85 |
| 3 | during | 2.74 | 105474 | 38497 | 0.878 | 2.507 | 2.880 | 3.269 | 8.12 | 7.46 |
| 3 | after | 3.00 | 295164 | 98355 | 0.480 | 0.796 | 0.951 | 1.208 | 3.31 | 2.00 |
| **median** | baseline | 5.00 | 493116 | 98535 | 0.481 | 0.789 | 0.937 | 1.177 | 3.69 | 0.90 |
| **median** | during | 2.77 | 105474 | 38018 | 0.878 | 2.531 | 2.943 | 3.269 | 6.18 | 4.76 |
| **median** | after | 3.00 | 294288 | 98074 | 0.485 | 0.796 | 0.944 | 1.183 | 3.31 | 0.84 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.804 | 1096.4 | 1126.4 | 1126.4 | 1145.3 | 18.8 | 18.8 | 1169.3 | 1187.5 | 1128.7 | 12.3 |
| 2 | 2.773 | 1096.4 | 1126.4 | 1126.5 | 1145.3 | 18.9 | 18.8 | 1167.4 | 1185.6 | 1128.7 | 12.6 |
| 3 | 2.740 | 1096.4 | 1126.4 | 1126.4 | 1145.4 | 19.0 | 18.9 | 1168.5 | 1187.2 | 1128.7 | 13.1 |
| **median** | 2.773 | 1096.4 | 1126.4 | 1126.4 | 1145.3 | 18.9 | 18.8 | 1168.5 | 1187.2 | 1128.7 | 12.6 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 3783 | 1.313 | 1.849 | 2.37 |
| 1 | during | 1404 | 2.043 | 4.430 | 5.04 |
| 1 | after | 2268 | 1.309 | 1.901 | 6.18 |
| 2 | baseline | 3725 | 1.328 | 1.951 | 2.88 |
| 2 | during | 1401 | 2.046 | 4.161 | 8.87 |
| 2 | after | 2246 | 1.327 | 1.845 | 2.42 |
| 3 | baseline | 3749 | 1.324 | 1.911 | 2.29 |
| 3 | during | 1400 | 2.011 | 4.153 | 7.77 |
| 3 | after | 2237 | 1.329 | 1.960 | 2.90 |
| **median** | baseline | 3749 | 1.324 | 1.911 | 2.37 |
| **median** | during | 1401 | 2.043 | 4.161 | 7.77 |
| **median** | after | 2246 | 1.327 | 1.901 | 2.90 |

### str1m (no-toucher)

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 579915 | 115924 | 0.412 | 0.661 | 0.844 | 1.248 | 3.89 | 2.42 |
| 1 | during | 2.70 | 103787 | 38462 | 0.873 | 2.573 | 2.844 | 3.231 | 10.87 | 10.36 |
| 1 | after | 3.00 | 345748 | 115171 | 0.414 | 0.666 | 0.852 | 1.320 | 3.40 | 2.39 |
| 2 | baseline | 5.00 | 590963 | 118134 | 0.404 | 0.649 | 0.830 | 1.247 | 3.39 | 2.02 |
| 2 | during | 2.75 | 107099 | 38981 | 0.786 | 2.615 | 2.876 | 4.154 | 9.68 | 9.19 |
| 2 | after | 3.00 | 356037 | 118602 | 0.404 | 0.643 | 0.820 | 1.305 | 3.28 | 1.17 |
| 3 | baseline | 5.00 | 593626 | 118675 | 0.403 | 0.643 | 0.822 | 1.255 | 3.57 | 1.33 |
| 3 | during | 2.74 | 107773 | 39306 | 0.772 | 2.581 | 2.895 | 3.331 | 10.02 | 9.43 |
| 3 | after | 3.00 | 358887 | 119565 | 0.402 | 0.631 | 0.784 | 1.100 | 3.21 | 1.44 |
| **median** | baseline | 5.00 | 590963 | 118134 | 0.404 | 0.649 | 0.830 | 1.248 | 3.57 | 2.02 |
| **median** | during | 2.74 | 107099 | 38981 | 0.786 | 2.581 | 2.876 | 3.331 | 10.02 | 9.43 |
| **median** | after | 3.00 | 356037 | 118602 | 0.404 | 0.643 | 0.820 | 1.305 | 3.28 | 1.44 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.698 | 1096.4 | 1124.4 | 1124.4 | 1127.7 | 3.3 | 3.2 | 1167.8 | 1169.6 | 1126.8 | 12.5 |
| 2 | 2.747 | 1096.4 | 1124.4 | 1124.4 | 1127.7 | 3.3 | 3.2 | 1166.3 | 1168.8 | 1126.7 | 16.6 |
| 3 | 2.742 | 1096.4 | 1124.4 | 1124.4 | 1127.6 | 3.2 | 3.1 | 1166.7 | 1170.2 | 1126.7 | 12.6 |
| **median** | 2.742 | 1096.4 | 1124.4 | 1124.4 | 1127.7 | 3.3 | 3.2 | 1166.7 | 1169.6 | 1126.7 | 12.6 |

### str10m

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 451184 | 90128 | 0.451 | 1.789 | 2.094 | 2.509 | 3.19 | 2.37 |
| 1 | during | 2.59 | 69810 | 26944 | 0.527 | 11.305 | 16.092 | 17.585 | 18.03 | 17.25 |
| 1 | after | 3.00 | 256219 | 85372 | 0.472 | 1.903 | 2.240 | 2.564 | 3.25 | 2.15 |
| 2 | baseline | 5.01 | 444944 | 88779 | 0.457 | 1.810 | 2.109 | 2.489 | 3.22 | 2.55 |
| 2 | during | 2.62 | 67332 | 25743 | 0.537 | 11.845 | 16.334 | 18.123 | 18.47 | 17.89 |
| 2 | after | 3.00 | 250293 | 83423 | 0.483 | 1.950 | 2.247 | 2.592 | 3.84 | 2.52 |
| 3 | baseline | 5.01 | 446877 | 89262 | 0.455 | 1.798 | 2.100 | 2.419 | 2.94 | 2.18 |
| 3 | during | 2.54 | 64238 | 25253 | 0.529 | 12.226 | 16.017 | 17.802 | 18.13 | 17.55 |
| 3 | after | 3.00 | 259787 | 86517 | 0.469 | 1.870 | 2.191 | 2.491 | 3.34 | 2.36 |
| **median** | baseline | 5.01 | 446877 | 89262 | 0.455 | 1.798 | 2.100 | 2.489 | 3.19 | 2.37 |
| **median** | during | 2.59 | 67332 | 25743 | 0.529 | 11.845 | 16.092 | 17.802 | 18.13 | 17.55 |
| **median** | after | 3.00 | 256219 | 85372 | 0.472 | 1.903 | 2.240 | 2.564 | 3.34 | 2.36 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.591 | 1096.4 | 1140.2 | 1140.2 | 1270.9 | 130.7 | 130.7 | 1178.6 | 1308.8 | 1142.5 | 20.0 |
| 2 | 2.616 | 1096.4 | 1140.2 | 1140.2 | 1331.1 | 190.9 | 190.9 | 1179.7 | 1369.9 | 1142.5 | 24.1 |
| 3 | 2.544 | 1096.4 | 1140.2 | 1140.2 | 1301.1 | 160.9 | 160.9 | 1178.5 | 1338.7 | 1142.5 | 20.3 |
| **median** | 2.591 | 1096.4 | 1140.2 | 1140.2 | 1301.1 | 160.9 | 160.9 | 1178.6 | 1338.7 | 1142.5 | 20.3 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 593 | 8.464 | 9.646 | 10.47 |
| 1 | during | 196 | 10.320 | 33.705 | 34.21 |
| 1 | after | 344 | 8.764 | 10.434 | 10.95 |
| 2 | baseline | 594 | 8.471 | 9.924 | 10.87 |
| 2 | during | 206 | 9.799 | 30.583 | 32.32 |
| 2 | after | 348 | 8.640 | 9.810 | 10.13 |
| 3 | baseline | 591 | 8.498 | 9.854 | 10.36 |
| 3 | during | 195 | 11.312 | 31.157 | 32.17 |
| 3 | after | 348 | 8.627 | 9.873 | 9.95 |
| **median** | baseline | 593 | 8.471 | 9.854 | 10.47 |
| **median** | during | 196 | 10.320 | 31.157 | 32.32 |
| **median** | after | 348 | 8.640 | 9.873 | 10.13 |

### str10m (no-toucher)

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 587892 | 117524 | 0.409 | 0.643 | 0.805 | 1.087 | 3.19 | 0.76 |
| 1 | during | 2.55 | 73196 | 28662 | 0.505 | 11.151 | 15.453 | 16.747 | 17.52 | 16.40 |
| 1 | after | 3.00 | 346946 | 115503 | 0.415 | 0.662 | 0.838 | 1.147 | 3.12 | 0.57 |
| 2 | baseline | 5.00 | 599582 | 119863 | 0.401 | 0.633 | 0.797 | 1.062 | 3.21 | 0.76 |
| 2 | during | 2.48 | 70224 | 28329 | 0.480 | 11.356 | 15.606 | 16.577 | 17.03 | 16.35 |
| 2 | after | 3.00 | 356931 | 118888 | 0.405 | 0.635 | 0.795 | 1.056 | 3.20 | 0.45 |
| 3 | baseline | 5.00 | 593095 | 118561 | 0.403 | 0.649 | 0.826 | 1.151 | 3.34 | 0.88 |
| 3 | during | 2.50 | 75328 | 30092 | 0.460 | 10.192 | 15.400 | 17.118 | 18.52 | 17.85 |
| 3 | after | 3.00 | 364172 | 121283 | 0.397 | 0.625 | 0.781 | 1.036 | 3.14 | 0.43 |
| **median** | baseline | 5.00 | 593095 | 118561 | 0.403 | 0.643 | 0.805 | 1.087 | 3.21 | 0.76 |
| **median** | during | 2.50 | 73196 | 28662 | 0.480 | 11.151 | 15.453 | 16.747 | 17.52 | 16.40 |
| **median** | after | 3.00 | 356931 | 118888 | 0.405 | 0.635 | 0.795 | 1.056 | 3.14 | 0.45 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.554 | 1096.4 | 1124.2 | 1124.2 | 1127.6 | 3.4 | 3.4 | 1162.4 | 1165.3 | 1126.5 | 22.7 |
| 2 | 2.479 | 1096.4 | 1124.2 | 1124.2 | 1127.9 | 3.7 | 3.7 | 1163.6 | 1167.9 | 1126.5 | 23.2 |
| 3 | 2.503 | 1096.4 | 1124.2 | 1124.2 | 1127.8 | 3.6 | 3.6 | 1162.5 | 1167.2 | 1126.5 | 22.7 |
| **median** | 2.503 | 1096.4 | 1124.2 | 1124.2 | 1127.8 | 3.6 | 3.6 | 1162.5 | 1167.2 | 1126.5 | 22.7 |

### str100m

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 418509 | 83616 | 0.461 | 0.773 | 1.223 | 15.992 | 20.36 | 19.68 |
| 1 | during | 2.60 | 41330 | 15888 | 0.518 | 17.104 | 30.053 | 38.036 | 38.46 | 37.76 |
| 1 | after | 3.00 | 259113 | 86335 | 0.448 | 0.750 | 1.112 | 15.005 | 17.75 | 17.11 |
| 2 | baseline | 5.05 | 436506 | 86403 | 0.446 | 0.738 | 1.222 | 15.157 | 21.90 | 21.13 |
| 2 | during | 2.59 | 41285 | 15919 | 0.502 | 16.956 | 30.302 | 31.549 | 32.09 | 31.20 |
| 2 | after | 3.00 | 248741 | 82862 | 0.463 | 0.773 | 1.250 | 16.024 | 18.81 | 17.93 |
| 3 | baseline | 5.08 | 444738 | 87553 | 0.443 | 0.727 | 1.086 | 14.996 | 19.17 | 15.00 |
| 3 | during | 2.71 | 40683 | 15031 | 0.527 | 18.081 | 31.998 | 34.898 | 35.28 | 34.56 |
| 3 | after | 3.00 | 253959 | 84600 | 0.457 | 0.754 | 1.124 | 15.351 | 21.97 | 21.36 |
| **median** | baseline | 5.05 | 436506 | 86403 | 0.446 | 0.738 | 1.222 | 15.157 | 20.36 | 19.68 |
| **median** | during | 2.60 | 41285 | 15888 | 0.518 | 17.104 | 30.302 | 34.898 | 35.28 | 34.56 |
| **median** | after | 3.00 | 253959 | 84600 | 0.457 | 0.754 | 1.124 | 15.351 | 18.81 | 17.93 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.601 | 1096.4 | 1252.2 | 1252.2 | 2153.7 | 901.5 | 901.5 | 1291.4 | 2192.3 | 1254.6 | 46.3 |
| 2 | 2.593 | 1096.4 | 1252.2 | 1252.2 | 2053.2 | 801.0 | 800.9 | 1290.0 | 2090.2 | 1254.6 | 47.4 |
| 3 | 2.707 | 1096.4 | 1252.2 | 1252.3 | 2053.5 | 801.2 | 801.2 | 1291.3 | 2091.5 | 1254.6 | 41.5 |
| **median** | 2.601 | 1096.4 | 1252.2 | 1252.2 | 2053.5 | 801.2 | 801.2 | 1291.3 | 2091.5 | 1254.6 | 46.3 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 65 | 77.013 | 87.012 | 87.01 |
| 1 | during | 47 | 48.350 | 77.229 | 77.23 |
| 1 | after | 40 | 75.285 | 81.832 | 81.83 |
| 2 | baseline | 68 | 75.382 | 79.517 | 79.52 |
| 2 | during | 46 | 49.652 | 77.359 | 77.36 |
| 2 | after | 40 | 75.235 | 86.582 | 86.58 |
| 3 | baseline | 68 | 75.833 | 80.481 | 80.48 |
| 3 | during | 47 | 56.877 | 79.863 | 79.86 |
| 3 | after | 40 | 75.326 | 84.570 | 84.57 |
| **median** | baseline | 68 | 75.833 | 80.481 | 80.48 |
| **median** | during | 47 | 49.652 | 77.359 | 77.36 |
| **median** | after | 40 | 75.285 | 84.570 | 84.57 |

### str100m (no-toucher)

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 595447 | 119032 | 0.402 | 0.644 | 0.821 | 1.141 | 3.41 | 0.82 |
| 1 | during | 2.51 | 41556 | 16576 | 0.552 | 15.852 | 16.646 | 18.628 | 19.05 | 18.36 |
| 1 | after | 3.00 | 354402 | 118098 | 0.406 | 0.646 | 0.814 | 1.106 | 3.79 | 0.45 |
| 2 | baseline | 5.00 | 595555 | 119061 | 0.403 | 0.639 | 0.807 | 1.084 | 3.53 | 0.82 |
| 2 | during | 2.48 | 42662 | 17168 | 0.534 | 15.875 | 16.831 | 17.884 | 18.19 | 17.58 |
| 2 | after | 3.00 | 354794 | 118166 | 0.406 | 0.647 | 0.815 | 1.155 | 3.39 | 0.51 |
| 3 | baseline | 5.00 | 606097 | 121163 | 0.396 | 0.626 | 0.787 | 1.086 | 3.40 | 0.79 |
| 3 | during | 2.49 | 40996 | 16469 | 0.558 | 15.935 | 16.704 | 17.523 | 17.82 | 17.25 |
| 3 | after | 3.00 | 351734 | 117132 | 0.408 | 0.658 | 0.837 | 1.139 | 3.45 | 0.49 |
| **median** | baseline | 5.00 | 595555 | 119061 | 0.402 | 0.639 | 0.807 | 1.086 | 3.41 | 0.82 |
| **median** | during | 2.49 | 41556 | 16576 | 0.552 | 15.875 | 16.704 | 17.884 | 18.19 | 17.58 |
| **median** | after | 3.00 | 354402 | 118098 | 0.406 | 0.647 | 0.815 | 1.139 | 3.45 | 0.49 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.507 | 1096.4 | 1124.2 | 1124.2 | 1128.7 | 4.5 | 4.5 | 1162.8 | 1166.7 | 1126.5 | 25.9 |
| 2 | 2.485 | 1096.4 | 1124.2 | 1124.2 | 1129.1 | 4.9 | 4.9 | 1162.5 | 1169.2 | 1126.5 | 24.8 |
| 3 | 2.489 | 1096.4 | 1124.2 | 1124.2 | 1128.3 | 4.2 | 4.1 | 1164.3 | 1167.4 | 1126.5 | 25.1 |
| **median** | 2.489 | 1096.4 | 1124.2 | 1124.2 | 1128.7 | 4.5 | 4.5 | 1162.8 | 1167.4 | 1126.5 | 25.1 |

### coll

2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 590031 | 117938 | 0.409 | 0.638 | 0.787 | 1.060 | 3.51 | 0.82 |
| 1 | during | 0.66 | 46212 | 70117 | 0.429 | 0.668 | 0.906 | 101.132 | 101.50 | 100.84 |
| 1 | after | 3.00 | 355434 | 118456 | 0.408 | 0.631 | 0.775 | 1.013 | 3.21 | 0.56 |
| 2 | baseline | 5.00 | 589491 | 117835 | 0.408 | 0.639 | 0.792 | 1.076 | 3.20 | 0.72 |
| 2 | during | 0.68 | 48470 | 71728 | 0.431 | 0.669 | 0.929 | 97.564 | 98.05 | 97.38 |
| 2 | after | 3.00 | 357241 | 119031 | 0.405 | 0.630 | 0.774 | 1.026 | 3.67 | 0.67 |
| 3 | baseline | 5.00 | 599207 | 119763 | 0.403 | 0.627 | 0.773 | 1.019 | 3.52 | 0.71 |
| 3 | during | 0.66 | 45563 | 68683 | 0.425 | 0.668 | 0.922 | 99.589 | 99.96 | 99.25 |
| 3 | after | 3.00 | 362289 | 120715 | 0.401 | 0.619 | 0.755 | 0.984 | 3.34 | 0.28 |
| **median** | baseline | 5.00 | 590031 | 117938 | 0.408 | 0.638 | 0.787 | 1.060 | 3.51 | 0.72 |
| **median** | during | 0.66 | 46212 | 70117 | 0.429 | 0.668 | 0.922 | 99.589 | 99.96 | 99.25 |
| **median** | after | 3.00 | 357241 | 119031 | 0.405 | 0.630 | 0.774 | 1.013 | 3.34 | 0.56 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.659 | 158.7 | 476.9 | 477.0 | 541.1 | 64.2 | 64.1 | 602.8 | 636.3 | 479.2 | 101.0 |
| 2 | 0.676 | 158.7 | 476.9 | 477.0 | 541.1 | 64.2 | 64.1 | 602.5 | 636.0 | 479.2 | 97.8 |
| 3 | 0.663 | 158.7 | 476.9 | 477.0 | 541.1 | 64.2 | 64.1 | 602.9 | 636.4 | 479.2 | 99.8 |
| **median** | 0.663 | 158.7 | 476.9 | 477.0 | 541.1 | 64.2 | 64.1 | 602.8 | 636.3 | 479.2 | 99.8 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 1268 | 0.420 | 0.815 | 1.24 |
| 1 | during | 108 | 0.435 | 87.266 | 101.28 |
| 1 | after | 757 | 0.424 | 0.797 | 1.12 |
| 2 | baseline | 1268 | 0.424 | 0.810 | 1.33 |
| 2 | during | 112 | 0.441 | 85.335 | 97.73 |
| 2 | after | 760 | 0.409 | 0.829 | 1.06 |
| 3 | baseline | 1264 | 0.421 | 0.808 | 1.21 |
| 3 | during | 104 | 0.457 | 94.372 | 99.75 |
| 3 | after | 762 | 0.410 | 0.759 | 1.02 |
| **median** | baseline | 1268 | 0.421 | 0.810 | 1.24 |
| **median** | during | 108 | 0.441 | 87.266 | 99.75 |
| **median** | after | 760 | 0.410 | 0.797 | 1.06 |

Per collection: latency of the toucher's first command started after the BGSAVE reply, and its max during the save (ms):

| run | RPUSH list:0 first | RPUSH list:0 max | RPUSH list:1 first | RPUSH list:1 max | HSET hash:0 first | HSET hash:0 max | HSET hash:1 first | HSET hash:1 max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16.49 | 16.49 | 22.16 | 22.16 | 101.28 | 101.28 | 87.27 | 87.27 |
| 2 | 17.26 | 17.26 | 22.76 | 22.76 | 97.73 | 97.73 | 85.34 | 85.34 |
| 3 | 16.73 | 16.73 | 29.38 | 29.38 | 99.75 | 99.75 | 94.37 | 94.37 |
| **median** | 16.73 | 16.73 | 22.76 | 22.76 | 99.75 | 99.75 | 87.27 | 87.27 |

### slow-small

as 'small', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 28 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 573826 | 114701 | 0.418 | 0.664 | 0.829 | 1.147 | 3.37 | 0.84 |
| 1 | during | 43.37 | 1479456 | 34111 | 1.526 | 1.765 | 2.042 | 2.393 | 4.67 | 3.21 |
| 1 | after | 3.00 | 347805 | 115903 | 0.415 | 0.655 | 0.814 | 1.110 | 3.21 | 0.54 |
| 2 | baseline | 5.00 | 587166 | 117380 | 0.410 | 0.642 | 0.793 | 1.042 | 3.06 | 0.56 |
| 2 | during | 43.26 | 1477210 | 34149 | 1.523 | 1.760 | 2.048 | 2.432 | 8.70 | 8.25 |
| 2 | after | 3.00 | 348206 | 115961 | 0.414 | 0.653 | 0.817 | 1.201 | 3.14 | 2.05 |
| 3 | baseline | 5.00 | 574352 | 114810 | 0.416 | 0.666 | 0.841 | 1.307 | 3.45 | 1.53 |
| 3 | during | 43.29 | 1475244 | 34080 | 1.524 | 1.769 | 2.073 | 2.556 | 4.75 | 3.07 |
| 3 | after | 3.00 | 344527 | 114788 | 0.415 | 0.672 | 0.857 | 1.309 | 3.18 | 1.63 |
| **median** | baseline | 5.00 | 574352 | 114810 | 0.416 | 0.664 | 0.829 | 1.147 | 3.37 | 0.84 |
| **median** | during | 43.29 | 1477210 | 34111 | 1.524 | 1.765 | 2.048 | 2.432 | 4.75 | 3.21 |
| **median** | after | 3.00 | 347805 | 115903 | 0.415 | 0.655 | 0.817 | 1.201 | 3.18 | 1.63 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 43.372 | 965.0 | 1220.9 | 1220.9 | 1254.2 | 33.3 | 33.3 | 1530.8 | 1531.9 | 1251.3 | 12.9 |
| 2 | 43.258 | 965.0 | 1221.0 | 1221.0 | 1254.2 | 33.2 | 33.2 | 1540.1 | 1541.1 | 1251.4 | 12.7 |
| 3 | 43.287 | 965.0 | 1220.9 | 1220.9 | 1254.2 | 33.3 | 33.3 | 1543.8 | 1545.0 | 1251.3 | 13.6 |
| **median** | 43.287 | 965.0 | 1220.9 | 1220.9 | 1254.2 | 33.3 | 33.3 | 1540.1 | 1541.1 | 1251.3 | 12.9 |

### slow-str10m

as 'str10m', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 170 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 448803 | 89641 | 0.450 | 1.800 | 2.100 | 2.487 | 4.74 | 2.19 |
| 1 | during | 27.76 | 903614 | 32553 | 1.512 | 3.007 | 4.132 | 16.169 | 19.02 | 18.23 |
| 1 | after | 3.00 | 254017 | 84599 | 0.473 | 1.912 | 2.224 | 2.807 | 3.38 | 2.73 |
| 2 | baseline | 5.01 | 440687 | 87998 | 0.460 | 1.838 | 2.141 | 2.427 | 2.98 | 2.08 |
| 2 | during | 27.77 | 907735 | 32692 | 1.507 | 3.038 | 4.205 | 16.413 | 18.19 | 17.67 |
| 2 | after | 3.00 | 266953 | 88950 | 0.457 | 1.813 | 2.105 | 2.401 | 2.88 | 2.03 |
| 3 | baseline | 5.01 | 445626 | 88939 | 0.454 | 1.821 | 2.122 | 2.479 | 3.66 | 2.75 |
| 3 | during | 27.68 | 916525 | 33107 | 1.496 | 3.034 | 4.327 | 16.502 | 18.37 | 17.86 |
| 3 | after | 3.00 | 264755 | 88236 | 0.457 | 1.827 | 2.135 | 2.522 | 3.50 | 2.43 |
| **median** | baseline | 5.01 | 445626 | 88939 | 0.454 | 1.821 | 2.122 | 2.479 | 3.66 | 2.19 |
| **median** | during | 27.76 | 907735 | 32692 | 1.507 | 3.034 | 4.205 | 16.413 | 18.37 | 17.86 |
| **median** | after | 3.00 | 264755 | 88236 | 0.457 | 1.827 | 2.135 | 2.522 | 3.38 | 2.43 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 27.759 | 1096.4 | 1140.2 | 1140.2 | 1966.6 | 826.4 | 826.4 | 1179.9 | 2000.4 | 1142.5 | 25.1 |
| 2 | 27.766 | 1096.4 | 1140.2 | 1140.2 | 1925.2 | 785.0 | 784.9 | 1178.8 | 1959.5 | 1142.5 | 26.5 |
| 3 | 27.683 | 1096.4 | 1140.2 | 1140.2 | 1986.1 | 845.9 | 845.9 | 1178.8 | 2019.2 | 1142.5 | 29.5 |
| **median** | 27.759 | 1096.4 | 1140.2 | 1140.2 | 1966.6 | 826.4 | 826.4 | 1178.8 | 2000.4 | 1142.5 | 26.5 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 597 | 8.369 | 9.996 | 12.76 |
| 1 | during | 3973 | 6.383 | 18.312 | 22.80 |
| 1 | after | 354 | 8.505 | 9.886 | 10.58 |
| 2 | baseline | 588 | 8.560 | 9.800 | 10.52 |
| 2 | during | 4078 | 6.283 | 18.770 | 23.08 |
| 2 | after | 353 | 8.514 | 9.935 | 10.72 |
| 3 | baseline | 605 | 8.306 | 10.001 | 10.83 |
| 3 | during | 4069 | 6.292 | 18.262 | 22.18 |
| 3 | after | 356 | 8.454 | 10.018 | 10.76 |
| **median** | baseline | 597 | 8.369 | 9.996 | 10.83 |
| **median** | during | 4069 | 6.292 | 18.312 | 22.80 |
| **median** | after | 354 | 8.505 | 9.935 | 10.72 |

### idle

the 'small' dataset; BGSAVE with no traffic, then a blocking SAVE.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

| run | BGSAVE s | BGSAVE file MiB | SAVE s | SAVE file MiB | used before MiB | peak used during MiB | peak − before MiB | RSS peak MiB | used at end of after-window MiB | max INFO gap ms | reload s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.586 | 965.0 | 2.866 | 965.0 | 1214.3 | 1261.7 | 47.4 | 1327.2 | 1260.1 | 13.5 | 2.78 |
| 2 | 2.597 | 965.0 | 2.952 | 965.0 | 1214.3 | 1261.6 | 47.3 | 1327.7 | 1260.1 | 10.7 | 2.80 |
| 3 | 2.553 | 965.0 | 2.892 | 965.0 | 1214.3 | 1261.6 | 47.3 | 1326.1 | 1260.1 | 12.3 | 2.80 |
| **median** | 2.586 | 965.0 | 2.892 | 965.0 | 1214.3 | 1261.6 | 47.3 | 1327.2 | 1260.1 | 12.3 | 2.80 |


## Raw files

- `raw/<scenario>-runNN.json`: the 36 reported runs. Each holds the parameters, dataset, event times, per-window statistics (all, `GET`, `SET`, toucher), gaps, memory per window, per-target toucher data, the reload check, a 50 ms timeline and every memory sample.
- `raw/calibration/`: the two runs used to choose the slow-disk delays (36 µs and 220 µs, each 12.8 times slower; the reported runs use 28 µs and 170 µs).
- `raw/diagnostics/`: an idle run with a 30 s after-window, the repeated-`BGSAVE` memory script and its output, and the timer test (a 1 ms condition-variable timeout lasts a median 15.4 ms on this machine; a 1 ms sleep lasts 1.5 ms) with its source.
- `raw/verification/`: the re-runs and the no-wait experiment.

## Reproducing

From the workspace root; about 25 minutes for everything (about 25 s per run, 55–75 s per slow-disk run). Run nothing else heavy at the same time.

```
cargo build --release -p rexyra-server -p rexyra-bench
target\release\rexyra-bench.exe run --scenario small --runs 3
target\release\rexyra-bench.exe run --scenario str1m --runs 3
target\release\rexyra-bench.exe run --scenario str10m --runs 3
target\release\rexyra-bench.exe run --scenario str100m --runs 3
target\release\rexyra-bench.exe run --scenario str1m --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario str10m --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario str100m --runs 3 --no-toucher --label no-toucher
target\release\rexyra-bench.exe run --scenario coll --runs 3
target\release\rexyra-bench.exe run --scenario slow-small --runs 3 --save-delay-us 28
target\release\rexyra-bench.exe run --scenario slow-str10m --runs 3 --save-delay-us 170
target\release\rexyra-bench.exe run --scenario idle --runs 3
target\release\rexyra-bench.exe summarize
```
