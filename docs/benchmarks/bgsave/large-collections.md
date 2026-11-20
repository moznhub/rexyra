# BGSAVE under load: large collections

Third round, measured 2026-11-20 on the same machine with the same tool and method as the [first round](report.md), against a build that changes how a background save handles large lists, hashes, sets and sorted sets. The comparison is with the [second round](after-fix.md), whose raw files are in `raw-after-fix/`; this round's are in `raw-after-collection-fix/`.

## What changed in the server

Until this round a collection was captured in one go, so each large one stalled the whole server once per save: about 85 ms for a hash of 1,000,000 fields and 14 ms for a list of 2,000,000 elements, whether a client wrote to it or the save's walk simply reached it.

1. **A large collection is captured a piece at a time.** A list, hash, set or sorted set with 4,096 elements or more (or about 256 KB or more) is encoded in the same half-millisecond slices of background work as the rest of the save, into pieces of 256 KB.
2. **A client that writes to one waits on its own.** A write to a large collection the save has not finished capturing holds back that client only; the collection is captured next, in slices of 2 ms, and the write then runs. Other clients keep being served.
3. **Where a command cannot wait, nothing changed.** Inside `MULTI`/`EXEC` or a script, and when a large collection expires or is evicted, it is still captured in one go.

## Summary

- **The stall is gone.** With nothing writing to the collections, the longest pause seen by other clients during a save went from 85 ms to 0.7 ms. With a client writing to each of them, from 87 ms to 2 ms.
- **The writer waits about as long as everyone used to.** Its first write to each collection during a save took 16–19 ms for a list (was 14 ms, for all clients) and 93–107 ms for a hash (was 87 ms, for all clients).
- **While a writer is waiting, other clients are slowed, not stopped.** For the 220–250 ms per save that the writer spent waiting in this test, the others ran at about 23,000 commands per second, a fifth of normal, with no request slower than 2.8 ms. Before, they were stopped.
- **Saves with large collections take longer**, because the capture now shares the thread with the commands instead of pushing them aside: 0.55 → 0.63 s with the writer, 0.61 → 0.92 s without.
- **Nothing else changed** beyond run-to-run spread and the machine's own variation (below).
- All 36 runs, and the two reference runs, ended with a good snapshot that reloaded to exactly the key count and list lengths at the moment of `BGSAVE`. Contents were checked separately (last section).

## The machine during this round

The blocking `SAVE` in the idle scenario runs code that has not changed in any round, so it serves as a measure of the machine. It took 2.885 s (median) in the second round. In this round: 2.980 s just before the runs, 2.995 s as the median of the round's own idle runs, 2.900 s just after. So the machine was between 0.5 % and 4 % slower than in the second round for most of this one, and absolute figures across the two rounds are off by about that much. Comparisons below are within a run (during the save against the baseline of the same run) wherever possible.

## Collections, before → after (medians of 3)

| | with a client writing to each collection | nothing writing to them |
|---|---|---|
| Longest gap with no completed request, ms | 86.99 → 1.99 | 85.45 → 0.68 |
| Slowest request, ms | 87.57 → 3.72 | 86.80 → 4.08 |
| 50 ms buckets in the save with a request over 10 ms | 2 of 10 → 0 of 12 | 4 of 11 → 0 of 17 |
| Throughput during the save against baseline | −38.4 % → −34.0 % | −36.2 % → −21.1 % |
| p50 during the save, ms | 0.424 → 0.455 | 0.428 → 0.571 |
| p99 during the save, ms | 0.880 → 2.267 | 0.849 → 0.924 |
| Save duration, s | 0.553 → 0.629 | 0.606 → 0.918 |
| `used_memory` peak − before, MiB | 34.2 → 42.7 | 25.4 → 23.0 |

The writer's first command to each collection after `BGSAVE`, per run (ms):

| run | list | list | hash | hash | total |
|---|---|---|---|---|---|
| before, 1 | 14.14 | 13.94 | 87.74 | 86.17 | 202 |
| before, 2 | 14.22 | 14.72 | 86.80 | 86.10 | 202 |
| before, 3 | 14.57 | 14.52 | 87.36 | 86.78 | 203 |
| after, 1 | 15.98 | 18.60 | 95.91 | 92.95 | 223 |
| after, 2 | 17.10 | 18.96 | 100.20 | 97.17 | 233 |
| after, 3 | 17.84 | 18.79 | 107.08 | 100.54 | 244 |

Before, those milliseconds were a stall of the whole server; after, they are the wait of the one client. Every later command of the writer during the save was faster than its first.

Other clients while the writer waits, from the 50 ms timeline: in the buckets mostly covered by a wait, 23,250–25,190 commands per second on average and never below 23,000 (78–80 % below baseline; before, about 340, effectively stopped), with a slowest request of 2.5–2.8 ms. Throughput stays depressed for 250–400 ms per save, the 223–244 ms of waits plus the buckets at their edges. Outside those stretches the save costs 5.6–6.6 % of throughput.

With nothing writing to the collections, throughput stays between 70,000 and 95,000 commands per second for the whole save; the lowest 50 ms bucket is 69,840. The 2 ms slices are used only while a client is waiting: without one, p99 is 0.92–1.02 ms against 2.26–2.29 ms with one.

## The other scenarios

Throughput during the save against the baseline of the same runs, second round → this round (medians): small values −5.9 % → −6.6 %; 1 MiB strings −2.6 % → −4.0 % (−4.5 % → −3.8 % without the toucher); 10 MiB strings −3.9 % → −4.9 % (−3.3 % → −3.1 %); 100 MiB strings −3.8 % → −3.5 % (−2.5 % → −1.8 %); slowed disk, small values −1.3 % → −2.9 %; slowed disk, 10 MiB strings −3.0 % → −3.4 %. Save durations and memory peaks are unchanged to within a few percent, and the idle `BGSAVE` takes 2.07 s (2.06 s).

None of this is a change beyond the spread of the runs, with one possible exception: the slowed-disk run with small values is about two points worse in two rounds running (−2.6 to −3.6 % per run against −0.3 to −1.7 %).

## What got worse, and what is not explained

- **The writing client's wait**: lists +2 to +5 ms and hashes +6 to +20 ms compared with the stall they replace.
- **p99 during a save with a writer waiting**: 0.88 → 2.27 ms, the 2 ms slices.
- **p50 during a save with large collections and no writer**: 0.43 → 0.57 ms.
- **Save duration with large collections**: +14 % with a writer, +51 % without. The second is more than sharing the thread with commands would suggest, and longer than on an intermediate build (0.78 s) that differed only in encoding each collection into one growing buffer instead of 256 KB pieces. Not explained.
- **Peak memory with a writer** is 8 MiB higher than before (+34 → +43 MiB): a captured collection waits in memory for the disk while the next one is being captured.
- **The one 6 ms gap per slowed-disk save of small values**, found in the second round, is still there (6.2–7.5 ms at 25.2–25.5 s into a 36 s save) and still unexplained.
- **Two baselines dropped more than the machine reference accounts for** (100 MiB strings without toucher −4.9 %, collections without toucher −6.2 %). Baseline windows contain no save, so the changed code is not involved; unexplained.

## How these results were checked

- **The tables above were recomputed from the raw files with a second, independent script**, and every file was tested for internal consistency (throughput against count and window length, percentile order, save duration against event times, peak memory against the samples, timeline against request counts, status and reload flags): no discrepancy in 36 files. Every file records the same server binary.
- **An earlier attempt at this round was discarded.** It ran while the machine was busy with other work and its baselines were 1–8 % low; the same binary measured afterwards was back to normal. That is why this round carries a machine reference before and after.
- **That attempt found two things that were then fixed before this round**: a 2.3–2.7 ms pause with large collections and no writer, and a peak memory twice the expected. Both came from encoding a collection into a single buffer that grew by copying itself; encoding into 256 KB pieces removed the pause (0.63–0.71 ms here) and brought the peak down (+66 → +43 MiB).
- **Contents, not only counts.** The tool's reload check compares key counts and list lengths. Separately, a snapshot taken by a background save of this dataset under load was reloaded and every field of both 1,000,000-field hashes and every element of both 2,000,000-element lists was compared, in order: all correct. A snapshot with collections of 30,000 elements of each type, including writes made during the save, was also loaded into Redis 7.4 and compared element by element: identical.
- **The behaviour itself is covered by tests**: a piecewise capture reads back identically for every collection type; a held-back write runs after its collection has been captured and the snapshot has the state from before it; transactions and scripts still produce a correct snapshot; clients held back are released in order when a save is abandoned.

## Results per scenario

Every run and the median, generated by `rexyra-bench summarize --raw docs\benchmarks\bgsave\raw-after-collection-fix`. The two machine-reference runs are in `raw-after-collection-fix/reference/`.

### small

1,000,000 strings of 1,000 bytes; foreground GET/SET on them.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 550813 | 110115 | 0.435 | 0.697 | 0.873 | 1.202 | 3.40 | 0.91 |
| 1 | during | 3.24 | 343367 | 105878 | 0.453 | 0.720 | 0.893 | 1.193 | 4.44 | 0.59 |
| 1 | after | 3.00 | 329299 | 109688 | 0.435 | 0.701 | 0.884 | 1.216 | 3.31 | 0.43 |
| 2 | baseline | 5.00 | 569865 | 113920 | 0.422 | 0.665 | 0.824 | 1.127 | 3.22 | 0.90 |
| 2 | during | 3.26 | 351449 | 107691 | 0.448 | 0.695 | 0.849 | 1.127 | 4.28 | 0.78 |
| 2 | after | 3.00 | 343306 | 114410 | 0.422 | 0.654 | 0.800 | 1.052 | 3.24 | 0.46 |
| 3 | baseline | 5.00 | 576682 | 115276 | 0.417 | 0.656 | 0.818 | 1.108 | 3.24 | 0.87 |
| 3 | during | 3.29 | 349509 | 106383 | 0.452 | 0.708 | 0.873 | 1.164 | 4.61 | 0.84 |
| 3 | after | 3.00 | 337426 | 112404 | 0.428 | 0.677 | 0.839 | 1.118 | 3.26 | 0.51 |
| **median** | baseline | 5.00 | 569865 | 113920 | 0.422 | 0.665 | 0.824 | 1.127 | 3.24 | 0.90 |
| **median** | during | 3.26 | 349509 | 106383 | 0.452 | 0.708 | 0.873 | 1.164 | 4.44 | 0.78 |
| **median** | after | 3.00 | 337426 | 112404 | 0.428 | 0.677 | 0.839 | 1.118 | 3.26 | 0.46 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.243 | 965.0 | 1220.7 | 1220.7 | 1238.4 | 17.7 | 17.7 | 1531.8 | 1532.5 | 1223.6 | 11.0 |
| 2 | 3.264 | 965.0 | 1220.9 | 1220.9 | 1238.6 | 17.7 | 17.7 | 1543.7 | 1543.9 | 1223.8 | 11.1 |
| 3 | 3.285 | 965.0 | 1220.9 | 1220.9 | 1238.6 | 17.7 | 17.7 | 1517.6 | 1518.9 | 1223.7 | 11.3 |
| **median** | 3.264 | 965.0 | 1220.9 | 1220.9 | 1238.6 | 17.7 | 17.7 | 1531.8 | 1532.5 | 1223.7 | 11.1 |

### str1m

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 480398 | 96020 | 0.493 | 0.812 | 0.967 | 1.236 | 3.71 | 2.35 |
| 1 | during | 3.43 | 309771 | 90267 | 0.527 | 0.860 | 1.033 | 1.477 | 4.26 | 1.14 |
| 1 | after | 3.00 | 280381 | 93404 | 0.507 | 0.838 | 1.000 | 1.331 | 3.54 | 1.04 |
| 2 | baseline | 5.00 | 490172 | 97986 | 0.484 | 0.792 | 0.948 | 1.238 | 4.41 | 0.85 |
| 2 | during | 3.44 | 322102 | 93568 | 0.510 | 0.826 | 1.009 | 1.384 | 3.98 | 1.01 |
| 2 | after | 3.00 | 294924 | 98233 | 0.484 | 0.786 | 0.929 | 1.167 | 3.72 | 0.74 |
| 3 | baseline | 5.00 | 487849 | 97499 | 0.486 | 0.797 | 0.952 | 1.214 | 3.40 | 2.12 |
| 3 | during | 3.41 | 325705 | 95481 | 0.499 | 0.805 | 0.963 | 1.284 | 4.04 | 0.94 |
| 3 | after | 3.00 | 291990 | 97285 | 0.487 | 0.806 | 0.964 | 1.242 | 3.90 | 0.88 |
| **median** | baseline | 5.00 | 487849 | 97499 | 0.486 | 0.797 | 0.952 | 1.236 | 3.71 | 2.12 |
| **median** | during | 3.43 | 322102 | 93568 | 0.510 | 0.826 | 1.009 | 1.384 | 4.04 | 1.01 |
| **median** | after | 3.00 | 291990 | 97285 | 0.487 | 0.806 | 0.964 | 1.242 | 3.72 | 0.88 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.432 | 1096.4 | 1126.4 | 1126.4 | 1696.2 | 569.8 | 569.7 | 1167.9 | 1737.9 | 1126.4 | 11.7 |
| 2 | 3.442 | 1096.4 | 1126.4 | 1126.4 | 1714.6 | 588.2 | 588.1 | 1168.1 | 1756.4 | 1126.4 | 12.8 |
| 3 | 3.411 | 1096.4 | 1126.4 | 1126.5 | 1713.5 | 587.1 | 587.1 | 1167.7 | 1756.7 | 1126.4 | 11.3 |
| **median** | 3.432 | 1096.4 | 1126.4 | 1126.4 | 1713.5 | 587.1 | 587.1 | 1167.9 | 1756.4 | 1126.4 | 11.7 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 3749 | 1.314 | 1.870 | 3.37 |
| 1 | during | 2492 | 1.348 | 2.080 | 3.45 |
| 1 | after | 2208 | 1.334 | 1.965 | 2.67 |
| 2 | baseline | 3872 | 1.281 | 1.915 | 3.69 |
| 2 | during | 2609 | 1.294 | 2.001 | 3.20 |
| 2 | after | 2333 | 1.271 | 1.827 | 4.89 |
| 3 | baseline | 3855 | 1.284 | 1.895 | 6.33 |
| 3 | during | 2606 | 1.292 | 1.874 | 2.24 |
| 3 | after | 2261 | 1.304 | 1.979 | 2.86 |
| **median** | baseline | 3855 | 1.284 | 1.895 | 3.69 |
| **median** | during | 2606 | 1.294 | 2.001 | 3.20 |
| **median** | after | 2261 | 1.304 | 1.965 | 2.86 |

### str1m (no-toucher)

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 586584 | 117260 | 0.409 | 0.650 | 0.815 | 1.103 | 3.28 | 0.91 |
| 1 | during | 3.23 | 367879 | 113801 | 0.421 | 0.669 | 0.844 | 1.139 | 3.85 | 0.50 |
| 1 | after | 3.00 | 326871 | 108849 | 0.436 | 0.717 | 0.929 | 1.309 | 3.48 | 0.81 |
| 2 | baseline | 5.00 | 591987 | 118348 | 0.406 | 0.638 | 0.799 | 1.082 | 3.23 | 0.89 |
| 2 | during | 3.32 | 381226 | 114675 | 0.418 | 0.663 | 0.830 | 1.134 | 3.94 | 0.54 |
| 2 | after | 3.00 | 357476 | 119083 | 0.404 | 0.633 | 0.790 | 1.065 | 3.46 | 0.50 |
| 3 | baseline | 5.00 | 599312 | 119807 | 0.400 | 0.636 | 0.803 | 1.121 | 3.41 | 0.84 |
| 3 | during | 3.31 | 373942 | 113084 | 0.423 | 0.678 | 0.862 | 1.208 | 4.21 | 0.64 |
| 3 | after | 3.00 | 355341 | 118372 | 0.404 | 0.648 | 0.826 | 1.174 | 3.10 | 1.05 |
| **median** | baseline | 5.00 | 591987 | 118348 | 0.406 | 0.638 | 0.803 | 1.103 | 3.28 | 0.89 |
| **median** | during | 3.31 | 373942 | 113801 | 0.421 | 0.669 | 0.844 | 1.139 | 3.94 | 0.54 |
| **median** | after | 3.00 | 355341 | 118372 | 0.404 | 0.648 | 0.826 | 1.174 | 3.46 | 0.81 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.233 | 1096.4 | 1124.4 | 1124.4 | 1128.5 | 4.0 | 4.0 | 1165.5 | 1167.8 | 1124.4 | 11.4 |
| 2 | 3.324 | 1096.4 | 1124.4 | 1124.4 | 1129.0 | 4.6 | 4.5 | 1165.7 | 1169.3 | 1124.4 | 11.1 |
| 3 | 3.307 | 1096.4 | 1124.4 | 1124.4 | 1128.0 | 3.6 | 3.5 | 1166.7 | 1168.3 | 1124.4 | 11.3 |
| **median** | 3.307 | 1096.4 | 1124.4 | 1124.4 | 1128.5 | 4.0 | 4.0 | 1165.7 | 1168.3 | 1124.4 | 11.3 |

### str10m

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 446404 | 89135 | 0.453 | 1.829 | 2.146 | 2.518 | 3.25 | 2.44 |
| 1 | during | 3.12 | 262223 | 84024 | 0.484 | 1.809 | 2.208 | 2.696 | 4.10 | 2.34 |
| 1 | after | 3.00 | 261501 | 87124 | 0.463 | 1.873 | 2.186 | 2.480 | 2.97 | 2.09 |
| 2 | baseline | 5.01 | 443623 | 88587 | 0.454 | 1.841 | 2.155 | 2.537 | 3.17 | 2.42 |
| 2 | during | 3.05 | 257891 | 84566 | 0.477 | 1.808 | 2.208 | 3.208 | 6.10 | 5.41 |
| 2 | after | 3.00 | 267845 | 89208 | 0.452 | 1.819 | 2.138 | 2.528 | 3.45 | 2.24 |
| 3 | baseline | 5.01 | 443112 | 88522 | 0.457 | 1.822 | 2.135 | 2.478 | 3.73 | 2.17 |
| 3 | during | 3.09 | 259853 | 84209 | 0.484 | 1.807 | 2.210 | 2.643 | 6.32 | 5.25 |
| 3 | after | 3.00 | 268647 | 89484 | 0.455 | 1.803 | 2.105 | 2.410 | 3.18 | 2.27 |
| **median** | baseline | 5.01 | 443623 | 88587 | 0.454 | 1.829 | 2.146 | 2.518 | 3.25 | 2.42 |
| **median** | during | 3.09 | 259853 | 84209 | 0.484 | 1.808 | 2.208 | 2.696 | 6.10 | 5.25 |
| **median** | after | 3.00 | 267845 | 89208 | 0.455 | 1.819 | 2.138 | 2.480 | 3.18 | 2.24 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.121 | 1096.4 | 1140.2 | 1140.2 | 1850.7 | 710.5 | 710.4 | 1176.9 | 1886.3 | 1140.2 | 12.6 |
| 2 | 3.050 | 1096.4 | 1140.2 | 1140.2 | 1850.2 | 710.0 | 709.9 | 1178.9 | 1886.7 | 1140.2 | 12.5 |
| 3 | 3.086 | 1096.4 | 1140.2 | 1140.2 | 1839.8 | 699.6 | 699.6 | 1178.4 | 1876.0 | 1140.2 | 12.2 |
| **median** | 3.086 | 1096.4 | 1140.2 | 1140.2 | 1850.2 | 710.0 | 709.9 | 1178.4 | 1886.3 | 1140.2 | 12.5 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 587 | 8.559 | 10.132 | 10.88 |
| 1 | during | 365 | 8.537 | 10.347 | 11.60 |
| 1 | after | 349 | 8.626 | 10.145 | 10.21 |
| 2 | baseline | 596 | 8.371 | 10.350 | 11.66 |
| 2 | during | 372 | 8.168 | 10.445 | 11.55 |
| 2 | after | 364 | 8.287 | 9.535 | 9.77 |
| 3 | baseline | 588 | 8.526 | 10.207 | 11.46 |
| 3 | during | 363 | 8.522 | 10.371 | 11.49 |
| 3 | after | 351 | 8.540 | 9.944 | 10.65 |
| **median** | baseline | 588 | 8.526 | 10.207 | 11.46 |
| **median** | during | 365 | 8.522 | 10.371 | 11.55 |
| **median** | after | 351 | 8.540 | 9.944 | 10.21 |

### str10m (no-toucher)

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 590892 | 118125 | 0.407 | 0.641 | 0.799 | 1.086 | 3.19 | 0.85 |
| 1 | during | 3.21 | 367984 | 114501 | 0.420 | 0.661 | 0.825 | 1.098 | 4.07 | 0.65 |
| 1 | after | 3.00 | 354151 | 118039 | 0.406 | 0.644 | 0.817 | 1.127 | 3.11 | 0.98 |
| 2 | baseline | 5.00 | 584946 | 116932 | 0.411 | 0.650 | 0.814 | 1.139 | 3.51 | 0.80 |
| 2 | during | 3.23 | 370319 | 114485 | 0.420 | 0.664 | 0.830 | 1.147 | 3.88 | 0.45 |
| 2 | after | 3.00 | 359916 | 119893 | 0.402 | 0.631 | 0.788 | 1.045 | 3.28 | 0.50 |
| 3 | baseline | 5.00 | 596800 | 119303 | 0.402 | 0.637 | 0.798 | 1.096 | 3.33 | 0.81 |
| 3 | during | 3.21 | 372370 | 115844 | 0.415 | 0.652 | 0.812 | 1.096 | 3.87 | 0.37 |
| 3 | after | 3.00 | 359549 | 119791 | 0.402 | 0.631 | 0.786 | 1.053 | 3.20 | 0.58 |
| **median** | baseline | 5.00 | 590892 | 118125 | 0.407 | 0.641 | 0.799 | 1.096 | 3.33 | 0.81 |
| **median** | during | 3.21 | 370319 | 114501 | 0.420 | 0.661 | 0.825 | 1.098 | 3.88 | 0.45 |
| **median** | after | 3.00 | 359549 | 119791 | 0.402 | 0.631 | 0.788 | 1.053 | 3.20 | 0.58 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.214 | 1096.4 | 1124.2 | 1124.2 | 1131.2 | 7.0 | 7.0 | 1162.9 | 1168.4 | 1124.2 | 11.4 |
| 2 | 3.235 | 1096.4 | 1124.2 | 1124.2 | 1130.3 | 6.1 | 6.1 | 1163.6 | 1168.9 | 1124.2 | 11.5 |
| 3 | 3.214 | 1096.4 | 1124.2 | 1124.2 | 1131.2 | 7.0 | 7.0 | 1162.5 | 1167.6 | 1124.2 | 12.7 |
| **median** | 3.214 | 1096.4 | 1124.2 | 1124.2 | 1131.2 | 7.0 | 7.0 | 1162.9 | 1168.4 | 1124.2 | 11.5 |

### str100m

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 434906 | 86721 | 0.445 | 0.749 | 1.172 | 15.327 | 21.58 | 20.95 |
| 1 | during | 3.27 | 273554 | 83648 | 0.467 | 0.775 | 1.234 | 16.088 | 23.64 | 22.64 |
| 1 | after | 3.00 | 245043 | 81616 | 0.473 | 0.797 | 1.260 | 17.280 | 18.53 | 17.90 |
| 2 | baseline | 5.08 | 438969 | 86437 | 0.446 | 0.754 | 1.145 | 15.473 | 21.65 | 20.94 |
| 2 | during | 3.39 | 272032 | 80277 | 0.481 | 0.823 | 1.365 | 16.562 | 20.55 | 19.55 |
| 2 | after | 3.00 | 260744 | 86893 | 0.444 | 0.753 | 1.165 | 15.222 | 20.37 | 19.06 |
| 3 | baseline | 5.06 | 441472 | 87201 | 0.444 | 0.749 | 1.142 | 15.581 | 21.59 | 20.93 |
| 3 | during | 3.31 | 278926 | 84289 | 0.464 | 0.774 | 1.161 | 15.696 | 19.53 | 18.53 |
| 3 | after | 3.00 | 263701 | 87884 | 0.445 | 0.750 | 1.135 | 15.052 | 20.06 | 19.42 |
| **median** | baseline | 5.06 | 438969 | 86721 | 0.445 | 0.749 | 1.145 | 15.473 | 21.59 | 20.94 |
| **median** | during | 3.31 | 273554 | 83648 | 0.467 | 0.775 | 1.234 | 16.088 | 20.55 | 19.55 |
| **median** | after | 3.00 | 260744 | 86893 | 0.445 | 0.753 | 1.165 | 15.222 | 20.06 | 19.06 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.270 | 1096.4 | 1252.2 | 1252.3 | 2077.8 | 825.6 | 825.5 | 1289.7 | 2114.2 | 1252.2 | 33.4 |
| 2 | 3.389 | 1096.4 | 1252.2 | 1252.3 | 2077.7 | 825.5 | 825.5 | 1290.5 | 2113.8 | 1252.2 | 26.5 |
| 3 | 3.309 | 1096.4 | 1252.2 | 1252.3 | 2078.8 | 826.6 | 826.6 | 1289.8 | 2114.8 | 1252.3 | 29.6 |
| **median** | 3.309 | 1096.4 | 1252.2 | 1252.3 | 2077.8 | 825.6 | 825.5 | 1289.8 | 2114.2 | 1252.2 | 29.6 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 65 | 77.866 | 84.842 | 84.84 |
| 1 | during | 41 | 77.594 | 85.168 | 85.17 |
| 1 | after | 37 | 83.299 | 92.898 | 92.90 |
| 2 | baseline | 66 | 76.850 | 85.208 | 85.21 |
| 2 | during | 43 | 77.635 | 86.529 | 86.53 |
| 2 | after | 39 | 77.196 | 81.839 | 81.84 |
| 3 | baseline | 64 | 79.006 | 92.415 | 92.42 |
| 3 | during | 42 | 78.993 | 83.846 | 83.85 |
| 3 | after | 37 | 78.995 | 85.235 | 85.24 |
| **median** | baseline | 65 | 77.866 | 85.208 | 85.21 |
| **median** | during | 42 | 77.635 | 85.168 | 85.17 |
| **median** | after | 37 | 78.995 | 85.235 | 85.24 |

### str100m (no-toucher)

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 598872 | 119715 | 0.400 | 0.638 | 0.804 | 1.107 | 3.25 | 0.84 |
| 1 | during | 3.36 | 388802 | 115741 | 0.416 | 0.655 | 0.813 | 1.085 | 4.05 | 0.59 |
| 1 | after | 3.00 | 363175 | 120950 | 0.397 | 0.628 | 0.790 | 1.065 | 3.46 | 0.61 |
| 2 | baseline | 5.00 | 572258 | 114406 | 0.419 | 0.669 | 0.845 | 1.148 | 3.42 | 0.82 |
| 2 | during | 3.38 | 382777 | 113148 | 0.424 | 0.673 | 0.841 | 1.137 | 4.11 | 0.62 |
| 2 | after | 3.00 | 349761 | 116553 | 0.410 | 0.656 | 0.831 | 1.232 | 3.89 | 0.86 |
| 3 | baseline | 5.00 | 576134 | 115176 | 0.416 | 0.661 | 0.827 | 1.131 | 3.52 | 0.84 |
| 3 | during | 3.50 | 378277 | 108051 | 0.443 | 0.713 | 0.906 | 1.286 | 4.29 | 0.52 |
| 3 | after | 3.00 | 336541 | 112103 | 0.428 | 0.677 | 0.846 | 1.151 | 3.30 | 0.47 |
| **median** | baseline | 5.00 | 576134 | 115176 | 0.416 | 0.661 | 0.827 | 1.131 | 3.42 | 0.84 |
| **median** | during | 3.38 | 382777 | 113148 | 0.424 | 0.673 | 0.841 | 1.137 | 4.11 | 0.59 |
| **median** | after | 3.00 | 349761 | 116553 | 0.410 | 0.656 | 0.831 | 1.151 | 3.46 | 0.61 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.359 | 1096.4 | 1124.2 | 1124.2 | 1138.6 | 14.4 | 14.4 | 1162.7 | 1177.2 | 1124.2 | 11.0 |
| 2 | 3.383 | 1096.4 | 1124.2 | 1124.2 | 1140.4 | 16.2 | 16.2 | 1163.2 | 1176.1 | 1124.2 | 11.4 |
| 3 | 3.501 | 1096.4 | 1124.2 | 1124.2 | 1140.4 | 16.2 | 16.1 | 1163.1 | 1176.7 | 1124.2 | 11.0 |
| **median** | 3.383 | 1096.4 | 1124.2 | 1124.2 | 1140.4 | 16.2 | 16.1 | 1163.1 | 1176.7 | 1124.2 | 11.0 |

### coll

2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 584177 | 116753 | 0.410 | 0.658 | 0.835 | 1.177 | 3.37 | 0.94 |
| 1 | during | 0.63 | 50072 | 79556 | 0.454 | 2.147 | 2.267 | 2.391 | 3.72 | 1.99 |
| 1 | after | 3.00 | 343876 | 114582 | 0.416 | 0.675 | 0.859 | 1.183 | 3.41 | 0.44 |
| 2 | baseline | 5.00 | 593362 | 118593 | 0.405 | 0.642 | 0.802 | 1.078 | 3.33 | 0.88 |
| 2 | during | 0.60 | 46319 | 77045 | 0.455 | 2.151 | 2.258 | 2.388 | 4.01 | 1.98 |
| 2 | after | 3.00 | 336192 | 112020 | 0.425 | 0.691 | 0.888 | 1.297 | 3.35 | 0.48 |
| 3 | baseline | 5.00 | 560697 | 112053 | 0.426 | 0.686 | 0.871 | 1.197 | 3.47 | 0.82 |
| 3 | during | 0.66 | 49951 | 75397 | 0.473 | 2.164 | 2.287 | 2.438 | 2.77 | 2.04 |
| 3 | after | 3.00 | 335693 | 111831 | 0.428 | 0.686 | 0.878 | 1.239 | 3.23 | 0.38 |
| **median** | baseline | 5.00 | 584177 | 116753 | 0.410 | 0.658 | 0.835 | 1.177 | 3.37 | 0.88 |
| **median** | during | 0.63 | 49951 | 77045 | 0.455 | 2.151 | 2.267 | 2.391 | 3.72 | 1.99 |
| **median** | after | 3.00 | 336192 | 112020 | 0.425 | 0.686 | 0.878 | 1.239 | 3.35 | 0.44 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.629 | 158.7 | 476.9 | 477.0 | 519.6 | 42.7 | 42.7 | 601.8 | 645.8 | 476.9 | 11.7 |
| 2 | 0.601 | 158.7 | 476.9 | 477.0 | 522.2 | 45.3 | 45.2 | 603.1 | 646.2 | 476.9 | 10.8 |
| 3 | 0.663 | 158.7 | 476.9 | 477.0 | 519.1 | 42.2 | 42.2 | 602.1 | 647.6 | 476.9 | 12.2 |
| **median** | 0.629 | 158.7 | 476.9 | 477.0 | 519.6 | 42.7 | 42.7 | 602.1 | 646.2 | 476.9 | 11.7 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 1201 | 0.450 | 0.943 | 1.30 |
| 1 | during | 103 | 0.481 | 92.946 | 95.91 |
| 1 | after | 722 | 0.444 | 0.970 | 1.23 |
| 2 | baseline | 1200 | 0.430 | 0.964 | 1.23 |
| 2 | during | 89 | 0.460 | 100.204 | 100.20 |
| 2 | after | 719 | 0.461 | 0.988 | 1.61 |
| 3 | baseline | 1197 | 0.448 | 0.990 | 1.97 |
| 3 | during | 103 | 0.488 | 100.538 | 107.08 |
| 3 | after | 720 | 0.461 | 0.958 | 1.34 |
| **median** | baseline | 1200 | 0.448 | 0.964 | 1.30 |
| **median** | during | 103 | 0.481 | 100.204 | 100.20 |
| **median** | after | 720 | 0.461 | 0.970 | 1.34 |

Per collection: latency of the toucher's first command started after the BGSAVE reply, and its max during the save (ms):

| run | RPUSH list:0 first | RPUSH list:0 max | RPUSH list:1 first | RPUSH list:1 max | HSET hash:0 first | HSET hash:0 max | HSET hash:1 first | HSET hash:1 max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 15.98 | 15.98 | 18.60 | 18.60 | 95.91 | 95.91 | 92.95 | 92.95 |
| 2 | 17.10 | 17.10 | 18.96 | 18.96 | 100.20 | 100.20 | 97.17 | 97.17 |
| 3 | 17.84 | 17.84 | 18.79 | 18.79 | 107.08 | 107.08 | 100.54 | 100.54 |
| **median** | 17.10 | 17.10 | 18.79 | 18.79 | 100.20 | 100.20 | 97.17 | 97.17 |

### coll (no-toucher)

2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 569814 | 113827 | 0.421 | 0.669 | 0.837 | 1.153 | 4.15 | 3.71 |
| 1 | during | 0.92 | 81635 | 88912 | 0.571 | 0.798 | 0.915 | 1.277 | 4.38 | 0.71 |
| 1 | after | 3.00 | 340150 | 113365 | 0.423 | 0.673 | 0.841 | 1.125 | 3.57 | 0.54 |
| 2 | baseline | 5.00 | 539995 | 107913 | 0.442 | 0.716 | 0.918 | 1.337 | 3.39 | 1.23 |
| 2 | during | 1.01 | 82933 | 82519 | 0.614 | 0.853 | 1.018 | 1.464 | 4.08 | 0.68 |
| 2 | after | 3.00 | 348508 | 116140 | 0.414 | 0.654 | 0.816 | 1.083 | 3.27 | 0.52 |
| 3 | baseline | 5.00 | 563687 | 112652 | 0.425 | 0.680 | 0.855 | 1.151 | 3.60 | 0.98 |
| 3 | during | 0.88 | 78825 | 89559 | 0.571 | 0.792 | 0.924 | 1.313 | 3.94 | 0.63 |
| 3 | after | 3.00 | 343826 | 114543 | 0.418 | 0.670 | 0.846 | 1.165 | 3.57 | 0.48 |
| **median** | baseline | 5.00 | 563687 | 112652 | 0.425 | 0.680 | 0.855 | 1.153 | 3.60 | 1.23 |
| **median** | during | 0.92 | 81635 | 88912 | 0.571 | 0.798 | 0.924 | 1.313 | 4.08 | 0.68 |
| **median** | after | 3.00 | 343826 | 114543 | 0.418 | 0.670 | 0.841 | 1.125 | 3.57 | 0.52 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.918 | 158.7 | 476.9 | 476.9 | 499.9 | 23.0 | 23.0 | 603.1 | 627.9 | 476.9 | 10.9 |
| 2 | 1.005 | 158.7 | 476.9 | 477.0 | 499.0 | 22.0 | 22.0 | 602.9 | 628.3 | 476.9 | 11.1 |
| 3 | 0.880 | 158.7 | 476.9 | 476.9 | 501.6 | 24.7 | 24.7 | 602.8 | 631.7 | 476.9 | 11.1 |
| **median** | 0.918 | 158.7 | 476.9 | 476.9 | 499.9 | 23.0 | 23.0 | 602.9 | 628.3 | 476.9 | 11.1 |

### slow-small

as 'small', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 28 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 576513 | 115233 | 0.417 | 0.656 | 0.818 | 1.091 | 3.51 | 0.83 |
| 1 | during | 36.15 | 4044869 | 111889 | 0.430 | 0.674 | 0.836 | 1.180 | 7.25 | 6.65 |
| 1 | after | 3.00 | 336029 | 111976 | 0.426 | 0.689 | 0.880 | 1.320 | 3.18 | 0.71 |
| 2 | baseline | 5.00 | 569464 | 113845 | 0.423 | 0.665 | 0.824 | 1.100 | 3.30 | 0.76 |
| 2 | during | 36.32 | 3986699 | 109753 | 0.439 | 0.688 | 0.847 | 1.149 | 8.08 | 7.48 |
| 2 | after | 3.00 | 334020 | 111305 | 0.432 | 0.681 | 0.843 | 1.108 | 3.13 | 0.52 |
| 3 | baseline | 5.00 | 580123 | 115963 | 0.414 | 0.655 | 0.816 | 1.110 | 3.19 | 0.87 |
| 3 | during | 36.23 | 4093844 | 113004 | 0.427 | 0.665 | 0.817 | 1.087 | 6.89 | 6.18 |
| 3 | after | 3.00 | 347175 | 115672 | 0.417 | 0.650 | 0.796 | 1.043 | 3.20 | 0.47 |
| **median** | baseline | 5.00 | 576513 | 115233 | 0.417 | 0.656 | 0.818 | 1.100 | 3.30 | 0.83 |
| **median** | during | 36.23 | 4044869 | 111889 | 0.430 | 0.674 | 0.836 | 1.149 | 7.25 | 6.65 |
| **median** | after | 3.00 | 336029 | 111976 | 0.426 | 0.681 | 0.843 | 1.108 | 3.18 | 0.52 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 36.151 | 965.0 | 1220.9 | 1220.9 | 1387.4 | 166.5 | 166.5 | 1538.9 | 1650.9 | 1226.6 | 14.6 |
| 2 | 36.324 | 965.0 | 1220.9 | 1220.9 | 1381.6 | 160.7 | 160.7 | 1531.7 | 1643.4 | 1226.5 | 13.1 |
| 3 | 36.228 | 965.0 | 1221.0 | 1221.0 | 1388.5 | 167.6 | 167.5 | 1546.2 | 1652.8 | 1226.5 | 11.9 |
| **median** | 36.228 | 965.0 | 1220.9 | 1220.9 | 1387.4 | 166.5 | 166.5 | 1538.9 | 1650.9 | 1226.5 | 13.1 |

### slow-str10m

as 'str10m', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 170 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 442920 | 88438 | 0.458 | 1.837 | 2.160 | 2.476 | 3.55 | 2.30 |
| 1 | during | 20.57 | 1757081 | 85433 | 0.473 | 1.894 | 2.237 | 2.689 | 6.48 | 5.82 |
| 1 | after | 3.00 | 256416 | 85417 | 0.473 | 1.904 | 2.243 | 2.600 | 3.89 | 2.10 |
| 2 | baseline | 5.00 | 449851 | 89913 | 0.451 | 1.789 | 2.097 | 2.456 | 3.25 | 2.29 |
| 2 | during | 20.58 | 1802417 | 87560 | 0.465 | 1.819 | 2.163 | 2.519 | 3.37 | 2.51 |
| 2 | after | 3.00 | 259061 | 86325 | 0.470 | 1.863 | 2.187 | 2.539 | 3.36 | 2.42 |
| 3 | baseline | 5.01 | 440159 | 87898 | 0.459 | 1.819 | 2.123 | 2.465 | 3.04 | 2.33 |
| 3 | during | 20.60 | 1718282 | 83430 | 0.480 | 1.923 | 2.310 | 2.830 | 4.93 | 3.63 |
| 3 | after | 3.00 | 254632 | 84827 | 0.474 | 1.909 | 2.234 | 2.567 | 3.57 | 2.23 |
| **median** | baseline | 5.01 | 442920 | 88438 | 0.458 | 1.819 | 2.123 | 2.465 | 3.25 | 2.30 |
| **median** | during | 20.58 | 1757081 | 85433 | 0.473 | 1.894 | 2.237 | 2.689 | 4.93 | 3.63 |
| **median** | after | 3.00 | 256416 | 85417 | 0.473 | 1.904 | 2.234 | 2.567 | 3.57 | 2.23 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 20.567 | 1096.4 | 1140.2 | 1140.2 | 2121.4 | 981.2 | 981.2 | 1176.8 | 2157.9 | 1140.2 | 14.7 |
| 2 | 20.585 | 1096.4 | 1140.2 | 1140.2 | 2080.2 | 940.0 | 940.0 | 1179.8 | 2117.3 | 1140.2 | 12.5 |
| 3 | 20.596 | 1096.4 | 1140.2 | 1140.2 | 2119.2 | 979.0 | 978.9 | 1178.4 | 2155.7 | 1140.2 | 12.8 |
| **median** | 20.585 | 1096.4 | 1140.2 | 1140.2 | 2119.2 | 979.0 | 978.9 | 1178.4 | 2155.7 | 1140.2 | 12.8 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 572 | 8.796 | 10.213 | 11.21 |
| 1 | during | 2333 | 8.858 | 10.448 | 13.19 |
| 1 | after | 333 | 9.019 | 10.951 | 13.59 |
| 2 | baseline | 579 | 8.719 | 10.127 | 10.83 |
| 2 | during | 2371 | 8.741 | 10.184 | 10.92 |
| 2 | after | 339 | 8.904 | 10.422 | 10.99 |
| 3 | baseline | 597 | 8.366 | 10.071 | 13.64 |
| 3 | during | 2396 | 8.589 | 10.811 | 14.39 |
| 3 | after | 348 | 8.638 | 10.348 | 12.50 |
| **median** | baseline | 579 | 8.719 | 10.127 | 11.21 |
| **median** | during | 2371 | 8.741 | 10.448 | 13.19 |
| **median** | after | 339 | 8.904 | 10.422 | 12.50 |

### idle

the 'small' dataset; BGSAVE with no traffic, then a blocking SAVE.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

| run | BGSAVE s | BGSAVE file MiB | SAVE s | SAVE file MiB | used before MiB | peak used during MiB | peak − before MiB | RSS peak MiB | used at end of after-window MiB | max INFO gap ms | reload s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.035 | 965.0 | 2.995 | 965.0 | 1214.3 | 1229.1 | 14.8 | 1332.8 | 1214.3 | 10.7 | 3.10 |
| 2 | 2.086 | 965.0 | 3.078 | 965.0 | 1214.3 | 1230.1 | 15.8 | 1331.9 | 1214.3 | 11.0 | 2.84 |
| 3 | 2.069 | 965.0 | 2.893 | 965.0 | 1214.3 | 1230.4 | 16.1 | 1324.3 | 1214.3 | 11.9 | 2.88 |
| **median** | 2.069 | 965.0 | 2.995 | 965.0 | 1214.3 | 1230.1 | 15.8 | 1331.9 | 1214.3 | 11.0 | 2.88 |


## Reproducing

As in the [first round](report.md), with `--out docs\benchmarks\bgsave\raw-after-collection-fix` added to every `run` command, plus `coll` with `--no-toucher --label no-toucher`. Then:

```
python docs\benchmarks\bgsave\raw-after-fix\compare.py --old docs\benchmarks\bgsave\raw-after-fix --new docs\benchmarks\bgsave\raw-after-collection-fix
python docs\benchmarks\bgsave\raw-after-collection-fix\collections.py --timeline
```
