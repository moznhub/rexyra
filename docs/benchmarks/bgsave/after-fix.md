# BGSAVE under load, after the fixes

> The large-collection stall this round reports as remaining has since been addressed: see [large-collections.md](large-collections.md).

Second round, measured 2026-11-20 on the same machine with the same tool and method as [report.md](report.md), against a server build with the two defects that report found changed:

1. **The timed wait is gone.** When the save's queue is full the command thread no longer waits for the writer on a timer; it goes on with commands, and the writer wakes it through its message queue when the queue is down to half (and when the save finishes).
2. **Short strings are copied, not shared.** Strings under 64 KB travel to the writer as encoded batches of about 64 KB instead of being shared with it, so a save no longer leaves a reference count attached to every key and value. Strings of 64 KB and more are still shared without copying.

Raw files are in `raw-after-fix/`; `raw-after-fix/compare.py` computes the comparison below from both rounds' files. How the results were checked is in the last section.

## Summary

- **The regressions are gone.** With large strings in the keyspace or a slow disk, throughput during a save is now 1–6 % below baseline; it was 61–86 % below. p99 during a save equals baseline to within a few percent in every scenario.
- **A save leaves nothing behind.** On 200,000 keys the first `BGSAVE` adds 256 bytes in total (0.00 per key); it was 48.00 bytes per key. Peak memory during a save of small values is the documented 16 MiB; it was 38–47 MiB.
- **Saves of small values are faster**: 2.59 → 2.06 s idle, 4.30 → 3.26 s under load.
- **Peak memory is higher when commands overwrite data during a save**, because commands are no longer slowed down: +19 → +580 MiB with 1 MiB strings being overwritten, +161 → +711 MiB with 10 MiB strings, +33 → +174 MiB for small values on a slowed disk. This is the old versions of what was overwritten, queued until written: the cost the design states and accepts, bounded by the size of the data overwritten during the save. The old build avoided it only by throttling the commands.
- **Large collections still stall, with or without a writer.** The walk reaching a 1,000,000-field hash by itself stalls the server about 85 ms, the same as a first write to it; a 2,000,000-element list about 14 ms. Unchanged by these fixes.
- All 36 runs ended with a good snapshot that reloaded to exactly the key count at the moment of `BGSAVE`.

## Before → after (medians of 3; old from `raw/`, new from `raw-after-fix/`)

| scenario | window | ops/s | p50 ms | p99 ms | max ms | longest gap ms |
|---|---|---|---|---|---|---|
| small | baseline | 116,347 → 116,132 | 0.416 → 0.415 | 0.779 → 0.790 | 3.35 → 3.39 | 0.81 → 0.77 |
| small | during | 107,651 → 109,292 | 0.452 → 0.442 | 0.803 → 0.832 | 10.73 → 4.39 | 10.00 → 0.59 |
| 1 MiB strings + toucher | baseline | 98,535 → 97,877 | 0.481 → 0.483 | 0.937 → 0.935 | 3.69 → 3.46 | 0.90 → 1.31 |
| 1 MiB strings + toucher | during | 38,018 → 95,302 | 0.878 → 0.499 | 2.943 → 0.969 | 6.18 → 4.10 | 4.76 → 1.50 |
| 1 MiB strings, no toucher | baseline | 118,134 → 121,845 | 0.404 → 0.395 | 0.830 → 0.767 | 3.57 → 3.35 | 2.02 → 0.67 |
| 1 MiB strings, no toucher | during | 38,981 → 116,406 | 0.786 → 0.412 | 2.876 → 0.812 | 10.02 → 4.13 | 9.43 → 0.39 |
| 10 MiB strings + toucher | baseline | 89,262 → 90,462 | 0.455 → 0.449 | 2.100 → 2.067 | 3.19 → 3.20 | 2.37 → 2.43 |
| 10 MiB strings + toucher | during | 25,743 → 86,891 | 0.529 → 0.469 | 16.092 → 2.150 | 18.13 → 4.49 | 17.55 → 3.80 |
| 10 MiB strings, no toucher | baseline | 118,561 → 121,456 | 0.403 → 0.397 | 0.805 → 0.770 | 3.21 → 3.29 | 0.76 → 0.76 |
| 10 MiB strings, no toucher | during | 28,662 → 117,395 | 0.480 → 0.411 | 15.453 → 0.787 | 17.52 → 3.91 | 16.40 → 0.53 |
| 100 MiB strings + toucher | baseline | 86,403 → 87,148 | 0.446 → 0.441 | 1.222 → 1.139 | 20.36 → 21.23 | 19.68 → 20.57 |
| 100 MiB strings + toucher | during | 15,888 → 83,846 | 0.518 → 0.462 | 30.302 → 1.251 | 35.28 → 20.63 | 34.56 → 19.74 |
| 100 MiB strings, no toucher | baseline | 119,061 → 121,103 | 0.402 → 0.397 | 0.807 → 0.770 | 3.41 → 3.36 | 0.82 → 2.09 |
| 100 MiB strings, no toucher | during | 16,576 → 118,103 | 0.552 → 0.408 | 16.704 → 0.790 | 18.19 → 3.98 | 17.58 → 0.40 |
| collections + toucher | baseline | 117,938 → 119,785 | 0.408 → 0.402 | 0.787 → 0.784 | 3.51 → 3.47 | 0.72 → 0.75 |
| collections + toucher | during | 70,117 → 73,778 | 0.429 → 0.424 | 0.922 → 0.880 | 99.96 → 87.57 | 99.25 → 86.99 |
| collections, no toucher (new) | baseline | – → 120,083 | – → 0.401 | – → 0.775 | – → 3.47 | – → 0.88 |
| collections, no toucher (new) | during | – → 76,657 | – → 0.428 | – → 0.849 | – → 86.80 | – → 85.45 |
| slow disk, small (28 µs/key) | baseline | 114,810 → 116,620 | 0.416 → 0.414 | 0.829 → 0.789 | 3.37 → 3.50 | 0.84 → 0.70 |
| slow disk, small (28 µs/key) | during | 34,111 → 115,156 | 1.524 → 0.420 | 2.048 → 0.783 | 4.75 → 6.73 | 3.21 → 6.00 |
| slow disk, 10 MiB (170 µs/key) | baseline | 88,939 → 90,329 | 0.454 → 0.449 | 2.122 → 2.073 | 3.66 → 2.98 | 2.19 → 2.21 |
| slow disk, 10 MiB (170 µs/key) | during | 32,692 → 87,587 | 1.507 → 0.462 | 4.205 → 2.146 | 18.37 → 5.84 | 17.86 → 4.78 |

Save duration and memory (`used_memory`, MiB). "After − before" is the last sample of the 3 s after-window minus the value just before `BGSAVE`. The last column counts the 50 ms buckets wholly inside the save whose slowest request took over 10 ms.

| scenario | save s | peak − before | after − before | buckets over 10 ms |
|---|---|---|---|---|
| small | 4.30 → 3.26 | 37.8 → 17.7 | 33.8 → 2.9 | 1/85 → 0/64 |
| 1 MiB strings + toucher | 2.77 → 3.42 | **18.9 → 580.4** | 2.3 → 0.0 | 0/54 → 0/67 |
| 1 MiB strings, no toucher | 2.74 → 3.26 | 3.3 → 4.1 | 2.3 → 0.0 | 1/54 → 0/64 |
| 10 MiB strings + toucher | 2.59 → 3.03 | **160.9 → 710.6** | 2.3 → 0.0 | 38/51 → 0/60 |
| 10 MiB strings, no toucher | 2.50 → 3.21 | 3.6 → 5.9 | 2.3 → 0.0 | 42/49 → 0/63 |
| 100 MiB strings + toucher | 2.60 → 3.31 | 801.2 → 823.0 | 2.4 → 0.0 | 49/51 → 44/65 |
| 100 MiB strings, no toucher | 2.49 → 3.35 | 4.5 → 16.1 | 2.3 → 0.0 | 45/49 → 0/66 |
| collections + toucher | 0.66 → 0.55 | 64.2 → 34.2 | 2.3 → 0.0 | 4/12 → 2/10 |
| collections, no toucher | – → 0.61 | – → 25.4 | – → 0.0 | – → 4/11 |
| slow disk, small | 43.29 → 35.95 | **33.3 → 174.2** | 30.4 → 5.6 | 0/865 → 0/718 |
| slow disk, 10 MiB | 27.76 → 20.53 | 826.4 → 957.5 | 2.3 → 0.0 | 66/555 → 0/410 |
| idle | BGSAVE 2.59 → 2.06; SAVE 2.89 → 2.89 | 47.3 → 16.0 | 45.8 → 0.0 | – |

Baselines moved by −0.7 % to +3.1 % in throughput between the rounds, far less than the changes during a save.

## Findings

**Small values.** Throughput during a save is 6 % below baseline (was 7.5 %); the 10 ms stall per save is gone (longest gap 10.0 → 0.6 ms). The save is a second shorter and peaks at +17.7 MiB.

**Large strings.** Throughput during a save is 2.5–4.5 % below baseline for all three sizes, with or without the toucher (was 61–86 %). p99 with 10 MiB strings went from 16 ms to 2.15 ms with the toucher (baseline 2.07) and to 0.79 ms without. The 20 ms gaps that remain with the 100 MiB toucher are the same in baseline (20.6 ms): they are the 100 MiB `SET` itself. Saves over large strings take 0.4–0.9 s longer than before, because the command thread, now busy with three to seven times as many commands, has less idle time for the walk.

**Memory when large strings are overwritten during a save.** With the commands no longer throttled, the toucher overwrites nearly every large string before the writer gets to it, and each old version waits in memory until written. With 1 MiB strings the three runs peaked at +572 to +581 MiB against a ceiling of 1,000 MiB (every string overwritten once); the peak fits "overwritten so far, minus written so far" to about 6 % (about 970 strings overwritten and about 420 MiB written at the moment of the peak, at the measured 320–330 MiB/s). With a slowed disk and 10 MiB strings the peak reached the ceiling (+958 MiB median, +1,009 MiB in one run). Memory returned to where it was after every save.

**Slow disk.** Latency and throughput during the save are those of the baseline (p50 0.42 ms, was 1.52 ms; throughput −1.3 %, was −70 %). Small values peak at +174 MiB: old versions accumulate faster than the slowed writer takes them until most keys have been captured once, then drain. Each run has one 6 ms gap about 25 s into the 36 s save (see "Unexplained").

**Collections.** The first write to each collection after `BGSAVE`: lists 14.2 and 14.5 ms (were 16.7 and 22.8), hashes 87.4 and 86.2 ms (were 99.8 and 87.3). **Without any writer** (new), the walk reaching the collections stalls the server the same way: two gaps of 85–87 ms and two of 14–15 ms per run, wherever in the save the walk reaches them; throughput over the 0.6 s save is 36 % below baseline. So a large collection costs one stall of this size in every save, written to or not.

**Memory a save leaves.** The repeated-`BGSAVE` diagnostic on 200,000 keys: first save +256 bytes in total, second and third +0, and +0 after every key is `SET` again and saved once more (was +48, +0, +24 bytes per key). On the idle server the peak during the save is +16.0 MiB and nothing remains afterwards (was +47.3 and +45.8 MiB).

**The few MiB "left" after a save under traffic are not the save's.** `used_memory` rises about 4.5 MiB in every 5 s baseline window with no save running, in both rounds. It is the same reference-count block as before, from a different sharer: a `GET` hands the stored value to the reply without copying it, which costs 24 bytes on that value until it is next written. With equal `GET`s and `SET`s over a million keys, the share of values carrying one tends to a half (about 11.4 MiB) with a time constant of 8.7 s; that predicts +4.46 MiB over the baseline window and +1.13 MiB over the after-window of the small-value runs, against +4.4 to +4.5 and +1.1 to +1.2 measured. It is bounded at 24 bytes per key and unrelated to saving.

## Unexplained

- **One 6 ms gap per slow-disk save of small values**, at 24.8–25.5 s into a 36 s save in all four runs (three of the round and one re-run), while the queued old versions are draining (memory falling steadily, no change of slope). It is not in the load generator: the per-connection sample buffers do not fill at that moment, and the no-wait experiment build, under the same client load, did not show it. Cause not identified; a guess, untested, is the allocator returning the queue's freed memory to the operating system. The slowed 10 MiB runs have one 5 ms gap each at varying times.
- Whether the smaller collection stalls in this round (lists 14 ms against 17–23 ms, one of the two hashes 87 ms against 100 ms) are due to the server change. The differences are outside the run-to-run spread, and removing a wait of up to 15 ms after a capture would explain them, but the second hash did not change, so the fit is partial.

## How these results were checked

After the runs, independently of the tool's own summary:

- **The tables were recomputed from the raw files with a second, independent script**; every figure matched, and all 36 files passed the same internal-consistency tests as in the first round. Every file records the server binary that was measured (2,929,664 bytes), which is the fixed build; no source file is newer than it.
- **Three scenarios were run again** (`raw-after-fix/verification/`): 1 MiB strings with toucher, 98,807 → 95,692 ops/s and +576 MiB at the peak; collections without toucher, longest gap 84.9 ms; slow disk with small values, 115,003 → 115,078 ops/s, +168 MiB, and the 6 ms gap again at 25.5 s. All in line with the round's own runs.
- **The analysis was reviewed** for the 6 ms gap, the arithmetic behind the +580 MiB, the memory "left" after saves, the spread behind the medians, the collection figures and the longer saves. Two first readings were withdrawn (that a few MiB are left after a save under traffic: the same drift is in the baseline windows; and a guess about the cause of the 6 ms gap) and a third narrowed (only one of the two hashes stalls less than before).
- **Two explanations were tested rather than accepted.** That the 6 ms gap is an artefact of the load generator's sample buffers: refuted by the data (above). That the memory drift under traffic comes from `GET` replies sharing values: its prediction matches the measured drift in two different windows to within a few percent.
- **The fixes themselves** are covered by tests that fail without them: the walk pausing and being woken at half the limit, short values travelling as batches, and, with the real allocator, a save of 40,000 keys leaving less than 0.5 MB behind (with the old sharing restored it fails with +1,920,394 bytes, 48 bytes per key).

## Results per scenario

Every run and the median, generated by `rexyra-bench summarize --raw docs\benchmarks\bgsave\raw-after-fix`.

### small

1,000,000 strings of 1,000 bytes; foreground GET/SET on them.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 580894 | 116132 | 0.415 | 0.647 | 0.790 | 1.043 | 3.29 | 0.82 |
| 1 | during | 3.26 | 360115 | 110477 | 0.437 | 0.678 | 0.826 | 1.122 | 4.39 | 0.59 |
| 1 | after | 3.00 | 343560 | 114482 | 0.420 | 0.663 | 0.826 | 1.172 | 3.31 | 0.40 |
| 2 | baseline | 5.00 | 587523 | 117450 | 0.412 | 0.634 | 0.770 | 1.008 | 3.39 | 0.77 |
| 2 | during | 3.12 | 337104 | 108079 | 0.447 | 0.692 | 0.846 | 1.192 | 4.30 | 1.26 |
| 2 | after | 3.00 | 346965 | 115636 | 0.418 | 0.645 | 0.786 | 1.031 | 3.26 | 0.39 |
| 3 | baseline | 5.00 | 565613 | 113069 | 0.426 | 0.666 | 0.821 | 1.113 | 3.41 | 0.62 |
| 3 | during | 3.29 | 359202 | 109292 | 0.442 | 0.683 | 0.832 | 1.139 | 4.45 | 0.44 |
| 3 | after | 3.00 | 343926 | 114558 | 0.422 | 0.652 | 0.795 | 1.059 | 3.22 | 0.61 |
| **median** | baseline | 5.00 | 580894 | 116132 | 0.415 | 0.647 | 0.790 | 1.043 | 3.39 | 0.77 |
| **median** | during | 3.26 | 359202 | 109292 | 0.442 | 0.683 | 0.832 | 1.139 | 4.39 | 0.59 |
| **median** | after | 3.00 | 343926 | 114558 | 0.420 | 0.652 | 0.795 | 1.059 | 3.26 | 0.40 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.260 | 965.0 | 1220.9 | 1220.9 | 1238.6 | 17.7 | 17.7 | 1534.9 | 1535.0 | 1223.8 | 11.0 |
| 2 | 3.119 | 965.0 | 1221.0 | 1221.0 | 1238.6 | 17.6 | 17.6 | 1509.7 | 1510.9 | 1223.8 | 11.0 |
| 3 | 3.287 | 965.0 | 1220.8 | 1220.8 | 1238.6 | 17.7 | 17.7 | 1540.3 | 1540.5 | 1223.8 | 11.0 |
| **median** | 3.260 | 965.0 | 1220.9 | 1220.9 | 1238.6 | 17.7 | 17.7 | 1534.9 | 1535.0 | 1223.8 | 11.0 |

### str1m

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 492542 | 98444 | 0.481 | 0.788 | 0.935 | 1.263 | 3.46 | 1.37 |
| 1 | during | 3.34 | 318812 | 95396 | 0.499 | 0.810 | 0.969 | 1.366 | 4.10 | 1.39 |
| 1 | after | 3.00 | 293988 | 97926 | 0.485 | 0.788 | 0.936 | 1.268 | 3.52 | 1.25 |
| 2 | baseline | 5.00 | 488516 | 97627 | 0.486 | 0.788 | 0.931 | 1.248 | 3.41 | 1.31 |
| 2 | during | 3.42 | 322709 | 94349 | 0.504 | 0.814 | 0.971 | 1.489 | 3.98 | 1.50 |
| 2 | after | 3.00 | 292927 | 97620 | 0.486 | 0.786 | 0.927 | 1.214 | 3.50 | 2.35 |
| 3 | baseline | 5.00 | 489731 | 97877 | 0.483 | 0.796 | 0.959 | 1.375 | 3.55 | 1.16 |
| 3 | during | 3.44 | 328043 | 95302 | 0.498 | 0.813 | 0.969 | 1.403 | 4.19 | 1.78 |
| 3 | after | 3.00 | 288707 | 96178 | 0.490 | 0.815 | 0.990 | 1.555 | 3.42 | 1.18 |
| **median** | baseline | 5.00 | 489731 | 97877 | 0.483 | 0.788 | 0.935 | 1.263 | 3.46 | 1.31 |
| **median** | during | 3.42 | 322709 | 95302 | 0.499 | 0.813 | 0.969 | 1.403 | 4.10 | 1.50 |
| **median** | after | 3.00 | 292927 | 97620 | 0.486 | 0.788 | 0.936 | 1.268 | 3.50 | 1.25 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.342 | 1096.4 | 1126.4 | 1126.4 | 1706.8 | 580.4 | 580.4 | 1168.7 | 1751.0 | 1126.4 | 11.0 |
| 2 | 3.420 | 1096.4 | 1126.4 | 1126.5 | 1707.4 | 581.0 | 580.9 | 1168.4 | 1749.8 | 1126.4 | 11.8 |
| 3 | 3.442 | 1096.4 | 1126.4 | 1126.4 | 1698.6 | 572.2 | 572.2 | 1169.3 | 1741.9 | 1126.4 | 11.5 |
| **median** | 3.420 | 1096.4 | 1126.4 | 1126.4 | 1706.8 | 580.4 | 580.4 | 1168.7 | 1749.8 | 1126.4 | 11.5 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 3785 | 1.310 | 1.841 | 4.54 |
| 1 | during | 2499 | 1.318 | 1.962 | 5.33 |
| 1 | after | 2257 | 1.317 | 1.907 | 4.79 |
| 2 | baseline | 3837 | 1.284 | 1.936 | 6.22 |
| 2 | during | 2595 | 1.296 | 1.923 | 4.73 |
| 2 | after | 2318 | 1.282 | 1.813 | 6.47 |
| 3 | baseline | 3767 | 1.313 | 1.939 | 4.93 |
| 3 | during | 2544 | 1.335 | 1.922 | 6.56 |
| 3 | after | 2226 | 1.332 | 1.961 | 4.59 |
| **median** | baseline | 3785 | 1.310 | 1.936 | 4.93 |
| **median** | during | 2544 | 1.318 | 1.923 | 5.33 |
| **median** | after | 2257 | 1.317 | 1.907 | 4.79 |

### str1m (no-toucher)

1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 597786 | 119502 | 0.402 | 0.635 | 0.796 | 1.077 | 3.30 | 0.66 |
| 1 | during | 3.19 | 375805 | 117896 | 0.409 | 0.640 | 0.794 | 1.057 | 3.82 | 0.39 |
| 1 | after | 3.00 | 357122 | 118992 | 0.403 | 0.640 | 0.810 | 1.126 | 3.35 | 0.62 |
| 2 | baseline | 5.00 | 611054 | 122141 | 0.394 | 0.616 | 0.766 | 1.014 | 3.35 | 0.67 |
| 2 | during | 3.26 | 378233 | 115884 | 0.412 | 0.663 | 0.849 | 1.229 | 4.17 | 0.52 |
| 2 | after | 3.00 | 365823 | 121870 | 0.394 | 0.621 | 0.777 | 1.039 | 3.25 | 0.37 |
| 3 | baseline | 5.00 | 609560 | 121845 | 0.395 | 0.619 | 0.767 | 1.031 | 3.42 | 0.91 |
| 3 | during | 3.26 | 379264 | 116406 | 0.413 | 0.651 | 0.812 | 1.104 | 4.13 | 0.39 |
| 3 | after | 3.00 | 356022 | 118590 | 0.405 | 0.639 | 0.796 | 1.062 | 3.34 | 0.38 |
| **median** | baseline | 5.00 | 609560 | 121845 | 0.395 | 0.619 | 0.767 | 1.031 | 3.35 | 0.67 |
| **median** | during | 3.26 | 378233 | 116406 | 0.412 | 0.651 | 0.812 | 1.104 | 4.13 | 0.39 |
| **median** | after | 3.00 | 357122 | 118992 | 0.403 | 0.639 | 0.796 | 1.062 | 3.34 | 0.38 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.188 | 1096.4 | 1124.4 | 1124.4 | 1128.4 | 4.0 | 3.9 | 1166.8 | 1168.5 | 1124.4 | 10.9 |
| 2 | 3.264 | 1096.4 | 1124.4 | 1124.4 | 1128.6 | 4.2 | 4.2 | 1167.8 | 1169.1 | 1124.4 | 11.3 |
| 3 | 3.258 | 1096.4 | 1124.4 | 1124.4 | 1128.5 | 4.1 | 4.1 | 1165.9 | 1169.1 | 1124.4 | 11.0 |
| **median** | 3.258 | 1096.4 | 1124.4 | 1124.4 | 1128.5 | 4.1 | 4.1 | 1166.8 | 1169.1 | 1124.4 | 11.0 |

### str10m

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 452611 | 90462 | 0.448 | 1.786 | 2.067 | 2.375 | 3.20 | 2.44 |
| 1 | during | 3.02 | 266718 | 88381 | 0.462 | 1.731 | 2.083 | 2.395 | 5.81 | 5.08 |
| 1 | after | 3.00 | 267093 | 88973 | 0.455 | 1.790 | 2.101 | 2.501 | 3.26 | 2.32 |
| 2 | baseline | 5.01 | 453415 | 90588 | 0.449 | 1.774 | 2.062 | 2.345 | 3.05 | 2.43 |
| 2 | during | 3.07 | 264147 | 85972 | 0.474 | 1.766 | 2.150 | 2.538 | 4.49 | 3.80 |
| 2 | after | 3.00 | 267328 | 89105 | 0.455 | 1.792 | 2.115 | 2.460 | 3.05 | 2.30 |
| 3 | baseline | 5.00 | 446549 | 89267 | 0.453 | 1.810 | 2.108 | 2.480 | 3.54 | 2.13 |
| 3 | during | 3.03 | 262946 | 86891 | 0.469 | 1.779 | 2.161 | 2.488 | 3.40 | 2.46 |
| 3 | after | 3.00 | 264543 | 88076 | 0.458 | 1.850 | 2.132 | 2.419 | 2.89 | 2.07 |
| **median** | baseline | 5.00 | 452611 | 90462 | 0.449 | 1.786 | 2.067 | 2.375 | 3.20 | 2.43 |
| **median** | during | 3.03 | 264147 | 86891 | 0.469 | 1.766 | 2.150 | 2.488 | 4.49 | 3.80 |
| **median** | after | 3.00 | 267093 | 88973 | 0.455 | 1.792 | 2.115 | 2.460 | 3.05 | 2.30 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.018 | 1096.4 | 1140.2 | 1140.2 | 1860.1 | 719.9 | 719.8 | 1179.8 | 1897.3 | 1140.2 | 14.7 |
| 2 | 3.072 | 1096.4 | 1140.2 | 1140.2 | 1850.3 | 710.1 | 710.1 | 1180.0 | 1887.6 | 1140.2 | 14.0 |
| 3 | 3.026 | 1096.4 | 1140.2 | 1140.2 | 1850.8 | 710.6 | 710.6 | 1178.4 | 1889.0 | 1140.2 | 12.6 |
| **median** | 3.026 | 1096.4 | 1140.2 | 1140.2 | 1850.8 | 710.6 | 710.6 | 1179.8 | 1889.0 | 1140.2 | 14.0 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 608 | 8.239 | 9.642 | 11.97 |
| 1 | during | 374 | 8.093 | 9.994 | 11.77 |
| 1 | after | 363 | 8.267 | 9.622 | 9.84 |
| 2 | baseline | 604 | 8.358 | 9.548 | 10.82 |
| 2 | during | 371 | 8.275 | 9.799 | 12.08 |
| 2 | after | 359 | 8.400 | 9.655 | 11.45 |
| 3 | baseline | 602 | 8.295 | 9.871 | 13.74 |
| 3 | during | 365 | 8.279 | 9.581 | 9.61 |
| 3 | after | 362 | 8.296 | 9.294 | 9.62 |
| **median** | baseline | 604 | 8.295 | 9.642 | 11.97 |
| **median** | during | 371 | 8.275 | 9.799 | 11.77 |
| **median** | after | 362 | 8.296 | 9.622 | 9.84 |

### str10m (no-toucher)

100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 601576 | 120253 | 0.400 | 0.628 | 0.779 | 1.059 | 3.26 | 0.69 |
| 1 | during | 3.21 | 377031 | 117395 | 0.411 | 0.639 | 0.787 | 1.034 | 3.77 | 0.43 |
| 1 | after | 3.00 | 365799 | 121921 | 0.396 | 0.614 | 0.752 | 0.989 | 3.15 | 0.31 |
| 2 | baseline | 5.00 | 607519 | 121456 | 0.397 | 0.620 | 0.770 | 1.038 | 3.29 | 0.76 |
| 2 | during | 3.10 | 369568 | 119095 | 0.405 | 0.631 | 0.783 | 1.055 | 3.91 | 0.53 |
| 2 | after | 3.00 | 361303 | 120361 | 0.399 | 0.629 | 0.785 | 1.078 | 3.38 | 0.39 |
| 3 | baseline | 5.00 | 610531 | 122039 | 0.396 | 0.612 | 0.751 | 0.986 | 3.59 | 0.80 |
| 3 | during | 3.23 | 374890 | 116243 | 0.414 | 0.647 | 0.796 | 1.100 | 4.33 | 0.85 |
| 3 | after | 3.00 | 352989 | 117609 | 0.410 | 0.638 | 0.789 | 1.056 | 3.20 | 0.31 |
| **median** | baseline | 5.00 | 607519 | 121456 | 0.397 | 0.620 | 0.770 | 1.038 | 3.29 | 0.76 |
| **median** | during | 3.21 | 374890 | 117395 | 0.411 | 0.639 | 0.787 | 1.055 | 3.91 | 0.53 |
| **median** | after | 3.00 | 361303 | 120361 | 0.399 | 0.629 | 0.785 | 1.056 | 3.20 | 0.31 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.212 | 1096.4 | 1124.2 | 1124.2 | 1130.1 | 5.9 | 5.9 | 1162.6 | 1169.0 | 1124.2 | 11.0 |
| 2 | 3.103 | 1096.4 | 1124.2 | 1124.2 | 1129.2 | 5.1 | 5.0 | 1163.2 | 1166.4 | 1124.2 | 10.8 |
| 3 | 3.225 | 1096.4 | 1124.2 | 1124.2 | 1130.3 | 6.1 | 6.1 | 1164.2 | 1169.6 | 1124.2 | 11.5 |
| **median** | 3.212 | 1096.4 | 1124.2 | 1124.2 | 1130.1 | 5.9 | 5.9 | 1163.2 | 1169.0 | 1124.2 | 11.0 |

### str100m

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.02 | 437157 | 87148 | 0.443 | 0.732 | 1.139 | 15.057 | 19.77 | 19.07 |
| 1 | during | 3.26 | 277913 | 85289 | 0.457 | 0.753 | 1.251 | 15.623 | 20.63 | 19.74 |
| 1 | after | 3.00 | 260545 | 86812 | 0.447 | 0.744 | 1.221 | 15.394 | 18.11 | 17.39 |
| 2 | baseline | 5.08 | 449016 | 88454 | 0.439 | 0.720 | 1.072 | 15.047 | 23.24 | 22.62 |
| 2 | during | 3.31 | 277666 | 83846 | 0.462 | 0.771 | 1.281 | 16.294 | 21.47 | 20.32 |
| 2 | after | 3.00 | 261718 | 87175 | 0.444 | 0.733 | 1.161 | 14.793 | 22.45 | 21.28 |
| 3 | baseline | 5.04 | 438261 | 86931 | 0.441 | 0.728 | 1.152 | 16.302 | 21.23 | 20.57 |
| 3 | during | 3.32 | 277331 | 83445 | 0.466 | 0.770 | 1.231 | 16.084 | 17.71 | 16.81 |
| 3 | after | 3.00 | 259718 | 86532 | 0.445 | 0.732 | 1.104 | 15.266 | 16.78 | 16.12 |
| **median** | baseline | 5.04 | 438261 | 87148 | 0.441 | 0.728 | 1.139 | 15.057 | 21.23 | 20.57 |
| **median** | during | 3.31 | 277666 | 83846 | 0.462 | 0.770 | 1.251 | 16.084 | 20.63 | 19.74 |
| **median** | after | 3.00 | 260545 | 86812 | 0.445 | 0.733 | 1.161 | 15.266 | 18.11 | 17.39 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.258 | 1096.4 | 1252.2 | 1252.3 | 2075.2 | 823.0 | 823.0 | 1290.7 | 2112.2 | 1252.3 | 25.9 |
| 2 | 3.312 | 1096.4 | 1252.2 | 1252.3 | 2073.6 | 821.4 | 821.4 | 1290.4 | 2112.0 | 1252.2 | 28.0 |
| 3 | 3.324 | 1096.4 | 1252.2 | 1252.3 | 2078.9 | 826.7 | 826.7 | 1290.7 | 2115.7 | 1252.2 | 25.9 |
| **median** | 3.312 | 1096.4 | 1252.2 | 1252.3 | 2075.2 | 823.0 | 823.0 | 1290.7 | 2112.2 | 1252.2 | 25.9 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 67 | 75.850 | 82.011 | 82.01 |
| 1 | during | 43 | 76.179 | 83.502 | 83.50 |
| 1 | after | 39 | 76.457 | 84.745 | 84.75 |
| 2 | baseline | 67 | 76.186 | 80.665 | 80.67 |
| 2 | during | 43 | 76.470 | 87.285 | 87.28 |
| 2 | after | 40 | 75.945 | 77.989 | 77.99 |
| 3 | baseline | 68 | 73.952 | 80.781 | 80.78 |
| 3 | during | 44 | 74.382 | 81.555 | 81.56 |
| 3 | after | 41 | 73.813 | 78.294 | 78.29 |
| **median** | baseline | 67 | 75.850 | 80.781 | 80.78 |
| **median** | during | 43 | 76.179 | 83.502 | 83.50 |
| **median** | after | 40 | 75.945 | 78.294 | 78.29 |

### str100m (no-toucher)

10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 606049 | 121103 | 0.397 | 0.625 | 0.781 | 1.052 | 3.29 | 2.56 |
| 1 | during | 3.33 | 393065 | 118103 | 0.408 | 0.638 | 0.790 | 1.086 | 3.89 | 0.38 |
| 1 | after | 3.00 | 362024 | 120652 | 0.398 | 0.631 | 0.797 | 1.100 | 3.71 | 0.34 |
| 2 | baseline | 5.00 | 604265 | 120794 | 0.399 | 0.623 | 0.770 | 1.047 | 3.47 | 0.77 |
| 2 | during | 3.35 | 399919 | 119330 | 0.405 | 0.626 | 0.766 | 1.023 | 3.98 | 0.72 |
| 2 | after | 3.00 | 363118 | 120956 | 0.398 | 0.621 | 0.770 | 1.035 | 3.73 | 0.31 |
| 3 | baseline | 5.00 | 611425 | 122190 | 0.395 | 0.612 | 0.755 | 1.004 | 3.36 | 2.09 |
| 3 | during | 3.37 | 393132 | 116540 | 0.412 | 0.650 | 0.816 | 1.127 | 4.22 | 0.40 |
| 3 | after | 3.00 | 352447 | 117398 | 0.409 | 0.643 | 0.809 | 1.164 | 3.49 | 0.96 |
| **median** | baseline | 5.00 | 606049 | 121103 | 0.397 | 0.623 | 0.770 | 1.047 | 3.36 | 2.09 |
| **median** | during | 3.35 | 393132 | 118103 | 0.408 | 0.638 | 0.790 | 1.086 | 3.98 | 0.40 |
| **median** | after | 3.00 | 362024 | 120652 | 0.398 | 0.631 | 0.797 | 1.100 | 3.71 | 0.34 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.328 | 1096.4 | 1124.2 | 1124.2 | 1138.6 | 14.4 | 14.4 | 1162.4 | 1176.8 | 1124.2 | 11.9 |
| 2 | 3.351 | 1096.4 | 1124.2 | 1124.2 | 1140.3 | 16.1 | 16.0 | 1162.2 | 1178.8 | 1124.2 | 10.9 |
| 3 | 3.373 | 1096.4 | 1124.2 | 1124.2 | 1140.3 | 16.2 | 16.1 | 1163.9 | 1176.9 | 1124.2 | 11.8 |
| **median** | 3.351 | 1096.4 | 1124.2 | 1124.2 | 1140.3 | 16.1 | 16.0 | 1162.4 | 1176.9 | 1124.2 | 11.8 |

### coll

2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 597799 | 119473 | 0.402 | 0.637 | 0.801 | 1.097 | 3.47 | 0.48 |
| 1 | during | 0.54 | 38332 | 70773 | 0.433 | 0.674 | 0.896 | 87.554 | 87.84 | 87.27 |
| 1 | after | 3.00 | 363478 | 121062 | 0.399 | 0.620 | 0.767 | 1.011 | 3.74 | 0.38 |
| 2 | baseline | 5.00 | 601071 | 120130 | 0.401 | 0.630 | 0.784 | 1.054 | 3.66 | 0.75 |
| 2 | during | 0.55 | 40829 | 73778 | 0.420 | 0.658 | 0.880 | 86.805 | 87.06 | 86.55 |
| 2 | after | 3.00 | 363946 | 121240 | 0.398 | 0.619 | 0.762 | 1.008 | 3.25 | 0.53 |
| 3 | baseline | 5.00 | 599365 | 119785 | 0.402 | 0.630 | 0.781 | 1.064 | 3.12 | 0.88 |
| 3 | during | 0.59 | 43980 | 75069 | 0.424 | 0.662 | 0.844 | 87.244 | 87.57 | 86.99 |
| 3 | after | 3.00 | 364500 | 121397 | 0.397 | 0.619 | 0.768 | 1.052 | 3.15 | 0.46 |
| **median** | baseline | 5.00 | 599365 | 119785 | 0.402 | 0.630 | 0.784 | 1.064 | 3.47 | 0.75 |
| **median** | during | 0.55 | 40829 | 73778 | 0.424 | 0.662 | 0.880 | 87.244 | 87.57 | 86.99 |
| **median** | after | 3.00 | 363946 | 121240 | 0.398 | 0.619 | 0.767 | 1.011 | 3.25 | 0.46 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.542 | 158.7 | 476.9 | 477.0 | 511.1 | 34.2 | 34.1 | 603.7 | 637.6 | 476.9 | 87.6 |
| 2 | 0.553 | 158.7 | 476.9 | 477.0 | 511.1 | 34.2 | 34.1 | 603.6 | 637.5 | 476.9 | 86.9 |
| 3 | 0.586 | 158.7 | 476.9 | 477.0 | 511.7 | 34.7 | 34.7 | 603.6 | 636.6 | 476.9 | 87.3 |
| **median** | 0.553 | 158.7 | 476.9 | 477.0 | 511.1 | 34.2 | 34.1 | 603.6 | 637.5 | 476.9 | 87.3 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 1268 | 0.416 | 0.847 | 1.28 |
| 1 | during | 85 | 0.457 | 87.742 | 87.74 |
| 1 | after | 763 | 0.420 | 0.858 | 1.20 |
| 2 | baseline | 1264 | 0.415 | 0.809 | 1.02 |
| 2 | during | 92 | 0.469 | 86.800 | 86.80 |
| 2 | after | 760 | 0.405 | 0.791 | 1.30 |
| 3 | baseline | 1261 | 0.418 | 0.818 | 1.34 |
| 3 | during | 99 | 0.435 | 87.359 | 87.36 |
| 3 | after | 760 | 0.411 | 0.831 | 1.43 |
| **median** | baseline | 1264 | 0.416 | 0.818 | 1.28 |
| **median** | during | 92 | 0.457 | 87.359 | 87.36 |
| **median** | after | 760 | 0.411 | 0.831 | 1.30 |

Per collection: latency of the toucher's first command started after the BGSAVE reply, and its max during the save (ms):

| run | RPUSH list:0 first | RPUSH list:0 max | RPUSH list:1 first | RPUSH list:1 max | HSET hash:0 first | HSET hash:0 max | HSET hash:1 first | HSET hash:1 max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14.14 | 14.14 | 13.94 | 13.94 | 87.74 | 87.74 | 86.17 | 86.17 |
| 2 | 14.22 | 14.22 | 14.72 | 14.72 | 86.80 | 86.80 | 86.10 | 86.10 |
| 3 | 14.57 | 14.57 | 14.52 | 14.52 | 87.36 | 87.36 | 86.78 | 86.78 |
| **median** | 14.22 | 14.22 | 14.52 | 14.52 | 87.36 | 87.36 | 86.17 | 86.17 |

### coll (no-toucher)

2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 600776 | 120083 | 0.401 | 0.627 | 0.775 | 1.038 | 3.47 | 0.88 |
| 1 | during | 0.62 | 48603 | 77978 | 0.424 | 0.660 | 0.833 | 85.656 | 86.90 | 85.45 |
| 1 | after | 3.00 | 364369 | 121356 | 0.398 | 0.616 | 0.755 | 1.018 | 3.46 | 0.29 |
| 2 | baseline | 5.00 | 605079 | 120938 | 0.399 | 0.619 | 0.760 | 0.999 | 3.28 | 0.77 |
| 2 | during | 0.61 | 46462 | 76657 | 0.428 | 0.663 | 0.855 | 85.282 | 85.84 | 85.09 |
| 2 | after | 3.00 | 362896 | 120931 | 0.399 | 0.622 | 0.772 | 1.050 | 3.21 | 0.51 |
| 3 | baseline | 5.00 | 598380 | 119588 | 0.403 | 0.629 | 0.780 | 1.084 | 4.07 | 1.13 |
| 3 | during | 0.59 | 44079 | 74857 | 0.430 | 0.669 | 0.849 | 86.345 | 86.80 | 86.16 |
| 3 | after | 3.00 | 359514 | 119744 | 0.402 | 0.631 | 0.782 | 1.037 | 3.25 | 0.42 |
| **median** | baseline | 5.00 | 600776 | 120083 | 0.401 | 0.627 | 0.775 | 1.038 | 3.47 | 0.88 |
| **median** | during | 0.61 | 46462 | 76657 | 0.428 | 0.663 | 0.849 | 85.656 | 86.80 | 85.45 |
| **median** | after | 3.00 | 362896 | 120931 | 0.399 | 0.622 | 0.772 | 1.037 | 3.25 | 0.42 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.623 | 158.7 | 476.9 | 476.9 | 502.4 | 25.5 | 25.4 | 603.1 | 637.0 | 476.9 | 93.6 |
| 2 | 0.606 | 158.7 | 476.9 | 477.0 | 502.3 | 25.4 | 25.4 | 602.4 | 636.1 | 476.9 | 94.7 |
| 3 | 0.589 | 158.7 | 476.9 | 477.0 | 501.1 | 24.2 | 24.2 | 602.4 | 635.7 | 476.9 | 93.5 |
| **median** | 0.606 | 158.7 | 476.9 | 477.0 | 502.3 | 25.4 | 25.4 | 602.4 | 636.1 | 476.9 | 93.6 |

### slow-small

as 'small', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 28 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.00 | 584836 | 116915 | 0.412 | 0.642 | 0.793 | 1.110 | 4.00 | 0.69 |
| 1 | during | 35.90 | 4147331 | 115528 | 0.419 | 0.645 | 0.781 | 1.027 | 6.73 | 6.07 |
| 1 | after | 3.00 | 350748 | 116816 | 0.414 | 0.639 | 0.782 | 1.063 | 3.06 | 0.72 |
| 2 | baseline | 5.00 | 578010 | 115520 | 0.418 | 0.647 | 0.788 | 1.035 | 3.30 | 0.70 |
| 2 | during | 36.07 | 4154112 | 115156 | 0.420 | 0.647 | 0.783 | 1.031 | 6.49 | 5.81 |
| 2 | after | 3.00 | 348537 | 116068 | 0.417 | 0.642 | 0.780 | 1.033 | 3.33 | 0.46 |
| 3 | baseline | 5.00 | 583377 | 116620 | 0.414 | 0.644 | 0.789 | 1.053 | 3.50 | 0.90 |
| 3 | during | 35.95 | 4122144 | 114660 | 0.421 | 0.652 | 0.792 | 1.047 | 6.99 | 6.00 |
| 3 | after | 3.00 | 349194 | 116298 | 0.414 | 0.647 | 0.792 | 1.064 | 3.36 | 0.32 |
| **median** | baseline | 5.00 | 583377 | 116620 | 0.414 | 0.644 | 0.789 | 1.053 | 3.50 | 0.70 |
| **median** | during | 35.95 | 4147331 | 115156 | 0.420 | 0.647 | 0.783 | 1.031 | 6.73 | 6.00 |
| **median** | after | 3.00 | 349194 | 116298 | 0.414 | 0.642 | 0.782 | 1.063 | 3.33 | 0.46 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 35.899 | 965.0 | 1221.0 | 1221.0 | 1395.1 | 174.2 | 174.2 | 1530.8 | 1658.7 | 1226.6 | 13.1 |
| 2 | 36.074 | 965.0 | 1220.9 | 1220.9 | 1391.1 | 170.1 | 170.2 | 1537.7 | 1654.0 | 1226.6 | 12.1 |
| 3 | 35.951 | 965.0 | 1220.9 | 1220.9 | 1397.0 | 176.0 | 176.0 | 1541.5 | 1662.0 | 1226.5 | 15.2 |
| **median** | 35.951 | 965.0 | 1220.9 | 1220.9 | 1395.1 | 174.2 | 174.2 | 1537.7 | 1658.7 | 1226.6 | 13.1 |

### slow-str10m

as 'str10m', with rdb-key-save-delay set (slow disk stand-in).
rdb-key-save-delay: 170 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

Foreground latency (ms), all GET+SET together:

| run | window | secs | requests | ops/s | p50 | p95 | p99 | p99.9 | max | longest gap |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | baseline | 5.01 | 447729 | 89438 | 0.453 | 1.810 | 2.108 | 2.457 | 3.39 | 2.34 |
| 1 | during | 20.57 | 1801514 | 87587 | 0.463 | 1.838 | 2.147 | 2.469 | 5.84 | 5.12 |
| 1 | after | 3.00 | 265909 | 88613 | 0.458 | 1.809 | 2.113 | 2.426 | 3.04 | 2.24 |
| 2 | baseline | 5.01 | 452388 | 90329 | 0.449 | 1.797 | 2.073 | 2.345 | 2.98 | 2.21 |
| 2 | during | 20.52 | 1794685 | 87443 | 0.462 | 1.832 | 2.146 | 2.511 | 5.53 | 4.78 |
| 2 | after | 3.00 | 266399 | 88743 | 0.455 | 1.845 | 2.146 | 2.452 | 3.33 | 2.57 |
| 3 | baseline | 5.00 | 458462 | 91609 | 0.444 | 1.767 | 2.040 | 2.293 | 2.78 | 2.17 |
| 3 | during | 20.53 | 1808346 | 88099 | 0.460 | 1.813 | 2.143 | 2.505 | 6.01 | 4.78 |
| 3 | after | 3.00 | 255741 | 85204 | 0.471 | 1.912 | 2.228 | 2.717 | 3.55 | 2.75 |
| **median** | baseline | 5.01 | 452388 | 90329 | 0.449 | 1.797 | 2.073 | 2.345 | 2.98 | 2.21 |
| **median** | during | 20.53 | 1801514 | 87587 | 0.462 | 1.832 | 2.146 | 2.505 | 5.84 | 4.78 |
| **median** | after | 3.00 | 265909 | 88613 | 0.458 | 1.845 | 2.146 | 2.452 | 3.33 | 2.57 |

Save and memory (MiB = 2^20 bytes):

| run | save s | file MiB | used before | baseline max used | peak used during | peak − before | peak − baseline max | RSS before | RSS peak during | used after (last) | max INFO gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 20.568 | 1096.4 | 1140.2 | 1140.2 | 2097.7 | 957.5 | 957.5 | 1180.0 | 2135.1 | 1140.2 | 12.5 |
| 2 | 20.524 | 1096.4 | 1140.2 | 1140.2 | 2068.7 | 928.5 | 928.5 | 1179.0 | 2107.3 | 1140.2 | 13.0 |
| 3 | 20.526 | 1096.4 | 1140.2 | 1140.2 | 2149.2 | 1009.0 | 1009.0 | 1178.8 | 2186.2 | 1140.2 | 13.4 |
| **median** | 20.526 | 1096.4 | 1140.2 | 1140.2 | 2097.7 | 957.5 | 957.5 | 1179.0 | 2135.1 | 1140.2 | 13.0 |

Toucher latency (ms):

| run | window | ops | p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| 1 | baseline | 597 | 8.409 | 9.770 | 12.34 |
| 1 | during | 2435 | 8.475 | 9.701 | 11.05 |
| 1 | after | 358 | 8.368 | 9.796 | 10.21 |
| 2 | baseline | 615 | 8.153 | 9.386 | 10.12 |
| 2 | during | 2475 | 8.295 | 9.879 | 12.15 |
| 2 | after | 359 | 8.346 | 9.877 | 9.99 |
| 3 | baseline | 607 | 8.276 | 9.365 | 9.76 |
| 3 | during | 2452 | 8.372 | 9.894 | 11.72 |
| 3 | after | 349 | 8.614 | 10.157 | 11.08 |
| **median** | baseline | 607 | 8.276 | 9.386 | 10.12 |
| **median** | during | 2452 | 8.372 | 9.879 | 11.72 |
| **median** | after | 358 | 8.368 | 9.877 | 10.21 |

### idle

the 'small' dataset; BGSAVE with no traffic, then a blocking SAVE.
rdb-key-save-delay: 0 µs. Runs: 3. Checks per run — rdb_last_bgsave_status: ok, ok, ok; DBSIZE after reload equals DBSIZE at BGSAVE: yes, yes, yes.

| run | BGSAVE s | BGSAVE file MiB | SAVE s | SAVE file MiB | used before MiB | peak used during MiB | peak − before MiB | RSS peak MiB | used at end of after-window MiB | max INFO gap ms | reload s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.073 | 965.0 | 2.801 | 965.0 | 1214.3 | 1230.0 | 15.7 | 1329.0 | 1214.3 | 11.4 | 2.86 |
| 2 | 2.050 | 965.0 | 2.895 | 965.0 | 1214.3 | 1230.3 | 16.0 | 1329.5 | 1214.3 | 12.4 | 2.79 |
| 3 | 2.061 | 965.0 | 2.885 | 965.0 | 1214.3 | 1230.3 | 16.0 | 1314.4 | 1214.3 | 12.3 | 2.89 |
| **median** | 2.061 | 965.0 | 2.885 | 965.0 | 1214.3 | 1230.3 | 16.0 | 1329.0 | 1214.3 | 12.3 | 2.86 |


## Reproducing

As in [report.md](report.md), with `--out docs\benchmarks\bgsave\raw-after-fix` added to every `run` command, plus:

```
target\release\rexyra-bench.exe run --scenario coll --runs 3 --no-toucher --label no-toucher --out docs\benchmarks\bgsave\raw-after-fix
python docs\benchmarks\bgsave\raw-after-fix\compare.py
```
