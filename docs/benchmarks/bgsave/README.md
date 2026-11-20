# Background save under load

How Rexyra behaves while it writes a snapshot in the background: command latency, throughput and memory, measured with [`tools/rexyra-bench`](../../../tools/rexyra-bench) on one machine (Ryzen 9 7900X, 63 GB RAM, NVMe disk, Windows 11) in three rounds. Each round measured a build, found something, and led to a change that the next round measured.

| Round | Report | What it found |
|---|---|---|
| 1 | [report.md](report.md) | Small values fine. With large strings or a slow disk, a save cut throughput by 60–86 %: the command thread was waiting for the writer on a timer. A save also left 24 bytes allocated on every key and value. |
| 2 | [after-fix.md](after-fix.md) | Both fixed: throughput during a save 1–6 % below normal in every scenario, nothing left allocated. Remaining: each large collection stalled the server once per save (85 ms for a million-field hash). |
| 3 | [large-collections.md](large-collections.md) | Large collections captured a piece at a time: the longest pause for other clients went from 85 ms to under 1 ms, or 2 ms while a client is waiting to write to one. That client waits about as long as everyone used to. |

## Where things stand

With 50 connections sending `GET` and `SET` of 1 KB values, about 115,000 commands per second and a 99th percentile of 0.8 ms; during a background save:

| Keyspace | Throughput during a save | Longest pause for other clients |
|---|---|---|
| 1,000,000 keys of 1 KB | 6–7 % lower | under 1 ms |
| Strings of 1, 10 and 100 MiB, idle or being overwritten | 2–5 % lower | no longer than without a save |
| Slowed disk (save takes 10 times longer) | 3 % lower | one 6–7 ms pause per save, unexplained |
| Lists of 2,000,000 elements and hashes of 1,000,000 fields | 21 % lower for the 0.9 s the save takes | 0.7 ms |
| The same, with a client writing to each during the save | 34 % lower for 0.6 s | 2 ms; the writer waits 16–19 ms per list, 93–107 ms per hash |

Memory during a save: 16 MiB for what has been read ahead of the disk, plus the old versions of whatever is overwritten while the save runs, until they are written; nothing afterwards.

No comparison with Redis or any other server has been made.

## Files

- `raw/`, `raw-after-fix/`, `raw-after-collection-fix/`: one JSON file per run for each round, with every figure the reports use, a 50 ms timeline and every memory sample. Paths inside them are relative to the repository; the server and tool appear under the names they had when the runs were made (`rr-server`, `rr-bench`).
- `raw-after-fix/compare.py`: compares two rounds. `raw-after-collection-fix/collections.py`: the detailed collection analysis.
- `*/verification/`, `*/reference/`, `raw/diagnostics/`, `raw/calibration/`: re-runs made to check the rounds, machine-reference runs, and the diagnostics the reports cite.
