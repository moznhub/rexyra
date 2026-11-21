# Six-hour soak test

Six hours of steady, mixed load with background saves and append-only file rewrites going on all the time. Every reply was checked against an exact model of what it should be, and every key was read back at the end and again after a restart. The aim is to find what a short benchmark cannot: wrong replies that happen rarely, memory or handles that grow slowly, and data that does not survive a save, rewrite or restart.

## How it was run

[`tools/rexyra-soak`](../../../tools/rexyra-soak) starts the server with a fresh data folder and runs these at the same time:

- **40 client connections**, 30 sending one command at a time and 10 sending batches of 16. Each owns 1,000 keys of every kind and keeps an exact copy of what they should hold, so every reply is checked. The mix covers strings, counters, hashes, lists, sets, sorted sets, keys that expire, `MULTI`/`EXEC` and Lua scripts.
- **`BGSAVE`** every 2 minutes and **`BGREWRITEAOF`** every 5 minutes, with `appendonly yes`.
- **20 new connections a second**, each of which names itself, sends `PING` and disconnects.
- Every 30 seconds it records throughput, latency percentiles and `INFO` memory.

At the end every key is read back and compared with the model. The server is then shut down, started again on the same files, and every key is compared once more.

```powershell
cargo build --release -p rexyra-server -p rexyra-soak
target\release\rexyra-soak.exe --duration 6h --server target\release\rexyra-server.exe --port 6400 --report-secs 30 --out target\soak\6h
```

A separate script recorded the server process's handles, threads, memory and processor time every minute.

**Machine:** AMD Ryzen 9 7900X (12 cores, 24 threads), 64 GB, Windows 11. The server and the load ran on the same machine, which was also being used for builds and Docker during the run.

**Version:** Rexyra at commit `702a6c0`, release build, default settings apart from `appendonly yes`. The changes merged since then (#14 to #16) are to how the server exits after an internal error, log rotation, and `COMMAND INFO`; none is on the path a command takes.

Raw results are in [raw](raw):
- [`summary.json`](raw/summary.json): the totals
- [`report.jsonl`](raw/report.jsonl): every 30-second interval
- [`events.jsonl`](raw/events.jsonl): errors
- [`process.csv`](raw/process.csv): the per-minute process samples

## Results

| | |
|---|---|
| Duration | 6 hours (21,600 s), 21 November 2026 |
| Operations checked | 4,934,539,721, of which **0 wrong** |
| Commands executed by the server | 5,674,907,627 |
| Errors on the 40 working connections | 0 |
| Background saves | 180, all succeeded |
| Append-only file rewrites | 2,509, all succeeded (72 asked for, the rest automatic: see below) |
| Keys expired by the server | about 15.6 million |
| Final check, all 244,000 keys | **0 differences** |
| After shutdown and restart | **0 differences**. Shutdown took 0.6 s, loading the append-only file 2.8 s. |
| Server crashes, failed admin commands | none |

### Throughput and latency

| | First quarter | Second | Third | Last |
|---|---:|---:|---:|---:|
| Operations per second (median) | 236,774 | 231,359 | 224,626 | 225,633 |
| Single-command p99 (median of the 30-second intervals) | 1.07 ms | 1.11 ms | 1.14 ms | 1.12 ms |

Over the whole run the median throughput was 226,861 operations a second. Across the 30-second intervals the median p99 was 1.1 ms and the median p99.9 2.3 ms; the worst p99.9 in any interval was 6.8 ms.

Throughput settled about 5% below its starting rate within the first half and stayed there. A larger data set to save and rewrite, and the work on the same machine, are both possible causes; this run cannot tell them apart.

**Single slow commands.** In 79 of the 720 intervals at least one command took longer than 100 ms; the longest took 360 ms. These are spread over the whole run, and in the same intervals p99.9 stayed under 7 ms, so they are rare single commands rather than stalls. Builds and Docker were running on the same machine at the time, so how much of this comes from Rexyra is not known. That needs a run on a machine doing nothing else.

### Memory, handles and threads

| | Second quarter | Last quarter | Change |
|---|---:|---:|---:|
| Memory used by the data (`used_memory`) | 333.7 MiB | 333.8 MiB | 0.0% |
| Memory of the process (`used_memory_rss`) | 993.2 MiB | 964.2 MiB | −2.9% |

Per-minute samples of the process, hour by hour:

| Time | 11:46 | 12:46 | 13:46 | 14:46 | 15:46 | 16:45 |
|---|---:|---:|---:|---:|---:|---:|
| Working set (MB) | 933 | 972 | 997 | 959 | 964 | 987 |
| Handles | 222 | 222 | 221 | 221 | 221 | 221 |
| Threads | 28 | 28 | 27 | 27 | 27 | 27 |

Neither memory nor handles grew. The working set reached its highest point, 1,002 MB, at 13:41, and was lower after that. The process stays about three times the size of the data. That gap is the room taken by the snapshots and rewrites running all the time, and memory the allocator keeps for reuse. It did not grow over the six hours. The server used about 4.8 processor cores on average.

### Append-only file rewrites

The test asked for a rewrite every 5 minutes, 72 in all. The server did 2,509, one about every 8.6 seconds. The rest were automatic. At about 250,000 commands a second the append-only file doubles from its last rewritten size within seconds, and the default `auto-aof-rewrite-percentage 100` then starts a rewrite. Redis does the same with these settings. Every rewrite succeeded, and the restart check loaded from the result.

### Why the tool printed FAIL

The tool fails a run on any error, including on the connections it opens and closes 20 times a second. Of those 429,229 connections, 4 failed with Windows error 10048, *Only one usage of each socket address is normally permitted*. Windows gives this error to the client when it tries to connect and the local port it chose is still in use. The connection never reached the server, and the server logged nothing. The other 429,225 connections, and all 40 working connections, had no errors. So this does not count against the server.

## Conclusion

Over six hours and nearly 5 billion checked operations:
- every reply was correct;
- every key survived 180 snapshots, 2,509 rewrites and a restart;
- memory, handles and threads stayed level.
