# Spike: consistent snapshots without `fork()`

Status: implemented in phase 4 as designed here, with the differences listed under "As built" at the end.

## Problem

`BGSAVE` and AOF rewrite must write a **point-in-time** image of the keyspace while the server keeps serving writes. Redis gets this from `fork()`: the child sees a frozen copy-on-write view of memory for free. Windows has no `fork()`, so we have to build the frozen view ourselves.

Requirements:

1. The file reflects the keyspace exactly as it was at the instant the snapshot started.
2. Commands keep running. No pause proportional to dataset size.
3. Extra memory is proportional to what is *written during* the snapshot, not to the dataset.
4. Bounded extra latency per command, including for very large values.

## What we can rely on

One thread executes all commands and owns the keyspace (see `crates/server/src/executor.rs`). So there is no concurrent mutation to defend against: a snapshot only has to be consistent with respect to that one thread, and every decision about it can be made on that thread without locks.

## Options considered

| Option | How | Verdict |
|--------|-----|---------|
| A. Stop the world | Pause commands, serialise, resume | Rejected. Pause grows with dataset size; seconds for a few GB |
| B. Full in-memory copy, then write | Clone the keyspace on the executor, write the clone in the background | Rejected. Doubles memory; the clone itself is a long pause |
| C. Persistent (immutable) data structures | Keyspace is an HAMT; a snapshot is a pointer copy | Rejected for now. Slows every read and write all the time to make a rare operation cheap; large rewrite of the engine |
| D. OS-level copy-on-write (`VirtualProtect` + guard pages, or a section mapped twice) | Emulate what `fork()` does | Rejected. Needs a custom allocator over a mapped section, `unsafe` everywhere, page-fault handling on the hot path |
| **E. Key-level copy-on-write with a scan cursor** | Described below | **Chosen** |
| F. AOF-only persistence | No snapshots at all; RDB only on clean shutdown | Fallback if E disappoints |

## Chosen design: epoch-tagged, key-level copy-on-write

### State

- `snapshot_epoch: u64` on the keyspace, starting at 0 and incremented when a snapshot starts, plus a `snapshot_active: bool`.
- Every entry stores `epoch: u64` — the snapshot epoch in which it was last **saved or created**.
- The snapshot holds a **cursor** over the hash table's bucket order and a channel to the writer thread.

### Rules

Starting a snapshot is O(1): bump `snapshot_epoch`, reset the cursor to bucket 0, spawn the writer.

**Scanner** (runs on the executor in small time slices between commands, e.g. ≤ 200 µs or 64 buckets): for each entry in the next buckets, if `entry.epoch < snapshot_epoch`, serialise it, send the bytes to the writer and set `entry.epoch = snapshot_epoch`. Entries already tagged with the current epoch are skipped.

**Write barrier** (in the one place every mutating command obtains `&mut` to an entry): if a snapshot is running and `entry.epoch < snapshot_epoch`, serialise the entry **before** the mutation, send it, and set `entry.epoch = snapshot_epoch`. Then let the command proceed.

**Inserts** during a snapshot are created with `epoch = snapshot_epoch`, so the scanner skips them: they did not exist at the snapshot instant.

**Deletes and expirations** go through the write barrier like any other mutation: the old value is saved first if the scanner has not reached it.

**Finish**: when the cursor passes the last bucket, send the trailer (checksum, EOF), the writer flushes, `FlushFileBuffers`, and renames the temp file over the target with `ReplaceFileW`. Clear `snapshot_active`.

### Why this is correct

Each entry that existed at the snapshot instant is serialised exactly once, in its snapshot-instant state: either by the scanner (never mutated before the cursor reached it) or by the write barrier (first mutation after the snapshot started). The epoch tag makes "already saved" a single integer comparison and prevents double emission. Entries created afterwards carry the current epoch and are never emitted. RDB does not require keys in any order, so interleaving scanner output and barrier output is fine.

### Incremental rehash

The table rehashes incrementally (phase 1), so an entry can move between buckets while the cursor is mid-scan. Because "saved" is recorded **on the entry**, not derived from cursor position, a moved entry is neither missed nor duplicated as long as the scanner visits both the old and the new table. Simplest rule: **do not start a rehash step while a snapshot is running, and finish any in-progress rehash before starting one.** Redis does the equivalent (it avoids resizing while a child exists). Growth is still allowed if the load factor becomes extreme (> 5); in that case the scanner walks both tables.

### Large values

The barrier serialises the whole old value before the first write, so `LPUSH` onto a 2 GB list would stall the executor. Mitigations, in order:

1. **Scan big keys first.** At snapshot start, entries above a size threshold (say 1 MB serialised estimate) are queued and serialised by the scanner in chunks before the general scan, keeping the window in which a write can hit an unsaved large value short.
2. **Chunked serialisation with a per-collection cursor** for values above the threshold: the collection keeps a small side-log of elements changed during its own scan. More complex; only if measurements show (1) is not enough.
3. Document the worst case and expose it in `LATENCY`.

### Memory and backpressure

Extra memory = bytes serialised but not yet written to disk. The channel to the writer is bounded (e.g. 64 MB). The scanner simply stops producing when it is full. The write barrier cannot skip its work, so when the channel is full it blocks the executor briefly; this is the mechanism that keeps memory bounded when the disk is slower than the write rate. Blocked time is reported as a latency event.

Entry overhead: 8 bytes per key for `epoch`. A `u32` is enough in practice (wraps after 4 billion snapshots; reset all tags on wrap) — decide in phase 1 when the entry layout is fixed.

### AOF rewrite

The same mechanism produces the RDB preamble of a rewritten AOF. Commands executed during the rewrite are appended to an in-memory rewrite buffer and written after the preamble, exactly as Redis does.

### Lua scripts and MULTI

Both run on the executor and reach entries through the same accessor, so the barrier applies with no special handling. A snapshot can only start between commands, never in the middle of a script or transaction, which keeps the snapshot instant well defined.

## Costs when no snapshot is running

One predictable branch per mutating command (`snapshot_active`) and 4–8 bytes per key. Nothing on reads.

## Risks to retire with a prototype

| # | Question | How to measure | Abandon E if |
|---|----------|----------------|--------------|
| 1 | p99 latency during a snapshot under a write-heavy load | 10 M keys of 100 B, 50% writes, uniform and Zipfian key choice; compare p99/p999 with and without a snapshot running | p99 more than 5× the idle p99 |
| 2 | Peak extra memory | Same workload, slow disk simulated by throttling the writer | Exceeds the channel bound plus 10% |
| 3 | Total snapshot time versus `fork()`-based Valkey | Same dataset on the same machine (Valkey under WSL2) | More than 3× slower when idle |
| 4 | Large-value stall | 1 GB list, `LPUSH` during snapshot | Stall above 100 ms after mitigation 1 |

The prototype needs only a `HashMap<Bytes, (u32, Bytes)>`, the scanner, the barrier and a file writer — roughly 300 lines plus a load generator — and does not depend on any phase 1 work. It should be built before phase 1's entry layout is finalised, because the epoch field lives in that layout.

## Fallback

If the prototype fails the thresholds: ship AOF-only persistence (option F) with `appendfsync everysec`, write RDB only on `SAVE`/`SHUTDOWN` (stop-the-world is acceptable there), and implement AOF rewrite as a compaction of the log itself rather than from a keyspace snapshot. RDB import from real Redis is unaffected.

## As built

Implemented in `crates/engine/src/db.rs` (`begin_snapshot`, `snapshot_step`, the `preserve` barrier) and `crates/commands/src/persistence.rs` (`BackgroundSave`, the writer thread).

Differences from the proposal:

- **Rehashing is not paused.** The table scan uses the same reverse-binary cursor as `SCAN`, which already guarantees that an entry present throughout is visited even if the table grows, shrinks or rehashes meanwhile. Since "saved" is recorded on the entry, a second visit is harmless.
- **The epoch is a `u32` in a `Cell`**, so the scan can mark entries while holding only shared access to the table.
- **Each database has its own epoch and cursor**; the sinks of all databases feed one file, so entries of different databases can be interleaved, separated by database-selection markers.
- **The execution thread never waits for the disk, and long strings are not copied.** (Third version. The first encoded everything on the execution thread into a queue of 256 chunks and blocked when it was full. The second shared every string with the writer and, when the queue was full and no command was waiting, waited for the writer on a 1 ms timer; the load benchmark showed that Windows stretches such a timer to about 15 ms and that arriving commands did not end the wait, which cost 60–86 % of throughput with large strings or a slow disk.) Now: a string of 64 KB or more is handed to the writer as it is; strings are immutable and reference-counted, so this copies nothing and costs the same for 100 MB as for 64 KB. Everything else (shorter strings, and collections, which can change in place) is encoded on the execution thread into batches of about 64 KB, which are handed over.
- **No timers between the two threads.** When the walk has to pause for the writer, the execution thread just goes back to its commands. The writer wakes it with a message in its ordinary inbox when the queue is down to half its limit, and again when the save has finished.
- **Why short strings are copied.** Sharing a buffer for the first time makes the `bytes` library attach a 24-byte reference count to it for the rest of its life. Sharing every key and value with the writer therefore left 48 bytes per key allocated after the first save. Copying a short string costs less than that and leaves nothing; for the same reason the key of a long string is copied rather than shared.
- **Memory is bounded in bytes for the walk, not for the barrier.** The walk pauses while 16 MB of entries are waiting for the writer (counted by the memory the batches occupy), so a slow disk slows the save down and nothing else. The write barrier never waits: an entry about to be changed is queued whatever the queue holds. The extra memory a save can take is therefore 16 MB plus the old versions of what commands change while it runs, the same shape as the copy-on-write cost of `fork`, and bounded by the size of the data changed during the save. Measured: +16 to +18 MiB for small values on a normal disk; +174 MiB for small values with the writer slowed tenfold under 57,000 `SET`s a second; +580 MiB with 1,000 strings of 1 MiB being overwritten in turn; the full 1,000 MiB with 10 MiB strings and a slowed writer.
- **A slice of walking ends after 256 buckets or 500 µs**, whichever comes first, checked every 16 buckets.
- **`FLUSHALL` and `FLUSHDB` abandon a running save** instead of making it take everything first, and a new one starts once the command (or the transaction or script around it) has finished. Redis kills its child process on `FLUSHALL` for the same reason.
- **Each save writes to a temporary file of its own**, and abandoning a save tells its writer to stop, so the writer of an abandoned save cannot touch the file of the next one.
- **Large values.** Strings are solved by sharing (above). A large collection (4,096 elements or more, or about 256 KB) is not captured in one go: the walk leaves it for later and it is encoded a piece at a time, in the same slices as the rest of the save, into 256 KB pieces that reach the file together once it is complete. Any change to it has to wait for that. A client's write command is therefore checked before it runs: if one of its keys is a large collection the save has still to capture, that client alone is held back, the collection is captured next (in slices of 2 ms rather than 0.5 ms, since someone is waiting), and the command then runs. Where waiting is impossible (inside `MULTI`/`EXEC` or a script, on expiry and on eviction) the rest of the collection is encoded there and then, as before. Copying the collection for the writer to encode was tried first and measured: it took as long as encoding and used several times the memory, so it was dropped.

First measurements (one machine, release build, through Docker's network proxy, so treat them as indicative): 970,000 keys of 100 bytes, a 108 MB file. Blocking `SAVE` took about 0.95 s; `BGSAVE` about 0.8 s idle and 0.95 s under a 20-client write load. During the save the median `SET` latency went from 0.80 ms to 0.83 ms and the 99th percentile from 1.5 ms to about 4 ms (2.7 times, against the threshold of 5 times set above). Peak memory, the Zipfian workload, the comparison with a `fork`-based server and the large-value stall have not been measured.

Measurements after the revision (same caveats: one machine, release build, one run of three, read from the server's slow log): the first write during a save to a value the walk had not reached took 5–21 µs for strings of 1 MB, 10 MB and 100 MB (encoding the 100 MB string, as the first version did at that point, takes about 180 ms); about 30 ms for a list of 2,000,000 short elements and about 90 ms for a hash of 1,000,000 fields, which is the cost of encoding them. Latency under concurrent load, peak memory and behaviour with a deliberately slow disk have still not been measured on the running server; the slow-disk case is covered only by a unit test that stalls the writer.

Lifecycle, as built after the review:

- **Start-up removes the unfinished files of an interrupted save** (`temp-<pid>.rdb`, `temp-bg-<pid>.rdb.<n>`, and `temp-rewrite-<pid>.rdb.<n>` in the append-only directory), with a warning for each. Only files with exactly those names are touched. This assumes what is true anyway: one server per data directory.
- **The rename that makes a file the snapshot is followed by a flush of its directory**, where the rename is recorded, for the snapshot, the base file of a rewritten log and the log's manifest. On Windows this opens the directory for writing and calls `FlushFileBuffers` on it (through `std`, no `unsafe`); the call was checked to succeed on NTFS. What it guarantees beyond NTFS's own journalling is not something this project has tested: a power failure cannot be staged from a test. If the flush fails the rename has still happened.
- **The writer does not copy a long string to encode it**: from 64 KB up, the header is written and then the string itself. The file is byte-for-byte what it was.
- **An abandoned save's writer is not waited for.** It is told to stop, has a file of its own, and removes it. If the process exits first, the file is removed at the next start.
- **Turning the log on inside a transaction** (`CONFIG SET appendonly yes` between `MULTI` and `EXEC`) starts the snapshot in the middle of the transaction. This was suspected of applying part of the transaction twice on replay; a test shows it does not: what ran before is in the snapshot, what ran after is in the log.
- **Killing the process during a background save** is tested with the real binary: the previous snapshot is byte-identical afterwards, the next start loads it, clears the leftovers and can save again.

Measured under load (2026-11-20, `docs/benchmarks/bgsave/`): on the fixed build, throughput during a save is 1–6 % below the same traffic with no save, and p99 within a few percent of it, for 1 KB values, strings of 1, 10 and 100 MiB, and a writer slowed tenfold. A save of 1,000,000 keys of 1 KB takes 2.06 s idle (blocking `SAVE`: 2.89 s) and 3.26 s under 110,000 commands a second. A save leaves no memory allocated behind it.

Large collections (third round, `docs/benchmarks/bgsave/large-collections.md`): with hashes of 1,000,000 fields and lists of 2,000,000 elements in the keyspace, the longest pause other clients see during a save went from 85 ms to 0.7 ms, or 2 ms while a client is waiting to write to one of them; that client waits 16–19 ms per list and 93–107 ms per hash, about what every client used to wait. The save itself takes longer (0.6 → 0.9 s for that dataset), since the capture now shares the thread with the commands.

Still open: collections inside transactions and scripts still stall the server while they are captured; one unexplained 6 ms pause per save in the slowed-disk runs; and no comparison with a `fork`-based server has been made.
