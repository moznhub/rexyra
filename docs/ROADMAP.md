# Roadmap

What Rexyra does not do yet, roughly in the order it is likely to be tackled. Nothing here is promised or dated; if you need one of these, say so in an issue, or better, offer to build it (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

## Next

- **Testing with real client libraries.** So far the server has been exercised with `redis-cli`, `redis-benchmark` and its own tests. StackExchange.Redis, redis-py, node-redis, Jedis, go-redis and redis-rs each need a smoke test, and whatever they turn up needs fixing.
- **TLS.** An optional encrypted listener.
- **ACL persistence and audit.** `aclfile`, `ACL SAVE` / `ACL LOAD`, `ACL LOG`, selectors.
- **`CONFIG REWRITE`**, so that settings and users changed at run time survive a restart.
- **Check and repair tools** for snapshot and append-only files.
- **Per-client buffer limits**, so that one client cannot push memory use past `maxmemory`.

## Commands and features not implemented

- `SWAPDB`, `CLIENT PAUSE` / `REPLY` / `TRACKING`, `LATENCY`, `DEBUG`, `MONITOR`, `WAIT`
- HyperLogLog (`PFADD` ...), geospatial commands (`GEOADD` ...), streams (`XADD` ...)
- `OBJECT HELP` and the other `HELP` subcommands

## Windows integration

- Running as a Windows Service, with logging to the Event Log
- An installer and a `winget` package
- Signed release binaries

## Known limits of the current design

- **Large collections inside transactions and scripts.** During a background save, a list, hash, set or sorted set with thousands of elements is captured a piece at a time, and a client that writes to one waits on its own. A write from inside `MULTI`/`EXEC` or a script cannot wait, so the whole collection is captured at once and every client waits (about 85 ms for a hash of a million fields).
- **Memory during a save** grows with whatever is overwritten while the save runs, until it has been written to disk.
- **A value handed to a reply carries 24 extra bytes** until it is next written.

Details and measurements are in [design/snapshot-without-fork.md](design/snapshot-without-fork.md) and [benchmarks/bgsave](benchmarks/bgsave).

## Out of scope for now

Rexyra is a single-instance server. These are not planned at present:

- Replication and Sentinel
- Cluster mode
- Pub/Sub
- Modules
- Platforms other than Windows
