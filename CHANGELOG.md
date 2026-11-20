# Changelog

All notable changes to Rexyra are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor version may change behaviour.

## [Unreleased]

## [0.1.0] - 2026-11-20

The first public release: a Redis-compatible in-memory data server for Windows, written in Rust. It is a pre-release, tested with its own suite, `redis-cli` and `redis-benchmark`, and not yet used in production.

### What is in it

- **Protocol**: RESP2 and RESP3, inline commands and pipelining; 180 Redis commands.
- **Data types**: strings and bitmaps, hashes, lists, sets and sorted sets, with expiry, blocking list and sorted-set commands, and `SORT`.
- **Transactions**: `MULTI` / `EXEC` / `WATCH`.
- **Scripting**: Lua 5.1 with `EVAL` and its family, the script cache, time limits and `SCRIPT KILL`, the `cjson`, `cmsgpack`, `struct` and `bit` libraries, and Redis Functions.
- **Persistence**: RDB snapshots and an append-only file in Redis's own formats, interchangeable with Redis 7 in both directions; background saves and log rewrites that keep serving commands, without `fork`.
- **Memory limit**: `maxmemory` with the eight Redis eviction policies, measured against what the process has allocated; background freeing for `UNLINK` and `FLUSHALL ASYNC`.
- **Access control**: `requirepass`, ACL users with command, category and key-pattern rules, and protected mode.
- **Administration**: `INFO`, `CONFIG`, `CLIENT`, `SLOWLOG`, a `redis.conf`-style configuration file.

### Known limits

- A single-instance server: no replication, cluster mode, Pub/Sub, streams, modules or TLS.
- Client libraries have not been tested systematically yet.
- Users created with `ACL SETUSER` are not saved across restarts; put them in the configuration file.
- Everything known to differ from Redis is listed in [docs/COMPATIBILITY.md](https://github.com/moznhub/rexyra/blob/main/docs/COMPATIBILITY.md), and what is not done in [docs/ROADMAP.md](https://github.com/moznhub/rexyra/blob/main/docs/ROADMAP.md).

[Unreleased]: https://github.com/moznhub/rexyra/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/moznhub/rexyra/releases/tag/v0.1.0
