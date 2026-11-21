# Changelog

All notable changes to Rexyra are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor version may change behaviour.

## [Unreleased]

### Added

- **Pub/Sub**: `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBLISH`, `SPUBLISH` and `PUBSUB`, in RESP2 and RESP3, with channel permissions for ACL users and Redis's default output limit for subscribers that do not read. `CLIENT LIST` shows subscriptions and output waiting to be written, and `INFO` counts channels and patterns.
- Tests with the StackExchange.Redis client library (versions 2.12 and 3.4), run in CI: [clients/README.md](https://github.com/moznhub/rexyra/blob/main/clients/README.md).
- `DEBUG PANIC`, behind the new `enable-debug-command` setting (`no` by default, as in Redis), to test what happens after an internal error.

### Changed

- Replies to the commands before a blocking command in the same pipeline are sent while it waits, as Redis does, instead of after it.
- **Licence.** Rexyra is licensed under the Apache License, Version 2.0, instead of The Unlicense. Version 0.2.0 was released into the public domain and stays that way; later versions are under Apache-2.0.

### Fixed

- **An internal error now gets the service restarted.** If the thread that runs commands failed, the server used to shut down as if asked to, with exit code 0, so Windows did not restart the service. Now an internal error on any thread is logged and the process exits with code 70, which the service manager treats as a crash and restarts from the data on disk.

## [0.2.0] - 2026-11-21

Rexyra can now be installed as a Windows service.

### Added

- **An installer** (`.msi`). Setup asks for the folder and the port, then installs and starts Rexyra as a Windows service under the `LocalService` account, with the settings, data and log in `C:\ProgramData\Rexyra`. An upgrade keeps the folder, the settings and the data; removing the program leaves the settings and the data in place.
- **A service mode**, `rexyra-server.exe --service`, for running under the Windows service manager without a wrapper. The server reports its progress while it loads and saves, and writes a snapshot before it exits when the service is stopped or Windows shuts down.
- **`logfile`**, to write the log to a file instead of standard error.
- **`--create-config <PATH>`**, which writes a starting configuration file from the other options, unless the file exists.
- `THIRD-PARTY-NOTICES.txt` in the zip and the installer, with the licence notices of the libraries built into the program.
- A guide to running Rexyra as a Windows service: [docs/WINDOWS-SERVICE.md](https://github.com/moznhub/rexyra/blob/main/docs/WINDOWS-SERVICE.md).

### Changed

- **Licence.** Rexyra is now released into the public domain under The Unlicense: free for anyone to use for anything, without conditions. Earlier versions remain available under MIT or Apache-2.0.
- The log is written without colour codes when it goes to a file or a pipe rather than a terminal, or when `NO_COLOR` is set.
- The line logged at start-up includes the version.

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

[Unreleased]: https://github.com/moznhub/rexyra/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/moznhub/rexyra/releases/tag/v0.2.0
[0.1.0]: https://github.com/moznhub/rexyra/releases/tag/v0.1.0
