# Changelog

All notable changes to Rexyra are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). From 1.0, what stays compatible within a major version is set out in the [README](https://github.com/moznhub/rexyra/blob/main/README.md#versions-and-compatibility); before 1.0 a minor version could change behaviour.

## [Unreleased]

## [1.0.0] - 2026-11-22

The first stable release. From this version on, settings, data files, command-line options and the Windows service stay compatible within 1.x (see the [README](https://github.com/moznhub/rexyra/blob/main/README.md#versions-and-compatibility)). New in it: client-side caching, `COMMAND GETKEYSANDFLAGS` with Redis's key specifications, and fixes that bring `BGSAVE`, `BGREWRITEAOF` and `SUBSCRIBE` in line with Redis. Every client library tested, StackExchange.Redis, redis-py, Jedis, Lettuce and go-redis, now passes its full suite apart from streams, HyperLogLog and geospatial commands, which Rexyra does not have.

### Added

- `COMMAND GETKEYSANDFLAGS`, which go-redis calls: the keys of a command line with how the command uses each (`RW access delete`, `OW update`, ...), as Redis 7.4 gives them, also for `SET ... GET`, `BITFIELD`, `SORT ... STORE` and a `numkeys` that is not a plain number, where Redis reads the arguments with a procedure of the command's own.
- **Key specifications** in `COMMAND INFO` for every command, the same as Redis 7.4's. The ACL key checks, `COMMAND GETKEYS` and `GETKEYSANDFLAGS` all find keys from them, and `tools/compare-command-info.py` compares `COMMAND INFO` of every command, and `GETKEYS` and `GETKEYSANDFLAGS` of 276 command lines, with a Redis server, in RESP2 and RESP3.
- **Client-side caching**: `CLIENT TRACKING` with every option (`REDIRECT`, `BCAST`, `PREFIX`, `OPTIN`, `OPTOUT`, `NOLOOP`), `CLIENT CACHING`, `CLIENT GETREDIR` and `CLIENT TRACKINGINFO`. Invalidations go out as RESP3 pushes, or as `__redis__:invalidate` messages to a RESP2 redirect target, with the same content, to the same connections and in the same order as in Redis 7.4, relative to replies and keyspace notifications; broadcast prefixes are batched once per pass of the execution loop. `tracking-table-max-keys` (configuration file, command line, `CONFIG SET`, `CONFIG REWRITE`), the `t`, `B` and `R` flags and `redir` in `CLIENT LIST`, and the `tracking_*` fields of `INFO`. Compared with Redis by a new tool, `tools/rexyra-tracking`. go-redis's and redis-py's client-side caches now work. Unused, it costs one test of a flag per write.

### Changed

- `CONFIG SET maxmemory` with a limit below the memory in use evicts at once, as in Redis, instead of at the next command.
- `HELLO`, `INFO` (`redis_version`) and scripts (`redis.REDIS_VERSION`) report Redis 7.4.0 instead of 7.2.0: redis-py enables its client-side cache only for 7.4 or later, and Rexyra already has 7.4's hash field expiry.

### Fixed

- `BGSAVE` and `BGREWRITEAOF` inside `MULTI` are no longer refused: as in Redis, they reply that the save or the rewrite is scheduled, and it starts once the transaction is over. `BGSAVE` while a rewrite of the append-only file runs is refused with Redis's message, which suggests `BGSAVE SCHEDULE`, and `BGSAVE SCHEDULE` then schedules it.
- `SUBSCRIBE`, `PSUBSCRIBE` and `SSUBSCRIBE` are refused with the `OOM` error when the memory limit is reached and nothing can be evicted, as in Redis, since a subscription takes memory. Unsubscribing still works.
- `COMMAND INFO` reports the flags Redis 7.4 reports: `movablekeys` for `SORT`, `SORT_RO`, `LMPOP`, `BLMPOP`, `ZMPOP`, `BZMPOP`, `SINTERCARD`, `ZINTERCARD`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZUNIONSTORE`, `ZINTERSTORE` and `ZDIFFSTORE`; Redis's order (`write blocking fast` for `BZPOPMIN`); none for containers such as `CONFIG` and `OBJECT`, whose subcommands carry them; `loading` and `stale` for `ECHO`, `allow_busy` for `HELLO` and `QUIT`; and no `may_replicate` on `PUBLISH`, `SPUBLISH`, `EVAL`, `EVALSHA` and `FCALL`, nor `skip_monitor` and `skip_slowlog` on `AUTH` and `HELLO`, which still stay out of the slow log. `SSUBSCRIBE`, `SUNSUBSCRIBE` and `SPUBLISH` have key positions, as in Redis, where their shard channels sit. The tips and subcommands are empty sets in RESP3, not arrays.
- `COMMAND GETKEYS` follows Redis in the remaining cases: a command without keys is reported as such before a wrong number of arguments (`ECHO`), a subcommand is looked up (`OBJECT ENCODING k`, and `Invalid command specified` for one that does not exist), `ZUNIONSTORE` names its destination first, `SORT` finds the destination of its last `STORE` and skips the arguments of `LIMIT`, `BY` and `GET`, and a key count larger than the arguments there are, or one that is no number at all, is `Invalid arguments specified for command` (or no keys for `EVAL` and `FCALL`). ACL key checks see the same keys, so the destination of `ZUNIONSTORE` is the key an error names when both it and a source are denied, and for a key count that is not valid the keys checked are those Redis checks (none for `EVAL s 3 a b`, which then fails with its own error; `a` for `EVAL s 1x a`).

## [0.4.0] - 2026-11-21

Rexyra is now tested with five client libraries (StackExchange.Redis, redis-py, Jedis, Lettuce and go-redis), and has what they turned up: expiry of single hash fields as in Redis 7.4, keyspace notifications, and `MOVE`, `SWAPDB` and `COMMAND GETKEYS`.

### Added

- **Expiry of single hash fields** (Redis 7.4): `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME` and `HPERSIST`, with Redis's replies, errors and conditions (`NX`, `XX`, `GT`, `LT`). Expired fields disappear from every hash command and are reclaimed in the background like keys; a hash whose last field expires is deleted. Field TTLs are kept by `RENAME`, `COPY`, `MOVE`, `SWAPDB`, `DUMP`/`RESTORE`, snapshots (in Redis 7.4's RDB format, which Redis reads and writes) and the append-only file. `INFO` reports `subexpiry` and `expired_subkeys`, and `OBJECT ENCODING` `listpackex`.
- `MOVE`, `SWAPDB` and `COMMAND GETKEYS`, which redis-py, Jedis and go-redis call. `SWAPDB` during a background save waits until the save has read the data, so the snapshot still has each database as it was.
- Tests with the redis-py (8.1 and 5.3), Jedis (8.0 and 7.5), Lettuce (7.9 and 6.9) and go-redis (9.23 and 9.7) client libraries, run in CI: [clients/README.md](https://github.com/moznhub/rexyra/blob/main/clients/README.md).
- **Keyspace notifications** (`notify-keyspace-events`, in the configuration file, on the command line or with `CONFIG SET`, and kept by `CONFIG REWRITE`). Every command raises the events Redis 7.4 raises, with the same names and in the same order, including `expired`, `evicted`, `keymiss` (`m`) and `new` (`n`), inside transactions, scripts and functions and when a blocked client is served. Compared with Redis by a new tool, `tools/rexyra-keyevents`. Off, the default, it costs nothing measurable. Hash fields raise `hexpire`, `hpersist` and `hexpired`, as in Redis 7.4.
- `DEBUG SET-ACTIVE-EXPIRE 0|1`, as in Redis, to stop the periodic deletion of expired keys in tests.
- `rexyra-bench ops`, the throughput and latency of `SET`, `GET` and `LPUSH` on their own.


### Changed

- `SORT ... STORE` with `BY` or `GET` is written to the append-only file as the list it stored, so that a replay gives the same list even if a key or hash field it read had passed its TTL without having been removed yet.

### Fixed

- `SCAN` deletes the expired keys it comes across, as Redis does, instead of only leaving them out.
- `GETEX` with an `EXAT` or `PXAT` time already past deletes the key at once, as Redis does, and logs a `DEL`.


## [0.3.0] - 2026-11-21

Rexyra now has Pub/Sub, and settings changed while it runs can be saved with `CONFIG REWRITE`. It was tested with the StackExchange.Redis client library and for six hours under load with every reply checked ([docs/benchmarks/soak](https://github.com/moznhub/rexyra/blob/main/docs/benchmarks/soak/README.md)).

### Added

- **Pub/Sub**: `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBLISH`, `SPUBLISH` and `PUBSUB`, in RESP2 and RESP3, with channel permissions for ACL users and Redis's default output limit for subscribers that do not read. `CLIENT LIST` shows subscriptions and output waiting to be written, and `INFO` counts channels and patterns.
- Tests with the StackExchange.Redis client library (versions 2.12 and 3.4), run in CI: [clients/README.md](https://github.com/moznhub/rexyra/blob/main/clients/README.md).
- `DEBUG PANIC`, behind the new `enable-debug-command` setting (`no` by default, as in Redis), to test what happens after an internal error.
- **`CONFIG REWRITE`**, which writes the settings changed with `CONFIG SET`, and the users made with `ACL SETUSER`, to the configuration file, so that they survive a restart. As in Redis, comments, order and lines it does not know are kept; Windows paths are written as they are, without doubled backslashes.
- **Log rotation.** When the log file reaches `logfile-max-size` (100 MB by default) it is renamed `rexyra.1.log`, the earlier ones move up a number, and a new file is started; `logfile-max-files` (5 by default) are kept and the oldest deleted. If another program holds the file so that it cannot be renamed, the log carries on in it and the server tries again later.

### Changed

- **The server does not start if its data folder (`dir`) does not exist**, or is a file, and says which folder in the log, on standard error and, for the service, in `rexyra-startup-error.txt`. Redis does the same. Before, it started anyway and every save failed. A Windows path in double quotes, where backslashes start escapes, was the usual way to get there.
- Replies to the commands before a blocking command in the same pipeline are sent while it waits, as Redis does, instead of after it.
- **Licence.** Rexyra is licensed under the Apache License, Version 2.0, instead of The Unlicense. Version 0.2.0 was released into the public domain and stays that way; later versions are under Apache-2.0.

### Fixed

- `ACL SETUSER`, and so a `user` line in the configuration file, accepts `sanitize-payload` and `skip-sanitize-payload`, as Redis does. Every `ACL LIST` line says `sanitize-payload`, so before this one could not be used as a `user` line.
- `COMMAND INFO` lists every ACL category of a command, as Redis does (`@read @string @fast` for `GET`), instead of only its group. `SORT` and `SORT_RO` are now also in `@set`, `@sortedset` and `@list`, `LASTSAVE` in `@admin`, and `EVAL_RO`, `EVALSHA_RO` and `FCALL_RO` no longer in `@read`, which `ACL CAT` and user permissions follow. All 190 commands and all 21 categories were compared with Redis 7.4.
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

[Unreleased]: https://github.com/moznhub/rexyra/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/moznhub/rexyra/releases/tag/v1.0.0
[0.4.0]: https://github.com/moznhub/rexyra/releases/tag/v0.4.0
[0.3.0]: https://github.com/moznhub/rexyra/releases/tag/v0.3.0
[0.2.0]: https://github.com/moznhub/rexyra/releases/tag/v0.2.0
[0.1.0]: https://github.com/moznhub/rexyra/releases/tag/v0.1.0
