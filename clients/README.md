# Client library tests

Tests that drive Rexyra through real Redis client libraries, the way an application would. Each one starts its own `rexyra-server` on a free port with an empty data folder and a password, and stops it at the end.

| Library | Folder | Versions tested in CI |
|---|---|---|
| [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) (.NET) | [dotnet/StackExchangeRedis](dotnet/StackExchangeRedis) | 2.12.14, 3.4.35 |
| [redis-py](https://github.com/redis/redis-py) (Python) | [python/redis-py](python/redis-py) | 8.1.0, 5.3.1 |
| [Jedis](https://github.com/redis/jedis) (Java) | [java](java) | 8.0.2, 7.5.3 |
| [Lettuce](https://github.com/redis/lettuce) (Java) | [java](java) | 7.9.0, 6.9.0 |
| [go-redis](https://github.com/redis/go-redis) (Go) | [go/go-redis](go/go-redis) | 9.23.0, 9.7.3 |

Every suite also passes against Redis 7.4, which is how the tests themselves are checked. A test of something Rexyra does not have yet is marked as such (skipped, disabled or an expected failure, depending on the language) with the reason; those features are listed in [COMPATIBILITY.md](../docs/COMPATIBILITY.md#not-implemented). node-redis and redis-rs are still to come; see the [roadmap](../docs/ROADMAP.md).

Every suite takes the same settings:

- `REXYRA_SERVER`: the executable to test, instead of `target/release/rexyra-server.exe`.
- `REXYRA_ENDPOINT` (`host:port`) and `REXYRA_PASSWORD`: run against a server that is already running instead of starting one. **That server's data is written to and some databases are flushed or swapped; never point this at a server whose data matters.**

## StackExchange.Redis

Covers connecting in RESP2 and RESP3, every data type, keys and expiry, `SELECT`, transactions with conditions (`WATCH`), batches and many concurrent callers on one multiplexer, Lua scripts (`EVAL`, `LuaScript` with named parameters, `EVALSHA`), the built-in lock helpers, the server API (`INFO`, `CONFIG GET`, `CLIENT LIST`, `FLUSHDB` ...), a wrong password, reconnecting after the server drops the connection, and Pub/Sub (subscribe, publish, receive).

Needs the .NET 8 SDK and a release build of the server:

```powershell
cargo build --release -p rexyra-server
dotnet test clients/dotnet/StackExchangeRedis
```

`-p:StackExchangeRedisVersion=3.4.35` tests another library version (default 2.12.14).

## redis-py

Covers RESP2 and RESP3, each with the pure-Python parser and with hiredis; `from_url`, a blocking connection pool shared by 20 threads, a wrong or missing password and reconnecting after `CLIENT KILL`; every data type, keys and expiry, `DUMP`/`RESTORE` and `SORT`; pipelines, transactions with `WATCH` and the `transaction()` retry helper; Lua with `register_script`; the `Lock` helper; the server API; Pub/Sub with patterns, sharded channels and `run_in_thread`; and `redis.asyncio`. Details: [python/redis-py/README.md](python/redis-py/README.md).

Needs Python 3.10 or later:

```powershell
cd clients/python/redis-py
py -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest
```

To test another redis-py version, install it over the pinned one: `.venv\Scripts\python -m pip install "redis==5.3.1"`.

## Jedis and Lettuce

One Maven project for both Java libraries. Covers each library's default handshake, RESP2 and RESP3, pooled clients used from many threads, every data type, transactions with `WATCH`, pipelines of 2,000 commands, Lua, the server API, a wrong or missing password, reconnecting after `CLIENT KILL`, and Pub/Sub, including re-subscribing after the connection is dropped. For Lettuce also the async and reactive APIs and `ConnectionPoolSupport`.

Needs JDK 17 or later and Maven:

```powershell
cd clients/java
mvn -B clean test "-Dtest=Jedis*"
mvn -B clean test "-Dtest=Lettuce*"
```

`-Djedis.version=7.5.3` or `-Dlettuce.version=6.9.0.RELEASE` tests another version; keep `clean`, as Maven does not recompile when only a dependency changes.

## go-redis

Covers go-redis's default handshake (`HELLO 3`, `CLIENT SETINFO`) and `Protocol: 2`, the connection pool under 64 goroutines, every data type, `Pipelined`, `TxPipelined` and `Watch`, Lua and functions, blocking commands, the server API, reconnecting after `CLIENT KILL`, and Pub/Sub. Details: [go/go-redis/README.md](go/go-redis/README.md).

Needs Go 1.26 or later:

```powershell
cd clients/go/go-redis
go test -v ./...
```

`./test-version.ps1 v9.7.3` tests another go-redis release without changing `go.mod`.
