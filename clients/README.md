# Client library tests

Tests that drive Rexyra through real Redis client libraries, the way an application would. Each one starts its own `rexyra-server` on a free port with an empty data folder and a password, and stops it at the end.

| Library | Folder | Versions tested in CI |
|---|---|---|
| [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) (.NET) | [dotnet/StackExchangeRedis](dotnet/StackExchangeRedis) | 2.12.14, 3.4.35 |

More libraries (redis-py, node-redis, Jedis, go-redis, redis-rs) are planned; see the [roadmap](../docs/ROADMAP.md).

## StackExchange.Redis

Covers connecting in RESP2 and RESP3, every data type, keys and expiry, `SELECT`, transactions with conditions (`WATCH`), batches and many concurrent callers on one multiplexer, Lua scripts (`EVAL`, `LuaScript` with named parameters, `EVALSHA`), the built-in lock helpers, the server API (`INFO`, `CONFIG GET`, `CLIENT LIST`, `FLUSHDB` ...), a wrong password, and reconnecting after the server drops the connection. The Pub/Sub test is skipped until Pub/Sub is implemented.

Needs the .NET 8 SDK and a release build of the server:

```powershell
cargo build --release -p rexyra-server
dotnet test clients/dotnet/StackExchangeRedis
```

Settings, all optional:

- `REXYRA_SERVER`: the executable to test, instead of `target/release/rexyra-server.exe`.
- `-p:StackExchangeRedisVersion=3.4.35`: the library version to test (default 2.12.14).
- `REXYRA_ENDPOINT` (`host:port`) and `REXYRA_PASSWORD`: run against a server that is already running instead of starting one. This is how the tests themselves are checked against Redis. **That server's databases are written to and database 5 is flushed.**
