# go-redis tests

Drives Rexyra through [go-redis](https://github.com/redis/go-redis) v9 the way an application would. `TestMain` starts its own `rexyra-server` on a free port with an empty data folder, `--save ""` and a password, and stops it at the end.

Covers the connect handshake (`HELLO 3`/`HELLO 2`, `CLIENT SETINFO`, `CLIENT MAINT_NOTIFICATIONS`) with the default options and `Protocol: 2`, a wrong or missing password, the `DB` option, `SELECT`, `MOVE`, `SWAPDB`, the connection pool under many goroutines, `Pipelined`, `TxPipelined`, `Watch` with retry, Lua (`Eval`, `EvalSha`, `EvalRO`, `redis.NewScript(...).Run` with its EVALSHA-then-EVAL fallback), Redis 7 functions, blocking commands, every data type (strings, bitmaps, hashes with field expiry, lists, sets, sorted sets), keys and expiry, the server API (`INFO`, `CONFIG GET`, `CLIENT LIST`/`INFO`/`SETNAME`, `COMMAND`, `FLUSHDB`, `DBSIZE`, the `SCAN` iterator), reconnecting after `CLIENT KILL`, Pub/Sub (`Subscribe`, `PSubscribe`, `SSubscribe`, `Channel()`, resubscribing after the connection is killed) and client-side caching. Most tests run twice: with the library default (RESP3) and with `Protocol: 2`.

Needs Go 1.26 or later and a release build of the server:

```powershell
cargo build --release -p rexyra-server
cd clients/go/go-redis
go test -v ./...
```

Settings, all optional:

- `REXYRA_SERVER`: the executable to test, instead of `target/release/rexyra-server.exe`.
- `REXYRA_ENDPOINT` (`host:port`) and `REXYRA_PASSWORD`: run against a server that is already running instead of starting one. This is how the tests themselves are checked against Redis. **That server's databases are written to, database 5 is flushed, databases 0 and 5 are swapped (and swapped back), and its Pub/Sub clients are killed.**

Tests of commands Rexyra does not have yet are skipped with `Rexyra: ...` when the tests start their own server, and still run against `REXYRA_ENDPOINT`.

## go-redis versions

`go.mod` pins v9.23.0. To test another release without changing `go.mod`:

```powershell
./test-version.ps1 v9.7.3        # extra arguments go to go test, e.g. -v -run TestPubSub
```

The script copies `go.mod`/`go.sum` to a temporary folder, runs `go get github.com/redis/go-redis/v9@<version>` and `go test` there through `-modfile`, and builds with `-tags goredis_old` for releases before v9.18 (no client-side caching API). Checked with v9.23.0, v9.7.3 and v9.5.5.
