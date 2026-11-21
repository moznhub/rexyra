# redis-py client tests

Drives Rexyra through [redis-py](https://github.com/redis/redis-py) the way an application would. One `rexyra-server` is started for the run on a free port, with an empty data folder, no automatic saves and a password, and stopped at the end.

Covers connecting in RESP2 and RESP3 (`protocol=3`) with both the pure-Python reply parser and hiredis, `from_url`, connection pools shared by threads, every data type (strings, bitmaps, hashes, lists, sets, sorted sets), keys and expiry, `SELECT`/`db`, transactions (`pipeline(transaction=True)`, `WATCH` with the `transaction()` retry helper), plain pipelines, Lua (`eval`, `register_script` with its `EVALSHA` fallback), the `Lock` helper, the server API (`INFO`, `CONFIG`, `CLIENT ...`, `FLUSHDB`, `DBSIZE`, `scan_iter`), wrong and missing passwords, reconnecting after `CLIENT KILL`, Pub/Sub (`subscribe`, `psubscribe`, sharded, `get_message`, `run_in_thread`, resubscribing after a dropped connection), and `redis.asyncio`.

Tests marked `differs(...)` show a known difference from Redis 7.4: they are strict `xfail` on Rexyra and must pass on Redis.

Needs Python 3.10+ and a release build of the server:

```powershell
cargo build --release -p rexyra-server
cd clients/python/redis-py
py -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest
```

Settings, all optional:

- `REXYRA_SERVER`: the executable to test, instead of `target/release/rexyra-server.exe`.
- The library version: `requirements.txt` pins redis-py 8.1.0. To test another, install it over the pinned one, e.g. `.venv\Scripts\python -m pip install "redis==5.3.1"`. Without hiredis (`pip uninstall hiredis`) the hiredis variants are skipped.
- `REXYRA_ENDPOINT` (`host:port`) and `REXYRA_PASSWORD`: run against a server that is already running instead of starting one. This is how the tests themselves are checked against Redis. **That server's databases are written to, databases 5 and 8/9 are flushed or swapped, its script cache is flushed and `CLIENT KILL SKIPME yes` drops its other connections.**

```powershell
docker run -d --name rr-clients-py -p 6381:6379 redis:7.4-alpine redis-server --requirepass client-tests-password
$env:REXYRA_ENDPOINT = "127.0.0.1:6381"; $env:REXYRA_PASSWORD = "client-tests-password"
.venv\Scripts\python -m pytest
```
