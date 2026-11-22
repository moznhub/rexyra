# Client-side caching (`CLIENT TRACKING`)

Status: design, not yet built.

## Problem

Client libraries can keep a local copy of values they read and serve repeated reads from it. For that to be safe, the server must tell each client when a value it holds has changed. Redis does this with *tracking*: `CLIENT TRACKING ON` makes the server remember the keys a connection reads, and when one of them changes it sends that connection an *invalidation* message.

Rexyra has none of this today. The client-library tests show what that costs: go-redis's cache quietly turns itself off, and redis-py refuses to connect when a cache is configured. Applications still work, but without the cache.

What has to be built:

- `CLIENT TRACKING ON|OFF` with all of its options (`REDIRECT`, `BCAST`, `PREFIX`, `OPTIN`, `OPTOUT`, `NOLOOP`), plus `CLIENT CACHING`, `CLIENT GETREDIR` and `CLIENT TRACKINGINFO`.
- The tracking table, and its size limit (`tracking-table-max-keys`).
- The invalidation messages themselves: RESP3 pushes, and RESP2 through a redirected Pub/Sub connection.
- What other commands show about it: `CLIENT LIST`/`INFO` flags and `redir`, and the `INFO` fields.

## Requirements

1. **Same messages as Redis 7.4.** Every message Redis would send for a given sequence of commands is sent by Rexyra too, with the same content, to the same connection, and in the same order relative to replies and to other pushed messages (keyspace notifications). The behaviour was recorded from a running Redis 7.4.12 (see [Observed behaviour](#observed-behaviour)). The implementation is checked against it by a tool that runs the same script on both servers.
2. **No cost when unused.** While no connection has ever turned tracking on, a write costs one test of a flag more than it does today.
3. **Bounded cost when used.** The cost of a write grows with the number of clients told about it, not with the number of tracked keys or clients. The table cannot grow past `tracking-table-max-keys`.
4. **No lost invalidation.** A client must never keep a stale value because Rexyra failed to send a message Redis would have sent. Sending one Redis would not send is a defect too, but the lesser one: it costs a cache miss, not a wrong answer.

## What Rexyra already has

- **One thread runs every command** (`crates/server/src/executor.rs`). The tracking table needs no locks, and every decision can be made where the command runs.
- **Change detection.** The engine (`crates/engine/src/db.rs`) already records every change to a key for `WATCH`:
  - `touch` marks a definite change;
  - `touch_later`, `unchanged` and `settle_watches` handle a value taken out for writing that may not have changed.

  It covers writes, deletes, expiry found by a lookup or by the cycle, hash-field expiry, eviction and renames. Redis uses one function, `signalModifiedKey`, for both `WATCH` and tracking, and the recorded behaviour matches exactly the cases that already bump a `WATCH` version. One exception: `SWAPDB` touches watchers but sends no invalidation, and neither should Rexyra.
- **Pushed messages** (`Server::pushes`, delivered by the executor after the current command's reply). Each message is encoded in its receiver's protocol, with Pub/Sub's output limit applied, so invalidations can travel the same way as keyspace notifications.
- **Per-command bookkeeping** in `commands::run`. Keyspace notifications (`notify::settle`) and the `WATCH` settle already run there, after every command, including each command inside `EXEC` and each `redis.call` from a script, all as the calling client.
- **Key extraction** (`acl::command_keys`), the same keys `COMMAND GETKEYS` reports.

## Options considered

| | Option | Verdict |
|---|---|---|
| A | **The engine records each changed key name while tracking is in use; the commands layer turns that list into invalidations at fixed points** (described below) | **Chosen.** Reuses the change detection that `WATCH` already relies on and that matches Redis case by case. Off, it costs one branch per write. |
| B | Raise invalidations from each command handler, next to its keyspace notification | Rejected. About 200 handlers would need a second call each, easy to miss in one, and a miss is requirement 4's "stale value". The notification points do not match Redis's `signalModifiedKey` points either: an `SADD` of an existing member notifies nothing and changes nothing, but a `SORT ... STORE` to the same list notifies and changes. |
| C | Keep the tracking table in the engine, per database | Rejected. Redis's table is keyed by name only, ignoring the database (recorded: a write in db 1 invalidates a key read in db 0). A per-database table would send fewer messages than Redis, which is not a correctness problem, but it differs from Redis and gains nothing. It would also put client concepts into the engine. |
| D | Decide who receives a message in the executor, from a typed queue | Rejected. The commands layer can see every client (`ctx.clients` plus the running one), and the expiry cycle's code can be given the executor's map. Resolving there keeps `Server::pushes` a plain list of frames, and keeps redirect and broken-redirect logic in one module. |

## Chosen design

### State

**Per client**, in a registry on the server (`Server::tracking`, keyed by client id, in id order), so code without a client in hand can reach it (the expiry cycle, eviction, the size limit):

```text
ClientTracking {
    redirect: u64,            // 0: none
    bcast, optin, optout, noloop: bool,
    caching: Option<bool>,    // CLIENT CACHING YES/NO, for the next command only
    prefixes: Vec<Bytes>,     // BCAST only
    broken: bool,             // the redirect target was found gone
}
```

The registry also gives `INFO tracking_clients` and the `CLIENT LIST` flags (`t` tracking, `B` broadcast, `R` broken redirect) and `redir` field.

**The table**: `HashMap<Bytes, IdSet>`, from a key name to the ids of the clients that read it, with no database in the key. There is a running count of (key, client) pairs for `tracking_total_items`.

Entries are not removed when a client turns tracking off or disconnects. They go when their key is invalidated, as in Redis, where a client that has gone or stopped tracking is skipped. This was recorded: a key read before `OFF` and written while off produces nothing after tracking is turned back on.

**Broadcast prefixes**: `BTreeMap<Bytes, Prefix { clients: IdSet, pending: BTreeMap<Bytes, Modifier> }>`. `pending` holds the keys under that prefix changed since the last broadcast, in sorted order, as Redis sends them. Each records which client changed it, or "several", for `NOLOOP`. The empty prefix (`BCAST` without `PREFIX`) is just another entry.

**In the engine**: a flag per database (`track_changes`) and a list of changed key names. `touch` appends to the list when the flag is set. `touch_later` keeps the key aside, `unchanged` drops it, and `settle` moves it to the list, mirroring what they already do for watched keys. `clear` and `take_all` (the flushes), `swap_contents` and the snapshot code record nothing.

The flag is on for every database while the table is non-empty or any client broadcasts. When neither holds it is turned off, and the cost is back to a single branch.

### Remembering keys

At the end of `commands::run`, after the command's changes have been turned into invalidations (next section), the keys are remembered. This applies when:

- the command is read-only, according to the command table;
- the calling client tracks without `BCAST`;
- `OPTIN` and `OPTOUT` allow it: with `OPTIN`, only when `caching == Some(true)`; with `OPTOUT`, not when `caching == Some(false)`.

All of `command_keys` are remembered, whether or not the keys exist and whether or not the command failed with `WRONGTYPE`. Redis does both (recorded). A command refused before it runs (arity, ACL, OOM) remembers nothing.

The order matters: invalidate first, then remember. A read that finds its own tracked key expired gets the invalidation after its reply, and the key is tracked again (recorded).

Inside a script, each `redis.call` goes through `run`, as the calling client. So a script that writes a key and then reads it leaves it tracked, and one that reads and then writes it sends an invalidation (both recorded). `EVAL_RO`, `EVALSHA_RO` and `FCALL_RO` are marked read-only in the command table but are left out here, as in Redis: their own `KEYS` arguments are not remembered. Only the reads the script makes count (recorded for `EVAL_RO`).

**When `caching` is cleared**: after each top-level command (in `dispatch`, not in `run`), unless it was `CLIENT CACHING` itself, or the client is now in a transaction. So it survives `MULTI` and the queued commands, and `EXEC` or `DISCARD` clears it. Recorded: `CACHING YES` then `MULTI`, two `GET`s and `EXEC` tracks both keys, while `CACHING YES` then `PING` then `GET` tracks none. In a pipeline, `CACHING YES` followed by two `GET`s tracks only the first.

### Invalidating

`tracking::flush(server, keyspace, clients, current)` takes the changed keys from every database. For each key:

- **Table:** remove the key's entry. For each client id in it, queue an invalidation `[key]` for that client, except:
  - clients no longer tracking, or now broadcasting (skipped);
  - the modifying client itself when it has `NOLOOP`.
  
  Redis sends one message per key, never several keys in one message in this mode (recorded: `MSET m1 1 m2 2` gives two messages).
- **Prefixes:** for each prefix the key starts with, add it to that prefix's `pending` together with the modifying client.

The modifying client is the one running the command: the caller, for a script. It is none for the expiry cycle, eviction and the size limit. It matters only for `NOLOOP`.

`flush` runs at these points:

1. **In `notify::notify`, before an event is published.** Recorded: on a client that both tracks and is subscribed to keyspace events, the invalidation comes before the `set` notification. The engine has to settle the keys it had set aside first. A write that turns out not to change anything is reported with `unchanged` before the command notifies, as it is for `WATCH` today; the implementation must check that every caller of `unchanged` does so before any `notify`.
2. **At the end of `run`**, for writes that raise no notification.
3. **After the expiry cycle**, in the executor's tick, next to `notify::flush`.
4. **After eviction.** This needs no extra call, because eviction raises `evicted` through `notify`, which flushes first. Recorded: invalidation, then `evicted`, for each key.

**Sending** is decided by `send(target, keys or null)`, using the target's registry entry at that moment, as Redis does:

- **With a redirect:** if the redirect target no longer exists, mark the client `broken` and, if the client itself speaks RESP3, push `tracking-redir-broken <id>` to it; nothing else is sent. This repeats on every later message (recorded). Otherwise the message goes to the redirect target:
  - if it speaks RESP3, as a push `invalidate [keys]`;
  - else, if it is subscribed to anything at all, as a Pub/Sub message `message __redis__:invalidate [keys]`, whatever it subscribed to (recorded);
  - else it is dropped.
- **Without a redirect:** a RESP3 client gets the push; a RESP2 client gets nothing (recorded, also after switching to RESP2 with `HELLO 2`).

Messages are queued on `Server::pushes` as ordinary frames. The executor delivers them after the reply to the running client, and to anyone else at once. That matches Redis, which defers the running client's own invalidations until after its reply (recorded: own `SET` after `GET`: reply, then push; the same after `EXEC` and after a script).

### Broadcasting

`tracking::broadcast(server, clients)` runs once per pass of the executor loop, just before `commit`, and after the tick. That is Redis's "before sleep". Recorded: five `SET`s pipelined, or one `MSET`, or a whole `EXEC`, give one message per prefix, keys sorted.

For each prefix with pending keys, and each of its clients, it sends the sorted keys. A `NOLOOP` client doesn't get the keys only it changed. If nothing is left for a client, it gets no message. Recorded: own pipelined `SET z1`/`SET z2`, and then another client's `SET z3`/`SET z1`, gives the `NOLOOP` client one message, `[z1, z3]`.

### Flushes

`FLUSHDB` and `FLUSHALL` (`ASYNC` too) clear the whole table and send a null invalidation to every client with tracking on, in every mode, in id order, through `send` (so redirects and broken redirects apply). Recorded. `SWAPDB` sends nothing (recorded).

### The size limit

`tracking-table-max-keys`: default 1,000,000, and 0 means no limit. It has `CONFIG GET`/`SET` (range 0 to 2⁶³−1, with Redis's error message), a configuration file directive and `CONFIG REWRITE`.

In the tick, while the table holds more keys than the limit, keys are dropped and each one is invalidated as if it had changed. Each tick has a budget of keys, so a sudden `CONFIG SET tracking-table-max-keys 1` does not stall commands. Redis picks keys at random; a `HashMap`'s order serves.

### Commands

The parsing order, every error text and the rules for changing options while tracking is on are those recorded:

- `BCAST` cannot be switched on or off while tracking is on, nor `OPTIN` to `OPTOUT`.
- Plain `ON` after `ON OPTIN` drops `OPTIN`, and `ON NOLOOP` replaces the other flags.
- Prefixes accumulate, and a new prefix may not overlap any of the client's existing ones. The error message for an overlap is worded differently for two prefixes given together and for an existing one.
- `REDIRECT` must name an existing client, which may be the caller itself.
- In RESP2 subscribed mode, `CLIENT TRACKING` is refused by the existing subscribed-mode rule.

`RESET` and disconnecting turn tracking off. Turning it off removes the client from the registry and from its prefixes; table entries are left to go lazily.

`CLIENT TRACKINGINFO` returns:
- `flags`, a set, in Redis's order: `on`/`off`, `bcast`, `optin`, `optout`, `caching-yes`, `caching-no`, `noloop`, `broken_redirect`;
- `redirect`: −1 when off, 0 when there's none;
- `prefixes`.

`CLIENT GETREDIR` returns the same −1, 0 or id.

`INFO clients` gains `tracking_clients`. `INFO stats` gains `tracking_total_keys`, `tracking_total_items` and `tracking_total_prefixes`, each at Redis's position in its section.

The `CLIENT` subcommands also need their entries in the ACL subcommand table and in `COMMAND`'s output, with Redis's categories.

### Cost

- **Off:** `touch` tests one flag that is false. `run` tests whether the client tracks (a registry lookup only when the registry is non-empty) and whether there is anything to flush.
- **On:**
  - each change appends one key to a list, then costs one hash lookup and one prefix scan;
  - each tracked read costs one hash insert per key;
  - each message is one push frame.

The design must be measured with `rexyra-bench ops` (`SET`, `GET`, `LPUSH`) in three conditions:
- before the change;
- after it, with tracking unused;
- after it, with one tracking client reading the benchmark's keys.

The first two must agree within the run-to-run noise.

## Verification

- **A comparison tool, `tools/rexyra-tracking`.** It runs a script of steps on several named connections against two servers and records everything each connection receives: replies, and pushes with their position relative to replies. The steps are:
  - `name: COMMAND`, which waits for the reply;
  - `name| A ;; B`, a pipeline;
  - `name! COMMAND`, sent without waiting;
  - `~sleep ms`;
  - `~drain`, which collects pushes.
  
  It is the Rust form of the Python probe used to establish the behaviour below. Its scripts cover every case in [Observed behaviour](#observed-behaviour), and the two transcripts must be identical. The only allowed differences are connection ids, and the order of keys dropped by the size limit, which Redis picks at random.
- **Unit tests** next to the code for the table, prefixes, option parsing and errors, `TRACKINGINFO`, the `caching` rules, and redirect and broken redirect. End-to-end tests in `crates/server/tests` cover real connections, RESP2 redirect, the expiry cycle and the size limit.
- **The client-library suites:** go-redis's `TestClientSideCache` and redis-py's `test_client_side_caching` lose their "not implemented" marks and must pass.

## Observed behaviour

Recorded from Redis 7.4.12 (Docker, `redis:7.4-alpine`) with the probe described above, on 22 November 2026. "Push" means a RESP3 push message.

- **Basic tracking:**
  - Reading a key with tracking on, then another client writing it, gives the reader the push `invalidate [k]`.
  - A second write gives nothing until the key is read again.
  - A read of a missing key is tracked.
  - A read that fails with `WRONGTYPE` is tracked.
  - `EXISTS`, `TYPE`, `TTL` and `STRLEN` track their keys.
- **Which writes invalidate:**
  - `MSET` of two tracked keys gives two messages, one key each.
  - A write in another database invalidates a key of the same name.
  - A write that changes nothing (`SADD` of an existing member) gives nothing, and neither does a failed write (`INCR` on a set).
  - `RENAME a b` invalidates both `a` and `b`. `EXPIRE` invalidates.
- **The tracking client's own writes:**
  - Its own write of a key it read gives its reply, then the push.
  - The same happens after `EXEC` (read and write in one transaction) and after `EVAL` (read then write).
  - `EVAL` (write then read) sends nothing and leaves the key tracked.
  - `EVAL_RO` reading a key tracks it.
- **`NOLOOP`:** own writes send nothing; others' writes do.
- **Expiry:**
  - Expiry invalidates however it is found: by the client's own read (push after its reply; the key is then tracked again), by another client's read, or by the cycle.
  - A hash field expiring invalidates the hash, by lookup or by the cycle. `HPEXPIRE` itself invalidates.
- **Eviction:** for each evicted tracked key, an invalidation and then the `evicted` notification.
- **Order:** on a client that both tracks and is subscribed to keyspace events, the invalidation comes before the event. The same holds for a write that serves a blocked `BLPOP`.
- **Flushes:**
  - `FLUSHDB` and `FLUSHALL` send `invalidate null` to every tracking client, including broadcasting ones, in id order.
  - A RESP2 redirect target gets `message __redis__:invalidate` with a null payload.
  - A client whose redirect is broken gets `tracking-redir-broken` instead.
  - `SWAPDB` sends nothing.
- **Redirects:**
  - A RESP2 redirect target in subscribed mode, subscribed to any channel, gets `message __redis__:invalidate [k]`.
  - RESP2 tracking without a redirect sends nothing.
  - A RESP3 redirect target that isn't subscribed gets the push.
  - When the redirect target has been killed, every message the tracking client would cause is replaced by `tracking-redir-broken <id>` to the tracking client. `TRACKINGINFO` then shows `broken_redirect`, and `CLIENT LIST` flags show `tR`.
- **Broadcast (`BCAST`):**
  - `BCAST PREFIX a PREFIX b`, then a pipeline `SET a2`, `SET b1`, `SET a1`, `SET c1`, `SET a2`, gives `invalidate [a1, a2]` and then `invalidate [b1]`.
  - A transaction's writes are batched the same way.
  - Own writes without `NOLOOP` give a push after the reply.
  - `BCAST` with no prefix covers every key, and `TRACKINGINFO` shows the prefix `""`.
- **`OPTIN`:**
  - A plain `GET` is not tracked.
  - `CACHING YES` then `GET` tracks it.
  - `CACHING YES`, `GET o3`, `GET o4` tracks only `o3`, and the same in a pipeline.
  - `CACHING YES`, `MULTI`, two `GET`s, `EXEC` tracks both.
  - `CACHING YES`, `PING`, `GET` tracks nothing.
  - `CACHING YES` then `EVAL` reading a key tracks it, and the next `EVAL` does not.
- **`OPTOUT`:** reads are tracked except the next command after `CACHING NO`.
- **`CLIENT TRACKINGINFO`:**
  - `{flags: {on}, redirect: 0, prefixes: []}`, with flags such as `optin`, `caching-yes`, `optout`, `caching-no`, `noloop`, `bcast`, `broken_redirect`.
  - When off: `{flags: {off}, redirect: -1, prefixes: []}`.
  - In RESP2 it is a flat array.
- **`CLIENT LIST`:** flags `t`, `tB`, `tR`, and `redir=-1` (off), `0`, or the target's id. An `OPTIN`/`NOLOOP` client shows only `t`.
- **`INFO`:** `tracking_clients` in `clients`; `tracking_total_keys`, `tracking_total_items` and `tracking_total_prefixes` in `stats`.
- **Errors:**
  - `ERR syntax error` for an unknown option or a missing value.
  - `ERR value is not an integer or out of range` for a non-numeric `REDIRECT`.
  - `ERR The client ID you want redirect to does not exist` for −1, 0 or an unknown id.
  - `ERR PREFIX option requires BCAST mode to be enabled`.
  - `ERR You can't specify both OPTIN mode and OPTOUT mode`.
  - `ERR OPTIN and OPTOUT are not compatible with BCAST`.
  - `ERR Prefix 'ab' overlaps with another provided prefix 'a'. Prefixes for a single client must not overlap.` and, for an existing prefix, `... overlaps with an existing prefix 'p'. ...`.
  - `ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.`
  - `ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.`
  - `ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.` and `... CACHING NO ... OPTOUT mode.`
  - `OFF` accepts and ignores options.
  - Arity errors are named `client|tracking`, `client|caching`, `client|trackinginfo` and `client|getredir`.
- **The size limit:**
  - `tracking-table-max-keys` defaults to 1000000.
  - Setting it to 2 with 6 keys tracked drops 4 keys at random, each sent as its own invalidation, within a few hundred milliseconds.
  - 0 means no limit.
  - −1 is refused: `ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') - argument must be between 0 and 9223372036854775807 inclusive`.
