# Roadmap and project plan

What comes next for Rexyra, in the order it is planned, and what each piece has to do to count as done. Nothing here is dated. If you need one of these sooner, say so in an issue, or better, offer to build it (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

Sizes are relative: **S** is a few focused hours, **M** a few days, **L** a week or more.

| Milestone | Theme | Size |
|---|---|---|
| [0.2.0](#020-loose-ends) | Loose ends of the service and installer release | S |
| [0.3.0](#030-pubsub) | Pub/Sub (released) | L |
| [0.4.0](#040-client-libraries-and-keyspace-notifications) | Client libraries and keyspace notifications (released) | L |
| [0.5.0](#050-settings-that-survive-and-a-server-you-can-leave-alone) | Settings that survive a restart, and day-to-day operation | M |
| [Later](#later) | TLS, more commands, the limits of the snapshot design | |

## How work is checked

The same for every item below, so it is not repeated:

- Behaviour is established from the Redis documentation and from a running Redis 7.4, and compared reply for reply in RESP2 and RESP3. A difference is either fixed or written down in [COMPATIBILITY.md](COMPATIBILITY.md).
- Every change in behaviour comes with a test that fails without it.
- Anything that touches the hot path is measured before and after with `rexyra-bench`, and the numbers are kept under [benchmarks](benchmarks).
- Anything that installs a service or changes a machine is tried on the build machine by the `Installer` workflow, not on a developer's own.

## 0.2.0: loose ends

Version 0.2.0 added the service mode, the installer, and the third-party notices. What it left to tidy up:

| | Item | Done when |
|---|---|---|
| S | Click through the installer by hand | The folder and port pages behave: a bad port is refused, Back and Next go where they should, and an upgrade skips the port page. The automated test installs silently, so nobody has seen these pages yet. |
| S | Dependency updates | The two open Dependabot pull requests are merged (mlua 0.12 is ready; `actions/checkout` needs a rebase), and the other actions that still run on a deprecated Node.js version are bumped. |
| S | Make the installer check required | `Windows service and installer` is a required status check on `main`. |
| S | Repository settings | Private vulnerability reporting on; labels and a few starter issues; Discussions if wanted. |

## 0.3.0: Pub/Sub

Pub/Sub was out of scope for the first version. It is now the next feature, for two reasons: it is one of the things Redis is most used for, and client libraries assume it is there. StackExchange.Redis, for one, opens a second connection for subscriptions as soon as it connects.

### What makes it more than a set of commands

Today a connection only ever receives the reply to something it sent: it hands a batch of commands to the execution thread and waits for the answer. A subscriber has to receive messages it did not ask for, at any moment, in order, and without ever being interleaved into the middle of a reply. So the first piece of work is in the server, not in the commands.

| | Item | Done when |
|---|---|---|
| M | **One ordered output path per connection.** Replies and pushed messages for a client travel through a single queue from the execution thread to its connection task, which writes them in that order. | A message published after a client's `SUBSCRIBE` was confirmed can never arrive before the confirmation; pipelined commands and blocking commands behave exactly as now; throughput and latency of ordinary commands are unchanged within noise in the benchmark. |
| M | **Output limits.** The bytes queued for a client and not yet written are counted. A subscriber that does not read is disconnected at the limit (`client-output-buffer-limit`, with Redis's defaults for the `pubsub` class), and the count shows as `omem` in `CLIENT LIST`. | A subscriber that stops reading while a publisher floods it is dropped at the limit, the server's memory stays bounded, and no other client is delayed. This also delivers the per-client buffer limits that were on the roadmap. |
| M | **The commands.** `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB CHANNELS` / `NUMSUB` / `NUMPAT`. | Replies and delivered messages match Redis in RESP2 (arrays) and RESP3 (push frames); `PUBLISH` returns the number of receivers; pattern matching uses the same glob rules as `KEYS`. |
| S | **Subscribed mode in RESP2.** Only the subscribe commands, `PING`, `QUIT` and `RESET` are accepted, `PING` answers in its Pub/Sub form, and the error for anything else has Redis's text. In RESP3 every command stays available. | Compared with Redis command by command. |
| S | **Sharded Pub/Sub.** `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`, `PUBSUB SHARDCHANNELS` / `SHARDNUMSUB`, which on a single instance behave like their plain counterparts with a separate set of channels. | As above. |
| S | **Access control.** The channel rules that `ACL SETUSER` already stores (`&pattern`, `allchannels`, `resetchannels`) are enforced, `ACL DRYRUN` checks them, and a subscriber whose permission is taken away is disconnected, as in Redis. | The ACL command scripts match Redis with the Pub/Sub cases added. |
| S | **Everything that mentions subscribers.** `CLIENT LIST` (`sub`, `psub`, `ssub`, the `P` flag), `CLIENT KILL TYPE pubsub`, `INFO` (`pubsub_channels`, `pubsub_patterns`, `pubsubshard_channels`), `RESET`, and cleaning up when a subscriber disconnects. `PUBLISH` from inside `MULTI` and from scripts; `SUBSCRIBE` refused inside scripts. | Covered by tests and by the comparison scripts. |
| M | **Verification.** A harness that drives several connections in a fixed order against Rexyra and Redis and compares everything each connection received. A fan-out benchmark: one publisher, many subscribers (done: [benchmarks/pubsub](benchmarks/pubsub)). | The harness runs in CI. The benchmark report gives messages per second at 1, 10, 100 and 1,000 subscribers, the latency of ordinary commands while it runs, and what happens with a slow subscriber. |
| S | **Documentation.** README, COMPATIBILITY.md and the changelog; Pub/Sub comes off the "not implemented" lists. | |

### Decisions taken

- Messages are not written to the append-only file and not part of a snapshot. A message published while nobody is subscribed is lost, as in Redis.
- Delivery to a subscriber is at most once and in publish order. There is no replay after a reconnect.
- Client-side caching (`CLIENT TRACKING`) builds on the same push path but is not part of this milestone.

## 0.4.0: client libraries and keyspace notifications

| | Item | Done when |
|---|---|---|
| L | **Client library smoke tests.** Done for StackExchange.Redis, redis-py, Jedis, Lettuce and go-redis, two versions of each in CI: [clients](../clients/README.md). Still to do: node-redis and redis-rs. Each connects the way its documentation shows, runs the basics of every data type, a transaction, a script, and a subscription. | Each library has a test that runs in CI, and whatever it turned up is fixed or listed in COMPATIBILITY.md. |
| M | **Keyspace notifications.** Done: `notify-keyspace-events` and the events for every command that changes a key, for expiry and for eviction, compared with Redis 7.4 by [rexyra-keyevents](../tools/rexyra-keyevents/src/main.rs); see [COMPATIBILITY.md](COMPATIBILITY.md#keyspace-notifications). | The events for each command match Redis's table, compared with the multi-connection harness. With the setting off, which is the default, the cost is not measurable. |
| S | **Commands the libraries ask for** on connecting or in common use and Rexyra lacks, as the smoke tests reveal them. Done: `MOVE`, `SWAPDB`, `COMMAND GETKEYS` and `COMMAND GETKEYSANDFLAGS`, with Redis's key specifications in `COMMAND INFO`. | |
| L | **Client-side caching** (`CLIENT TRACKING`), which go-redis and redis-py use when asked to cache. It needs the server to remember which keys each client has read and to push an invalidation when one changes. | go-redis and redis-py with caching on get cache hits and see changes made by other clients. |

## 0.5.0: settings that survive, and a server you can leave alone

Things found while building the service and the installer, and the items that were already next on the roadmap.

| | Item | Done when |
|---|---|---|
| M | **ACL persistence and audit.** `aclfile`, `ACL SAVE` / `ACL LOAD`, `ACL LOG`. Users created with `ACL SETUSER` survive a restart only through `CONFIG REWRITE` today. | |
| S | **`CONFIG GET` for the new settings** (`logfile`), and the service's state in `INFO`. | |
| M | **Signed releases.** The executable and the installer are unsigned, so Windows SmartScreen warns and antivirus products sometimes object. | Both are signed in the release workflow. This needs a code-signing certificate, which costs money and is the maintainers' decision. |
| S | **A `winget` package**, once releases are signed. | `winget install` works. |
| S | **Installer choices.** A password and "accept connections from other machines", with the firewall rule, as optional setup pages. | Tried by the installer test with those properties set. |
| S | **Windows Event Log.** A service that cannot start writes a note beside its configuration file today; the Event Log is where an administrator looks. | Start, stop and failure to start are logged there. |
| S | **Newer WiX.** The installer is built with WiX 5 because WiX 7 will not build until its licence agreement is accepted. | Decided by the maintainers; no code depends on it. |

## Later

Not scheduled. Roughly in order of likely demand.

- **TLS.** An optional encrypted listener. Until then, Rexyra belongs on networks you trust.
- **Check and repair tools** for snapshot and append-only files.
- **More commands:** `CLIENT PAUSE`, `LATENCY`, `DEBUG`, `MONITOR`, the `HELP` subcommands; HyperLogLog (`PFADD` ...); geospatial commands (`GEOADD` ...); streams (`XADD` ...).
- **Fewer I/O worker threads.** The network runtime starts one worker per logical processor. Under Pub/Sub fan-out, four workers delivered as much as 24 with under half the processor time ([benchmarks/pubsub](benchmarks/pubsub)). A smaller default, measured on ordinary traffic as well, and a setting like Redis's `io-threads`.
- **An ARM64 build.**

### Limits of the snapshot design, and questions it left open

These came out of the background-save work and its three rounds of measurement. Details are in [design/snapshot-without-fork.md](design/snapshot-without-fork.md) and [benchmarks/bgsave](benchmarks/bgsave).

- **Large collections inside transactions and scripts.** During a background save, a list, hash, set or sorted set with thousands of elements is captured a piece at a time, and a client that writes to one waits on its own. A write from inside `MULTI`/`EXEC` or a script cannot wait, so the whole collection is captured at once and every client waits (about 85 ms for a hash of a million fields).
- **Memory during a save** grows with whatever is overwritten while the save runs, until it has been written to disk.
- **A value handed to a reply carries 24 extra bytes** until it is next written.
- **Unexplained: a pause of about 6 ms per save** when the disk is slowed down artificially.
- **Saves of large collections take longer** than they did before the capture was split into pieces (0.61 s to 0.92 s in the benchmark), because the capture now shares the thread with the commands. Why it is as much as half as long again when no client is writing has not been looked into.

## Out of scope

Rexyra is a single-instance server for Windows. These are not planned:

- Replication and Sentinel
- Cluster mode
- Modules
- Platforms other than Windows
