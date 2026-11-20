# Contributing to Rexyra

Thank you for considering a contribution. Rexyra is a Redis-compatible server for Windows, and the most useful help is anything that makes it behave more like Redis for real applications.

## Ways to help

- **Report a difference from Redis.** If a command replies differently from Redis 7.x (a different value, error text, type or ordering), open a "Compatibility difference" issue with the commands to reproduce it and both replies. These reports are the most valuable contribution there is.
- **Try it with a real client library** (StackExchange.Redis, redis-py, node-redis, Jedis, go-redis, redis-rs) and tell us what broke.
- **Implement a missing command or feature.** See [docs/ROADMAP.md](docs/ROADMAP.md) for what is not done; open an issue first for anything larger than one command so the approach can be agreed before you write it.
- **Improve the documentation**, especially [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md), which must stay honest and current.
- **Fix a bug.** Issues labelled `good first issue` are small and self-contained.

## Ground rules

- **Windows is the platform.** The server is built and tested on Windows with the MSVC toolchain. Code that only works elsewhere will not be merged.
- **No `unsafe`.** It is forbidden across the workspace (`unsafe_code = "forbid"`).
- **Match Redis exactly where a client can see it.** Replies and error messages are compared with Redis byte for byte; client libraries and test suites depend on them.
- **Do not copy code from Redis.** Rexyra is an independent implementation under a different licence. Establish behaviour from the public documentation and by observing a running Redis server, not from its source code.
- **Every change in behaviour comes with a test** that fails without the change, and a line under "Unreleased" in `CHANGELOG.md` if users can notice it.
- **If Rexyra differs from Redis on purpose, say so** in [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md), in the same pull request.

## Getting set up

You need:

- Windows 10 or 11
- [Rust](https://rustup.rs/) (stable, MSVC toolchain)
- Visual Studio Build Tools with the "Desktop development with C++" workload (the embedded Lua interpreter is compiled from C)

```powershell
git clone https://github.com/moznhub/rexyra.git
cd rexyra
cargo build
cargo test --workspace
```

Run the server and talk to it with any Redis client:

```powershell
cargo run --release -p rexyra-server -- --port 6379
```

### Before you open a pull request

All three must pass; CI runs the same commands.

```powershell
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### Comparing with Redis

To check a reply against the real thing, run Redis 7 in Docker or WSL and send the same commands to both servers:

```powershell
docker run --rm -p 6380:6379 redis:7
redis-cli -p 6380 <command>   # Redis
redis-cli -p 6379 <command>   # Rexyra
```

Please include both replies in the pull request description when you change what a command returns.

### A note on antivirus software

Some antivirus products flag freshly compiled, unsigned test executables under `target\` with a generic heuristic verdict and lock or quarantine them, which makes `cargo test` fail with "the file is being used by another process". If that happens, excluding the repository's `target` folder from real-time scanning is the usual remedy.

## How the code is organised

| Crate | What it holds |
|---|---|
| `crates/resp` | RESP2/RESP3 parser and encoder. No I/O. |
| `crates/engine` | The keyspace: hash table, databases, value types, expiry. No I/O, no clock. |
| `crates/persist` | The RDB snapshot format: writer and reader. |
| `crates/commands` | The command table, every command handler, persistence, eviction, scripting, ACLs. |
| `crates/server` | Networking, the command-execution thread, configuration, the `rexyra-server` binary. |
| `tools/rexyra-bench` | Load generator for measuring background saves under traffic. |

One thread executes all commands and owns the keyspace; network I/O runs on a Tokio runtime and hands requests to it. Design notes are in [`docs/design`](docs/design).

### Adding a command

1. Add a line to the table in `crates/commands/src/table.rs` (name, arity, flags, key positions, group).
2. Write the handler next to its relatives (`strings.rs`, `lists.rs`, ...). Handlers are plain functions: `fn(&mut Ctx, &[Bytes]) -> Frame`.
3. Add unit tests in the same file using the test `Harness`, covering errors and edge cases as well as the normal reply.
4. Compare the replies with Redis, including wrong arguments and wrong types.
5. Add the command to the list in the README, and any deliberate difference to `docs/COMPATIBILITY.md`.

## Pull requests

- Keep each pull request to one change. Small ones are reviewed faster.
- Explain what the change does and how you checked it. "Compared with Redis 7.4: identical" is the kind of sentence reviewers look for.
- Do not mix reformatting or renaming with a change in behaviour.
- Expect review comments; they are about the code, not about you.

## Releasing (maintainers)

1. Set the new version in the root `Cargo.toml` and move the entries under "Unreleased" in `CHANGELOG.md` to a section for it, with the date. Merge that through a pull request.
2. Tag the merge commit on `main` and push the tag:

   ```powershell
   git tag -a v0.2.0 -m "Rexyra 0.2.0"
   git push origin v0.2.0
   ```

3. The "Release" workflow tests the tagged commit, builds a self-contained `rexyra-server.exe` and the installer, tries the installer out on the build machine, and publishes the release with checksums, using the changelog section as its notes. Versions before 1.0 are marked as pre-releases.

## Licence

Rexyra is dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at the user's option. By submitting a contribution you agree that it is licensed under the same terms, with no additional conditions, and you confirm that it is yours to give: your own work, not copied from Redis or from any source whose licence asks for more.

## Conduct and security

Everyone taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md). Please report security problems privately, as described in [SECURITY.md](SECURITY.md), not in a public issue.
