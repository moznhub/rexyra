//! Command table and handlers.
//!
//! Every command is described by a [`CommandSpec`] in [`COMMANDS`]. The
//! server's single execution thread calls [`dispatch`] for each parsed
//! request; handlers are plain synchronous functions that mutate state through
//! [`Ctx`] and return a [`Frame`].

pub mod acl;
mod admin;
pub mod aof;
mod bitmaps;
mod clients;
mod connection;
mod hashes;
mod introspection;
mod keys;
mod lcs;
mod lists;
mod lualibs;
pub mod memory;
mod object;
pub mod persistence;
mod scripting;
mod server;
mod sets;
mod sha1;
mod sha256;
mod sort;
mod strings;
mod table;
mod transactions;
mod zsets;

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use bytes::Bytes;
use rexyra_engine::{Db, Keyspace};
use rexyra_resp::{parse_i64, Frame, Protocol};

pub use memory::{MemoryMeter, Policy};
pub use scripting::Scripting;
pub use server::{BusyPoll, CommandStat, Effect, Server, SlowEntry, Stats};
pub use table::COMMANDS;
pub use transactions::{unwatch_all, Transaction, Watch};

/// Redis version whose command semantics we target. Reported by `HELLO` so
/// client libraries enable the right feature set.
pub const COMPAT_VERSION: &str = "7.2.0";

/// Per-connection state. Owned by the execution thread.
#[derive(Debug)]
pub struct ClientState {
    pub id: u64,
    pub addr: String,
    pub db: usize,
    pub proto: Protocol,
    pub name: Option<Bytes>,
    pub lib_name: Option<Bytes>,
    pub lib_ver: Option<Bytes>,
    /// Set by `QUIT`: flush the reply, then close the connection.
    pub close_after_reply: bool,
    /// The server-side address of the connection.
    pub laddr: String,
    /// When the client connected and when it last sent a command, in
    /// milliseconds since the Unix epoch.
    pub connected_at: u64,
    pub last_active: u64,
    /// Name of the last command sent, as `CLIENT LIST` shows it.
    pub last_command: &'static str,
    /// Waiting inside a blocking command. Maintained by the execution loop.
    pub blocked: bool,
    /// Set by `CLIENT NO-EVICT` and `CLIENT NO-TOUCH`.
    pub no_evict: bool,
    pub no_touch: bool,
    /// The open transaction, between `MULTI` and `EXEC` or `DISCARD`.
    pub multi: Option<Transaction>,
    /// Keys watched with `WATCH`. Release them with [`unwatch_all`] before
    /// dropping the client.
    pub watches: Vec<Watch>,
    /// The user the connection acts as, and whether it logged in as that
    /// user with `AUTH`. Until it does it is the `default` user, which may
    /// or may not be allowed to do anything; see [`acl`].
    pub user: String,
    pub authenticated: bool,
}

impl ClientState {
    pub fn new(id: u64, addr: String) -> Self {
        Self {
            id,
            addr,
            db: 0,
            proto: Protocol::Resp2,
            name: None,
            lib_name: None,
            lib_ver: None,
            close_after_reply: false,
            laddr: String::new(),
            connected_at: 0,
            last_active: 0,
            last_command: "NULL",
            blocked: false,
            no_evict: false,
            no_touch: false,
            multi: None,
            watches: Vec::new(),
            user: acl::DEFAULT_USER.to_owned(),
            authenticated: false,
        }
    }
}

/// Everything a handler may touch while executing one command.
pub struct Ctx<'a> {
    pub client: &'a mut ClientState,
    /// Every other connected client, by id.
    pub clients: &'a mut HashMap<u64, ClientState>,
    pub server: &'a mut Server,
    pub keyspace: &'a mut Keyspace,
    /// Milliseconds since the Unix epoch, sampled once for this command so
    /// every key it touches sees the same instant.
    pub now: u64,
    /// Set by a blocking command that found nothing to return. The caller
    /// then parks the client instead of sending the reply; see
    /// [`BlockRequest`].
    pub block: Option<BlockRequest>,
}

/// The type of value a blocked client is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    List,
    ZSet,
    /// Not a blocking command at all: a write to a large collection that a
    /// background save has still to capture. The client is held back until
    /// it has been; see `persistence::capture_blockers`.
    Capture,
}

/// A blocking command's request to wait, left in [`Ctx::block`].
///
/// The server parks the client and runs the same command again when one of
/// `keys` comes to hold a value of type `kind`. If `deadline` passes first,
/// the client receives the reply the handler returned alongside the request
/// (a null array for every blocking command).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRequest {
    pub keys: Vec<Bytes>,
    pub kind: BlockKind,
    /// Milliseconds since the Unix epoch; `None` waits forever.
    pub deadline: Option<u64>,
}

impl Ctx<'_> {
    /// Has `commands` written to the append-only file in place of the
    /// command now running; see [`aof`] for why. `commands` is only called
    /// if the file is enabled.
    pub(crate) fn log_instead(&mut self, commands: impl FnOnce() -> Vec<Vec<Bytes>>) {
        if let Some(aof) = &mut self.server.aof {
            aof.log_instead(commands());
        }
    }

    /// Asks to wait for one of `keys`, and returns the reply to send if the
    /// wait times out.
    pub(crate) fn block_on(
        &mut self,
        keys: &[Bytes],
        kind: BlockKind,
        deadline: Option<u64>,
    ) -> Frame {
        self.block = Some(BlockRequest {
            keys: keys.iter().map(copied).collect(),
            kind,
            deadline,
        });
        Frame::NullArray
    }

    /// The client's selected database, together with the command's clock.
    pub fn db(&mut self) -> (&mut Db, u64) {
        (self.keyspace.db(self.client.db), self.now)
    }
}

pub type Handler = fn(&mut Ctx<'_>, &[Bytes]) -> Frame;

/// Static description of one command, mirroring what `COMMAND INFO` reports.
pub struct CommandSpec {
    /// Lower-case command name.
    pub name: &'static str,
    /// Number of arguments including the command name. A negative value means
    /// "at least `-arity`".
    pub arity: i32,
    pub flags: &'static [&'static str],
    pub first_key: i64,
    pub last_key: i64,
    pub key_step: i64,
    pub group: &'static str,
    pub since: &'static str,
    pub summary: &'static str,
    pub handler: Handler,
}

/// Case-insensitive command lookup.
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    static INDEX: OnceLock<HashMap<&'static str, &'static CommandSpec>> = OnceLock::new();
    let index = INDEX.get_or_init(|| COMMANDS.iter().map(|spec| (spec.name, spec)).collect());

    // Lower-case into a stack buffer; no command name is anywhere near this long.
    let mut buf = [0u8; 32];
    let lowered = buf.get_mut(..name.len())?;
    lowered.copy_from_slice(name);
    lowered.make_ascii_lowercase();
    index.get(std::str::from_utf8(lowered).ok()?).copied()
}

/// Validates one command, then executes it, or queues it if the client is
/// inside a transaction.
pub fn dispatch(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let Some(name) = args.first() else {
        return Frame::error("ERR empty command");
    };
    let spec = match validate(name, args) {
        Ok(spec) => spec,
        Err(error) => {
            ctx.server.record_rejected(lookup(name), &error);
            if is(name, "EXEC") {
                return transactions::reject_exec(ctx, error);
            }
            // A command that cannot even be queued poisons the transaction.
            if let Some(transaction) = &mut ctx.client.multi {
                transaction.abort();
            }
            return error;
        }
    };
    ctx.client.last_active = ctx.now;
    ctx.client.last_command = spec.name;
    if let Err(text) = acl::check(ctx, spec, args) {
        if let Some(transaction) = &mut ctx.client.multi {
            transaction.abort();
        }
        let error = Frame::error(text);
        ctx.server.record_rejected(Some(spec), &error);
        return error;
    }
    if ctx.client.multi.is_some() && spec.flags.contains(&"no_multi") {
        if let Some(transaction) = &mut ctx.client.multi {
            transaction.abort();
        }
        let error = Frame::error("ERR Command not allowed inside a transaction");
        ctx.server.record_rejected(Some(spec), &error);
        return error;
    }
    // Make room first; if that is not possible, refuse what could add data.
    ctx.server.oom = memory::free_memory(ctx);
    if ctx.server.oom && spec.flags.contains(&"denyoom") {
        if let Some(transaction) = &mut ctx.client.multi {
            transaction.abort();
        }
        let error = Frame::error(memory::ERR_OOM);
        ctx.server.record_rejected(Some(spec), &error);
        return error;
    }
    match &mut ctx.client.multi {
        Some(transaction) if !transactions::runs_inside_multi(spec.name) => {
            transaction.queue(args);
            Frame::simple("QUEUED")
        }
        _ => {
            // A write to a large collection a background save has not got
            // to yet waits for it, rather than make everyone wait while the
            // whole collection is captured at once.
            if ctx.server.bgsave.is_some() && spec.flags.contains(&"write") {
                let keys = persistence::capture_blockers(ctx, spec, args);
                if !keys.is_empty() {
                    ctx.block = Some(BlockRequest {
                        keys,
                        kind: BlockKind::Capture,
                        deadline: None,
                    });
                    return Frame::NullArray;
                }
            }
            run(ctx, spec, args)
        }
    }
}

/// Runs a validated command and accounts for it.
pub(crate) fn run(ctx: &mut Ctx<'_>, spec: &CommandSpec, args: &[Bytes]) -> Frame {
    let started = Instant::now();
    let frame = (spec.handler)(ctx, args);
    let micros = started.elapsed().as_micros() as u64;
    ctx.keyspace.settle_watches();
    // A blocking command that is about to wait has not happened yet: it is
    // counted when it runs again and completes.
    let waiting = ctx.block.is_some();
    if !waiting {
        ctx.server
            .record(spec, args, micros, &frame, ctx.client, ctx.now);
    }
    aof::propagate(
        ctx,
        spec.flags.contains(&"write"),
        args,
        matches!(frame, Frame::Error(_)),
        waiting,
    );
    frame
}

fn validate(name: &Bytes, args: &[Bytes]) -> Result<&'static CommandSpec, Frame> {
    let spec = lookup(name).ok_or_else(|| unknown_command(name, &args[1..]))?;
    let given = args.len() as i64;
    let arity = i64::from(spec.arity);
    if (arity > 0 && given != arity) || (arity < 0 && given < -arity) {
        return Err(wrong_arity(spec.name));
    }
    Ok(spec)
}

fn unknown_command(name: &[u8], rest: &[Bytes]) -> Frame {
    const MAX_ECHOED: usize = 128;
    let mut preview = String::new();
    for arg in rest {
        if preview.len() >= MAX_ECHOED {
            break;
        }
        let room = MAX_ECHOED - preview.len();
        preview.push('\'');
        preview.extend(lossy(arg).chars().take(room));
        preview.push_str("' ");
    }
    let name: String = lossy(name).chars().take(MAX_ECHOED).collect();
    Frame::error(format!(
        "ERR unknown command '{name}', with args beginning with: {preview}"
    ))
}

// ---------------------------------------------------------------------------
// Helpers shared by handlers. Error texts match Redis byte for byte, because
// client libraries and test suites compare them.
// ---------------------------------------------------------------------------

pub(crate) const ERR_NOT_INTEGER: &str = "ERR value is not an integer or out of range";
pub(crate) const ERR_SYNTAX: &str = "ERR syntax error";

pub(crate) fn lossy(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

pub(crate) fn is(arg: &[u8], keyword: &str) -> bool {
    arg.eq_ignore_ascii_case(keyword.as_bytes())
}

pub(crate) fn int(arg: &[u8]) -> Result<i64, Frame> {
    parse_i64(arg).ok_or_else(|| Frame::error(ERR_NOT_INTEGER))
}

pub(crate) fn wrong_arity(command: &str) -> Frame {
    Frame::error(format!(
        "ERR wrong number of arguments for '{command}' command"
    ))
}

pub(crate) fn unknown_subcommand(command: &str, sub: &[u8]) -> Frame {
    Frame::error(format!(
        "ERR unknown subcommand '{}'. Try {command} HELP.",
        lossy(sub)
    ))
}

pub(crate) fn wrong_subcommand_arity(command: &str, sub: &[u8]) -> Frame {
    Frame::error(format!(
        "ERR wrong number of arguments for '{command}|{}' command",
        lossy(sub).to_ascii_lowercase()
    ))
}

/// Lets handlers use `?` on `Result<_, Frame>` and still return a `Frame`.
pub(crate) fn reply(result: Result<Frame, Frame>) -> Frame {
    result.unwrap_or_else(|error| error)
}

pub(crate) fn wrong_type() -> Frame {
    Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Parses the timeout of a blocking command, given in seconds with an
/// optional fraction, into an absolute deadline. Zero means "wait forever"
/// and yields `None`.
pub(crate) fn parse_timeout(arg: &[u8], now: u64) -> Result<Option<u64>, Frame> {
    let seconds = std::str::from_utf8(arg)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|seconds| !seconds.is_nan())
        .ok_or_else(|| Frame::error("ERR timeout is not a float or out of range"))?;
    let out_of_range = || Frame::error("ERR timeout is out of range");
    // Rounded up, so a wait is never shorter than what was asked for.
    let millis = (seconds * 1000.0).ceil();
    if millis > i64::MAX as f64 {
        return Err(out_of_range());
    }
    if millis < 0.0 {
        return Err(Frame::error("ERR timeout is negative"));
    }
    if millis == 0.0 {
        return Ok(None);
    }
    let deadline = (millis as u64).checked_add(now).ok_or_else(out_of_range)?;
    if deadline > i64::MAX as u64 {
        return Err(out_of_range());
    }
    Ok(Some(deadline))
}

/// Copies an argument into its own allocation before it is stored.
/// Arguments are slices of the connection's read buffer; storing one
/// directly would keep that whole buffer alive.
pub(crate) fn copied(arg: &Bytes) -> Bytes {
    Bytes::copy_from_slice(arg)
}

/// Parses a `SCAN`-family cursor.
pub(crate) fn parse_cursor(arg: &[u8]) -> Result<u64, Frame> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| Frame::error("ERR invalid cursor"))
}

pub(crate) struct ScanOptions<'a> {
    /// `None` when there is no pattern or it is `*`, which matches everything.
    pub pattern: Option<&'a [u8]>,
    pub count: usize,
}

/// Parses `[MATCH pattern] [COUNT count]` for `HSCAN`, `SSCAN` and `ZSCAN`.
pub(crate) fn parse_scan_options(mut options: &[Bytes]) -> Result<ScanOptions<'_>, Frame> {
    let mut parsed = ScanOptions {
        pattern: None,
        count: 10,
    };
    loop {
        match options {
            [] => return Ok(parsed),
            [opt, value, rest @ ..] if is(opt, "MATCH") => {
                parsed.pattern = (&value[..] != b"*").then_some(&value[..]);
                options = rest;
            }
            [opt, value, rest @ ..] if is(opt, "COUNT") => {
                parsed.count = match int(value)? {
                    n if n >= 1 => n as usize,
                    _ => return Err(Frame::error(ERR_SYNTAX)),
                };
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
}

/// Picks `count` distinct items at random. Asking for at least as many as
/// there are returns them all, in their original order.
pub(crate) fn sample_distinct<T>(mut items: Vec<T>, count: usize) -> Vec<T> {
    if count >= items.len() {
        return items;
    }
    // Partial Fisher–Yates shuffle: only the first `count` slots are settled.
    for index in 0..count {
        let remaining = items.len() - index;
        let pick = index + rexyra_engine::random_u64() as usize % remaining;
        items.swap(index, pick);
    }
    items.truncate(count);
    items
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub fn args(list: &[&str]) -> Vec<Bytes> {
        list.iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    /// A server in miniature: one client, a keyspace and a clock the test
    /// controls, so expiry can be tested without sleeping.
    pub struct Harness {
        pub keyspace: Keyspace,
        pub client: ClientState,
        /// Other connected clients, for the commands that list or kill them.
        pub clients: HashMap<u64, ClientState>,
        pub server: Server,
        pub now: u64,
        /// The wait requested by the most recent command, if it would block.
        pub block: Option<BlockRequest>,
    }

    impl Harness {
        pub fn new() -> Self {
            let server = Server::default();
            let mut client = ClientState::new(7, "127.0.0.1:50000".to_owned());
            acl::start_session(&server, &mut client);
            Self {
                keyspace: Keyspace::new(server.databases),
                client,
                clients: HashMap::new(),
                server,
                now: 1_700_000_000_000,
                block: None,
            }
        }

        pub fn raw(&mut self, args: &[Bytes]) -> Frame {
            let mut ctx = Ctx {
                client: &mut self.client,
                clients: &mut self.clients,
                server: &mut self.server,
                keyspace: &mut self.keyspace,
                now: self.now,
                block: None,
            };
            let frame = dispatch(&mut ctx, args);
            self.block = ctx.block;
            frame
        }

        pub fn cmd(&mut self, command: &[&str]) -> Frame {
            self.raw(&args(command))
        }

        /// Runs a command given as one whitespace-separated string.
        pub fn run(&mut self, line: &str) -> Frame {
            self.cmd(&line.split_whitespace().collect::<Vec<_>>())
        }

        pub fn advance(&mut self, millis: u64) {
            self.now += millis;
        }
    }

    /// Runs `commands` in order against one fresh client and returns the
    /// replies together with the final client state.
    pub fn run(commands: &[&[&str]]) -> (Vec<Frame>, ClientState) {
        let mut harness = Harness::new();
        let replies = commands.iter().map(|cmd| harness.cmd(cmd)).collect();
        (replies, harness.client)
    }

    pub fn one(cmd: &[&str]) -> Frame {
        run(&[cmd]).0.remove(0)
    }

    pub fn bulk(text: &str) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(text.as_bytes()))
    }

    pub fn err(text: &str) -> Frame {
        Frame::error(text)
    }

    pub fn bulks(items: &[&str]) -> Vec<Frame> {
        items.iter().map(|item| bulk(item)).collect()
    }

    pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
}

#[cfg(test)]
mod tests {
    use super::testing::one;
    use super::*;

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(lookup(b"PING").unwrap().name, "ping");
        assert_eq!(lookup(b"PiNg").unwrap().name, "ping");
        assert!(lookup(b"nope").is_none());
        assert!(lookup(&[b'a'; 100]).is_none());
        assert!(lookup(b"").is_none());
    }

    #[test]
    fn table_is_well_formed() {
        let mut seen = std::collections::HashSet::new();
        for spec in COMMANDS {
            assert_eq!(spec.name, spec.name.to_ascii_lowercase());
            assert!(seen.insert(spec.name), "duplicate {}", spec.name);
            assert_ne!(spec.arity, 0, "{}", spec.name);
            // A command either takes no keys or names a valid key range.
            if spec.first_key == 0 {
                assert_eq!((spec.last_key, spec.key_step), (0, 0), "{}", spec.name);
            } else {
                assert!(spec.key_step >= 1, "{}", spec.name);
                assert!(
                    // Negative counts back from the last argument.
                    spec.last_key < 0 || spec.last_key >= spec.first_key,
                    "{}",
                    spec.name
                );
                let writes = spec.flags.contains(&"write");
                let reads = spec.flags.contains(&"readonly");
                // `WATCH` names keys but neither reads nor writes them.
                assert!(
                    writes != reads || spec.name == "watch",
                    "{} must be write xor readonly",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn unknown_command_error_matches_redis() {
        assert_eq!(
            one(&["FOO", "a", "b"]),
            Frame::error("ERR unknown command 'FOO', with args beginning with: 'a' 'b' ")
        );
        assert_eq!(
            one(&["FOO"]),
            Frame::error("ERR unknown command 'FOO', with args beginning with: ")
        );
    }

    #[test]
    fn arity_is_enforced() {
        assert_eq!(
            one(&["ECHO"]),
            Frame::error("ERR wrong number of arguments for 'echo' command")
        );
        assert_eq!(
            one(&["ECHO", "a", "b"]),
            Frame::error("ERR wrong number of arguments for 'echo' command")
        );
        assert_eq!(
            one(&["CLIENT"]),
            Frame::error("ERR wrong number of arguments for 'client' command")
        );
        assert_eq!(
            one(&["GET"]),
            Frame::error("ERR wrong number of arguments for 'get' command")
        );
    }
}
