//! Lua scripting: `EVAL`, `EVALSHA`, their read-only variants and `SCRIPT`,
//! and Redis Functions: `FUNCTION` and `FCALL`.
//!
//! Scripts run on Lua 5.1, the version Redis embeds, inside one interpreter
//! owned by the execution thread. A script runs to completion before any
//! other command, so it is atomic like every other command.
//!
//! Inside a script, `redis.call` and `redis.pcall` run commands through the
//! same code path clients use. Replies are converted to Lua values and the
//! script's return value back to a reply, following Redis's rules (see
//! [`frame_to_lua`] and [`lua_to_frame`]).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use bytes::Bytes;
use mlua::{
    ChunkMode, Error as LuaError, Function, HookTriggers, Lua, LuaOptions, StdLib, Table, Value,
    Variadic, VmState,
};
use rexyra_engine::glob_match;
use rexyra_resp::{format_double, Frame, Protocol};

use crate::server::BusyPoll;
use crate::sha1::sha1_hex;
use crate::{
    int, is, lookup, lossy, reply, unknown_subcommand, wrong_subcommand_arity, Ctx, COMPAT_VERSION,
};

/// The chunk name scripts are compiled under; it appears in error messages.
const CHUNK_NAME: &str = "@user_script";
/// The chunk name function libraries are compiled under.
const LIBRARY_CHUNK_NAME: &str = "@user_function";
const ERR_NAME: &str = "ERR Library names can only contain letters, numbers, or underscores(_) \
                        and must be at least one character long";
/// How deeply nested a script's return value may be.
const MAX_REPLY_DEPTH: usize = 128;
/// The interpreter checks the clock once per this many instructions.
const HOOK_INTERVAL: u32 = 100_000;
const KILLED: &str = "ERR Script killed by user with SCRIPT KILL...";

/// What the interpreter's instruction hook needs to know about the script
/// that is running. Shared with the hook, which outlives any one script.
#[derive(Default)]
struct Watchdog {
    /// When the running script started; `None` between scripts.
    started: Mutex<Option<Instant>>,
    /// Milliseconds a script may run before other clients are told the
    /// server is busy. Zero means never.
    limit_ms: AtomicU64,
    /// Serves other clients once the script is over the limit.
    poll: Mutex<Option<Arc<dyn BusyPoll>>>,
    /// The script has run a write command, so it can no longer be killed
    /// without leaving its work half done.
    wrote: AtomicBool,
    killed: AtomicBool,
    /// Lines written with `redis.log`, with their level.
    logs: Mutex<Vec<(u8, String)>>,
}

/// Locks a mutex that guards plain data, which stays valid even if a holder
/// panicked.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Watchdog {
    /// Runs every [`HOOK_INTERVAL`] instructions.
    fn check(&self) -> mlua::Result<VmState> {
        if self.killed.load(Ordering::Relaxed) {
            return Err(killed_error());
        }
        let limit = self.limit_ms.load(Ordering::Relaxed);
        let over = lock(&self.started)
            .is_some_and(|started| limit > 0 && started.elapsed().as_millis() >= u128::from(limit));
        if over {
            let poll = lock(&self.poll).clone();
            let killable = !self.wrote.load(Ordering::Relaxed);
            if poll.is_some_and(|poll| poll.poll(killable)) {
                self.killed.store(true, Ordering::Relaxed);
                return Err(killed_error());
            }
        }
        Ok(VmState::Continue)
    }
}

fn killed_error() -> LuaError {
    LuaError::external(CommandError {
        message: KILLED.to_owned(),
    })
}

/// The reply to a script that is a precompiled chunk: what Redis's Lua, which
/// has no loader for them, says of the escape character they start with.
const BINARY_CHUNK: &str =
    "ERR Error compiling script (new function): user_script:1: unexpected symbol near 'char(27)'";

/// Runs once in a new interpreter. It receives functions implemented in
/// Rust and returns the two functions used to run scripts and library
/// functions, and the table scripts get as their globals.
const BOOTSTRAP: &str = r#"
local caught, handler, missing_global, readonly, clock, stop_if_killed = ...
local rawpcall, rawxpcall = pcall, xpcall

-- A script that catches a failed redis.call with pcall sees the error
-- message as a string, as it always has in Redis.
local function finish(ok, ...)
  if ok then return ok, ... end
  return ok, caught((...))
end
-- A script killed with SCRIPT KILL may catch the error once, but it does
-- not get to start another protected call: the error is raised again here,
-- outside the protection, so a loop around pcall cannot keep a killed
-- script alive.
pcall = function(f, ...)
  stop_if_killed()
  return finish(rawpcall(f, ...))
end
xpcall = function(f, on_error)
  stop_if_killed()
  return rawxpcall(f, on_error)
end

-- Not available to scripts. `getfenv`, `setfenv` and `newproxy` would be
-- ways to reach or replace the tables protected below.
local rawloadstring, rawsetfenv = loadstring, setfenv
dofile = nil
loadfile = nil
print = nil
getfenv = nil
setfenv = nil
newproxy = nil
os = { clock = clock }

-- One interpreter runs the scripts of every client, so nothing a script can
-- reach may be changed by it: a function it replaced would later run with
-- another caller's permissions. Redis marks the tables read-only inside its
-- Lua; stock Lua cannot, so scripts are given a read-only view of each
-- table instead: an empty table that reads through to the real one and
-- refuses every write.
local globals = _G
local rawnext, rawpairs, rawrawget, rawrawset = next, pairs, rawget, rawset
local rawgetmetatable, rawsetmetatable = getmetatable, setmetatable
local byte, concat, type, error = string.byte, table.concat, type, error
local READONLY = 'Attempt to modify a readonly table'
-- The real table behind each view, and what `getmetatable` says of a view.
local behind, metatable_of = {}, {}

local function view_of(real)
  local view = rawsetmetatable({}, {
    __index = real, __newindex = readonly, __metatable = false,
  })
  behind[view] = real
  return view
end

for _, name in ipairs({
  'string', 'table', 'math', 'os', 'coroutine',
  'bit', 'cjson', 'cmsgpack', 'struct', 'redis',
}) do
  globals[name] = view_of(globals[name])
end
-- Methods on strings keep finding the real `string` table; a script that
-- asks for the metatable gets a view of it.
rawgetmetatable('').__metatable = view_of({ __index = globals.string })

-- Scripts may not create or read undefined globals, which catches typos and
-- keeps one script from leaving state behind for the next.
local global_rules = { __index = missing_global, __newindex = readonly }
local script_globals = view_of(globals)
metatable_of[script_globals] = view_of(global_rules)

-- The functions that go around metamethods treat a view as the table it
-- stands for, and never hand out the table itself.
local function view_next(t, key)
  return rawnext(behind[t] or t, key)
end
next = view_next
pairs = function(t)
  if behind[t] then return view_next, t, nil end
  return rawpairs(t)
end
rawget = function(t, key)
  return rawrawget(behind[t] or t, key)
end
rawset = function(t, key, value)
  if behind[t] then error(READONLY, 0) end
  return rawrawset(t, key, value)
end
getmetatable = function(value)
  if behind[value] then return metatable_of[value] end
  return rawgetmetatable(value)
end
setmetatable = function(t, metatable)
  if behind[t] then error(READONLY, 0) end
  return rawsetmetatable(t, metatable)
end

-- Lua 5.1 runs precompiled chunks without verifying them, and a crafted one
-- can corrupt memory, so only source text is compiled, as in Redis. Parsing
-- an escape character as text gives the error Redis gives, with the chunk
-- named the way Lua would name it. What is compiled gets the same view of
-- the globals as the script that compiled it.
local function compile(chunk, name, default_name)
  if type(chunk) == 'string' and byte(chunk, 1) == 27 then
    return rawloadstring(' \27', name or default_name or chunk)
  end
  local compiled, problem = rawloadstring(chunk, name or default_name)
  if compiled then rawsetfenv(compiled, script_globals) end
  return compiled, problem
end
loadstring = function(chunk, name)
  return compile(chunk, name)
end
load = function(reader, name)
  local pieces, count = {}, 0
  while true do
    local piece = reader()
    if piece == nil or piece == '' then break end
    local kind = type(piece)
    if kind ~= 'string' and kind ~= 'number' then
      return nil, 'reader function must return a string'
    end
    count = count + 1
    pieces[count] = piece
  end
  return compile(concat(pieces), name, '=(load)')
end

globals._G = script_globals
rawsetmetatable(globals, global_rules)

-- Runs a script, and a library function with its keys and arguments.
return function(script) return rawxpcall(script, handler) end,
  function(f, keys, args)
    return rawxpcall(function() return f(keys, args) end, handler)
  end,
  script_globals
"#;

/// A command failed inside `redis.call`. Carried through Lua as an error
/// value and recognised again when it comes out the other side.
#[derive(Debug, Clone)]
struct CommandError {
    /// Error code and text, e.g. `WRONGTYPE Operation against ...`.
    message: String,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CommandError {}

/// Finds the [`CommandError`] inside an error that came back out of Lua.
fn command_error(error: &LuaError) -> Option<&CommandError> {
    match error {
        LuaError::CallbackError { cause, .. } => command_error(cause),
        LuaError::ExternalError(inner) => inner.downcast_ref::<CommandError>(),
        _ => None,
    }
}

/// The text of an error that is not a [`CommandError`].
fn plain_error_text(error: &LuaError) -> String {
    match error {
        LuaError::CallbackError { cause, .. } => plain_error_text(cause),
        LuaError::RuntimeError(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Gives an error message an error code if it has none, the way Redis does:
/// the first word is the code, and a one-word message gets `ERR`.
fn with_error_code(text: &str) -> String {
    let text = text.strip_prefix('-').unwrap_or(text);
    if text.contains(' ') {
        text.to_owned()
    } else {
        format!("ERR {text}")
    }
}

/// Where the script was when the function calling this was invoked: the
/// chunk name, its short form and the line. A built-in function such as
/// `error` is skipped, so this names the script line that called it.
fn script_position(lua: &Lua) -> Option<(String, String, i64)> {
    // Skip built-in functions and this module's own Lua wrappers.
    let mut level = 1;
    let frame = loop {
        let frame = lua.inspect_stack(level)?;
        let source = frame.source();
        let internal = source.what == "C" || source.source.as_deref() == Some("@bootstrap");
        if !internal || level >= 4 {
            break frame;
        }
        level += 1;
    };
    let line = i64::from(frame.curr_line());
    let source = frame.source();
    Some((
        source.source?.into_owned(),
        source.short_src?.into_owned(),
        line,
    ))
}

/// A Lua runtime error carrying `text`, prefixed with the script position
/// as Lua's own `error` function would.
pub(crate) fn error_at_caller(lua: &Lua, text: String) -> LuaError {
    match script_position(lua) {
        Some((_, short, line)) => LuaError::runtime(format!("{short}:{line}: {text}")),
        None => LuaError::runtime(text),
    }
}

/// What a script's `pcall` receives when the protected call failed: the
/// plain message of a failed command or of an `{err = ...}` table, and any
/// other error value unchanged.
fn caught_error(lua: &Lua, value: Value) -> mlua::Result<Value> {
    let text = match &value {
        Value::Error(error) => match command_error(error) {
            Some(failure) => failure.message.clone(),
            None => plain_error_text(error),
        },
        Value::Table(table) => match table.raw_get::<Value>("err") {
            Ok(Value::String(text)) => return Ok(Value::String(text)),
            _ => return Ok(value),
        },
        _ => return Ok(value),
    };
    Ok(Value::String(lua.create_string(text)?))
}

/// Runs when a script fails without catching the error. Turns the error
/// value into a table with `err`, `source` and `line`, while the stack still
/// shows where it happened.
fn uncaught_error(lua: &Lua, value: Value) -> mlua::Result<Table> {
    let message = match &value {
        Value::Error(error) => match command_error(error) {
            Some(failure) => failure.message.clone(),
            None => format!("ERR {}", plain_error_text(error)),
        },
        Value::Table(table) => match table.raw_get::<Value>("err") {
            Ok(Value::String(text)) => text.to_string_lossy(),
            _ => "ERR unknown error".to_owned(),
        },
        Value::String(text) => format!("ERR {}", text.to_string_lossy()),
        Value::Nil => "ERR nil".to_owned(),
        other => match lua.coerce_string(other.clone()) {
            Ok(Some(text)) => format!("ERR {}", text.to_string_lossy()),
            _ => "ERR unknown error".to_owned(),
        },
    };
    let table = lua.create_table()?;
    table.raw_set("err", message)?;
    if let Some((source, _, line)) = script_position(lua) {
        table.raw_set("source", source)?;
        table.raw_set("line", line)?;
    }
    Ok(table)
}

/// One compiled script.
struct Script {
    source: Bytes,
    function: Function,
    /// Declared with `#!lua flags=no-writes`.
    no_writes: bool,
}

/// The Lua interpreter and the script cache.
pub struct Scripting {
    lua: Lua,
    /// The `redis` table scripts see.
    api: Table,
    /// Runs a script function under `pcall`.
    run: Function,
    /// Runs a library function under `pcall`, passing its keys and arguments.
    run_function: Function,
    /// What scripts and loaded libraries see as their globals: a read-only
    /// view of the real ones, set up by [`BOOTSTRAP`]. `KEYS`, `ARGV` and
    /// the `redis` functions are still set on the real tables, which only
    /// this side can reach.
    script_globals: Table,
    scripts: HashMap<String, Script>,
    watchdog: Arc<Watchdog>,
    /// The metamethod that makes a table read-only; reused for each
    /// library's environment.
    readonly: Function,
    /// What a library sees as its globals while it is being loaded: only a
    /// cut-down `redis` table. See [`Scripting::load_library`].
    load_globals: Table,
    load_api: Table,
    /// Loaded function libraries by name.
    libraries: BTreeMap<String, Library>,
    /// Which library each function belongs to, by lower-cased function name.
    functions: HashMap<String, String>,
}

/// A function library loaded with `FUNCTION LOAD`.
struct Library {
    code: Bytes,
    functions: Vec<LibraryFunction>,
}

struct LibraryFunction {
    name: String,
    callback: Function,
    description: Option<String>,
    flags: Vec<String>,
}

impl LibraryFunction {
    fn no_writes(&self) -> bool {
        self.flags.iter().any(|flag| flag == "no-writes")
    }
}

impl Scripting {
    fn new() -> mlua::Result<Scripting> {
        // The base library is always loaded. No `io`, `os`, `package` or
        // `debug`: scripts cannot reach the file system or load code.
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )?;

        let api = lua.create_table()?;
        api.raw_set(
            "sha1hex",
            lua.create_function(|_, text: mlua::String| Ok(sha1_hex(&text.as_bytes())))?,
        )?;
        api.raw_set(
            "status_reply",
            lua.create_function(|lua, text: mlua::String| {
                let table = lua.create_table()?;
                table.raw_set("ok", text)?;
                Ok(table)
            })?,
        )?;
        api.raw_set(
            "error_reply",
            lua.create_function(|lua, text: mlua::String| {
                let table = lua.create_table()?;
                table.raw_set("err", with_error_code(&text.to_string_lossy()))?;
                Ok(table)
            })?,
        )?;
        let watchdog = Arc::new(Watchdog::default());
        let shared = Arc::clone(&watchdog);
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(HOOK_INTERVAL),
            move |_, _| shared.check(),
        );

        let shared = Arc::clone(&watchdog);
        api.raw_set(
            "log",
            lua.create_function(move |lua, args: Variadic<Value>| {
                let [level, message @ ..] = &args[..] else {
                    return Err(LuaError::runtime(
                        "redis.log() requires two arguments or more.",
                    ));
                };
                if message.is_empty() {
                    return Err(LuaError::runtime(
                        "redis.log() requires two arguments or more.",
                    ));
                }
                let level = match level {
                    Value::Integer(level) => *level,
                    Value::Number(level) => *level as i64,
                    _ => {
                        return Err(LuaError::runtime(
                            "First argument must be a number (log level).",
                        ))
                    }
                };
                if !(0..=3).contains(&level) {
                    return Err(LuaError::runtime("Invalid log level."));
                }
                let mut line = String::new();
                for (index, part) in message.iter().enumerate() {
                    if index > 0 {
                        line.push(' ');
                    }
                    if let Ok(Some(text)) = lua.coerce_string(part.clone()) {
                        line.push_str(&text.to_string_lossy());
                    }
                }
                lock(&shared.logs).push((level as u8, line));
                Ok(())
            })?,
        )?;
        // Script effects are always applied as the commands they ran.
        api.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
        for (name, level) in [
            ("LOG_DEBUG", 0),
            ("LOG_VERBOSE", 1),
            ("LOG_NOTICE", 2),
            ("LOG_WARNING", 3),
        ] {
            api.raw_set(name, level)?;
        }
        api.raw_set("REDIS_VERSION", COMPAT_VERSION)?;
        let mut version = COMPAT_VERSION
            .split('.')
            .map(|part| part.parse().unwrap_or(0));
        let mut next = || version.next().unwrap_or(0_i64);
        api.raw_set("REDIS_VERSION_NUM", (next() << 16) | (next() << 8) | next())?;
        lua.globals().raw_set("redis", api.clone())?;
        crate::lualibs::install(&lua)?;

        let caught = lua.create_function(caught_error)?;
        let handler = lua.create_function(uncaught_error)?;
        let missing_global = lua.create_function(|lua, (_, name): (Value, Value)| {
            let name = match lua.coerce_string(name) {
                Ok(Some(name)) => name.to_string_lossy(),
                _ => "?".to_owned(),
            };
            Err::<(), _>(error_at_caller(
                lua,
                format!("Script attempted to access nonexistent global variable '{name}'"),
            ))
        })?;
        let readonly = lua.create_function(|lua, _: Variadic<Value>| {
            Err::<(), _>(error_at_caller(
                lua,
                "Attempt to modify a readonly table".to_owned(),
            ))
        })?;
        // Seconds of processor time in Lua; time since start-up is a close
        // enough stand-in for timing code within one script.
        let started = std::time::Instant::now();
        let clock = lua.create_function(move |_, ()| Ok(started.elapsed().as_secs_f64()))?;
        let shared = Arc::clone(&watchdog);
        let stop_if_killed = lua.create_function(move |_, ()| {
            if shared.killed.load(Ordering::Relaxed) {
                Err(killed_error())
            } else {
                Ok(())
            }
        })?;
        let (run, run_function, script_globals) = lua
            .load(BOOTSTRAP)
            .set_name("@bootstrap")
            .call::<(Function, Function, Table)>((
                caught,
                handler,
                missing_global.clone(),
                readonly.clone(),
                clock,
                stop_if_killed,
            ))?;

        // The little a library may use while loading: logging and the
        // version, plus `register_function`, added for each load.
        let guard = lua.create_table()?;
        guard.raw_set("__index", missing_global.clone())?;
        guard.raw_set("__newindex", readonly.clone())?;
        let load_api = lua.create_table()?;
        for name in [
            "log",
            "LOG_DEBUG",
            "LOG_VERBOSE",
            "LOG_NOTICE",
            "LOG_WARNING",
            "REDIS_VERSION",
            "REDIS_VERSION_NUM",
        ] {
            load_api.raw_set(name, api.raw_get::<Value>(name)?)?;
        }
        load_api.set_metatable(Some(guard.clone()));
        let load_globals = lua.create_table()?;
        load_globals.raw_set("redis", load_api.clone())?;
        load_globals.set_metatable(Some(guard));

        Ok(Scripting {
            lua,
            api,
            run,
            run_function,
            script_globals,
            scripts: HashMap::new(),
            watchdog,
            readonly,
            load_globals,
            load_api,
            libraries: BTreeMap::new(),
            functions: HashMap::new(),
        })
    }

    /// Compiles `source` and adds it to the cache, returning its SHA-1.
    fn load(&mut self, source: &Bytes) -> Result<String, Frame> {
        let sha = sha1_hex(source);
        if self.scripts.contains_key(&sha) {
            return Ok(sha);
        }
        let (body, no_writes) = strip_shebang(source)?;
        // A precompiled chunk starts with an escape character. Lua 5.1 does
        // not verify bytecode, so it is never loaded: this is the error
        // Redis gives, whose Lua parses everything as source text.
        if body.first() == Some(&0x1B) {
            return Err(Frame::error(BINARY_CHUNK));
        }
        let function = self
            .lua
            .load(&body[..])
            .set_name(CHUNK_NAME)
            .set_mode(ChunkMode::Text)
            .into_function()
            .map_err(|error| {
                let text = match &error {
                    LuaError::SyntaxError { message, .. } => message.clone(),
                    other => other.to_string(),
                };
                Frame::error(format!("ERR Error compiling script (new function): {text}"))
            })?;
        function
            .set_environment(self.script_globals.clone())
            .map_err(|error| Frame::error(format!("ERR {error}")))?;
        self.scripts.insert(
            sha.clone(),
            Script {
                source: Bytes::copy_from_slice(source),
                function,
                no_writes,
            },
        );
        Ok(sha)
    }

    /// Runs the cached script `sha`.
    fn eval(
        &self,
        ctx: &mut Ctx<'_>,
        sha: &str,
        keys: &[Bytes],
        argv: &[Bytes],
        read_only: bool,
    ) -> Frame {
        let Some(script) = self.scripts.get(sha) else {
            return Frame::error("NOSCRIPT No matching script. Please use EVAL.");
        };
        let read_only = read_only || script.no_writes;
        self.execute(ctx, sha, &script.function, keys, argv, read_only, false)
    }

    /// Runs `function` as a script (keys and arguments in the `KEYS` and
    /// `ARGV` globals) or as a library function (passed as two tables).
    /// `label` names it in error messages.
    #[allow(clippy::too_many_arguments)]
    fn execute(
        &self,
        ctx: &mut Ctx<'_>,
        label: &str,
        function: &Function,
        keys: &[Bytes],
        argv: &[Bytes],
        read_only: bool,
        as_function: bool,
    ) -> Frame {
        // Commands run by the script reply in the script's protocol version,
        // whatever the client speaks.
        let client_proto = ctx.client.proto;
        let watchdog = &self.watchdog;
        watchdog.wrote.store(false, Ordering::Relaxed);
        watchdog.killed.store(false, Ordering::Relaxed);
        watchdog
            .limit_ms
            .store(ctx.server.busy_reply_threshold, Ordering::Relaxed);
        *lock(&watchdog.poll) = ctx.server.busy_poll.clone();
        *lock(&watchdog.started) = Some(Instant::now());

        // A script is logged as the commands it runs, as one unit.
        if let Some(aof) = &mut ctx.server.aof {
            aof.begin_batch();
        }
        let outcome = self.run_script(ctx, function, keys, argv, read_only, as_function);
        if let Some(aof) = &mut ctx.server.aof {
            aof.end_batch();
        }

        *lock(&watchdog.started) = None;
        *lock(&watchdog.poll) = None;
        ctx.server.logs.append(&mut lock(&watchdog.logs));
        ctx.client.proto = client_proto;

        match outcome {
            Ok((true, value, resp3)) => lua_to_frame(&value, resp3, 0),
            Ok((false, error, _)) => script_failure(label, &error),
            Err(error) => Frame::error(format!("ERR {}", plain_error_text(&error))),
        }
    }

    fn run_script(
        &self,
        ctx: &mut Ctx<'_>,
        function: &Function,
        keys: &[Bytes],
        argv: &[Bytes],
        read_only: bool,
        as_function: bool,
    ) -> mlua::Result<(bool, Value, bool)> {
        let lua = &self.lua;
        let strings = |items: &[Bytes]| -> mlua::Result<Table> {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (index, item) in items.iter().enumerate() {
                table.raw_seti(index + 1, lua.create_string(item)?)?;
            }
            Ok(table)
        };
        let (keys, argv) = (strings(keys)?, strings(argv)?);
        let globals = lua.globals();
        if as_function {
            // A function gets them as arguments; the globals are not defined.
            globals.raw_set("KEYS", Value::Nil)?;
            globals.raw_set("ARGV", Value::Nil)?;
        } else {
            globals.raw_set("KEYS", keys.clone())?;
            globals.raw_set("ARGV", argv.clone())?;
        }

        let proto = Cell::new(Protocol::Resp2);
        let ctx = RefCell::new(ctx);
        lua.scope(|scope| {
            let call = |raise: bool| {
                let (ctx, proto) = (&ctx, &proto);
                scope.create_function(move |lua, args: Variadic<Value>| {
                    let mut ctx = ctx
                        .try_borrow_mut()
                        .map_err(|_| LuaError::RecursiveMutCallback)?;
                    let resp3 = proto.get() == Protocol::Resp3;
                    ctx.client.proto = proto.get();
                    match run_command(&mut ctx, &args, read_only, &self.watchdog) {
                        Ok(frame) => frame_to_lua(lua, &frame, resp3),
                        Err(message) if raise => Err(LuaError::external(CommandError { message })),
                        Err(message) => {
                            let table = lua.create_table()?;
                            table.raw_set("err", message)?;
                            Ok(Value::Table(table))
                        }
                    }
                })
            };
            self.api.raw_set("call", call(true)?)?;
            self.api.raw_set("pcall", call(false)?)?;
            self.api.raw_set(
                "setresp",
                scope.create_function(|_, version: i64| {
                    proto.set(match version {
                        2 => Protocol::Resp2,
                        3 => Protocol::Resp3,
                        _ => return Err(LuaError::runtime("RESP version must be 2 or 3.")),
                    });
                    Ok(())
                })?,
            )?;
            let (ok, value) = if as_function {
                self.run_function
                    .call::<(bool, Value)>((function, keys, argv))?
            } else {
                self.run.call::<(bool, Value)>(function)?
            };
            // Whether the script switched to RESP3 decides how it returns
            // booleans.
            Ok((ok, value, proto.get() == Protocol::Resp3))
        })
    }
}

/// Handles a `#!lua [flags=...]` first line: blanks it out (keeping line
/// numbers intact) and reports whether the script declared `no-writes`.
fn strip_shebang(source: &[u8]) -> Result<(Vec<u8>, bool), Frame> {
    if !source.starts_with(b"#!") {
        return Ok((source.to_vec(), false));
    }
    let end = source
        .iter()
        .position(|&byte| byte == b'\n')
        .unwrap_or(source.len());
    let line = lossy(&source[2..end]);
    let mut words = line.split_whitespace();
    let engine = words.next().unwrap_or_default();
    if engine != "lua" {
        return Err(Frame::error(format!(
            "ERR Unexpected engine in script shebang: #!{engine}"
        )));
    }
    let mut no_writes = false;
    for word in words {
        let Some(flags) = word.strip_prefix("flags=") else {
            return Err(Frame::error(format!(
                "ERR Unknown lua shebang option: {word}"
            )));
        };
        for flag in flags.split(',').filter(|flag| !flag.is_empty()) {
            match flag {
                "no-writes" => no_writes = true,
                // These concern memory limits, replicas and clusters, which
                // this server does not have.
                "allow-oom" | "allow-stale" | "no-cluster" | "allow-cross-slot-keys" => {}
                other => {
                    return Err(Frame::error(format!(
                        "ERR Unexpected flag in script shebang: {other}"
                    )))
                }
            }
        }
    }
    let mut body = source.to_vec();
    body[..end].fill(b' ');
    Ok((body, no_writes))
}

/// Runs one command for `redis.call` / `redis.pcall`. `Err` carries the
/// error reply's text.
fn run_command(
    ctx: &mut Ctx<'_>,
    args: &[Value],
    read_only: bool,
    watchdog: &Watchdog,
) -> Result<Frame, String> {
    // A killed script that caught the error must not carry on.
    if watchdog.killed.load(Ordering::Relaxed) {
        return Err(KILLED.to_owned());
    }
    if args.is_empty() {
        return Err("ERR Please specify at least one argument for this redis lib call".to_owned());
    }
    let mut command = Vec::with_capacity(args.len());
    for arg in args {
        let text = match arg {
            Value::String(text) => Bytes::copy_from_slice(&text.as_bytes()),
            Value::Integer(number) => Bytes::from(number.to_string()),
            // The shortest text that reads back as the same number, so no
            // precision is lost on the way into a command.
            Value::Number(number) => Bytes::from(format_double(*number)),
            _ => return Err(NOT_STRINGS.to_owned()),
        };
        command.push(text);
    }

    let Some(spec) = lookup(&command[0]) else {
        return Err("ERR Unknown Redis command called from script".to_owned());
    };
    let given = command.len() as i64;
    let arity = i64::from(spec.arity);
    if (arity > 0 && given != arity) || (arity < 0 && given < -arity) {
        return Err("ERR Wrong number of args calling Redis command from script".to_owned());
    }
    if spec.flags.contains(&"noscript") {
        return Err("ERR This Redis command is not allowed from script".to_owned());
    }
    if read_only && spec.flags.contains(&"write") {
        return Err("ERR Write commands are not allowed from read-only scripts.".to_owned());
    }

    crate::acl::check_in_script(ctx, spec, &command)?;
    // Out of memory, a script may not add data, unless it has already
    // written: then it runs to the end rather than stop half-way.
    if ctx.server.oom && spec.flags.contains(&"denyoom") && !watchdog.wrote.load(Ordering::Relaxed)
    {
        return Err(crate::memory::ERR_OOM.to_owned());
    }
    if spec.flags.contains(&"write") {
        watchdog.wrote.store(true, Ordering::Relaxed);
    }
    let frame = crate::run(ctx, spec, &command);
    // A blocking command does not wait inside a script.
    ctx.block = None;
    match frame {
        Frame::Error(text) => Err(text),
        frame => Ok(frame),
    }
}

const NOT_STRINGS: &str = "ERR Lua redis lib command arguments must be strings or integers";

/// Converts a command's reply into the Lua value a script sees.
///
/// In RESP2 mode (the default) nulls become `false`, statuses become
/// `{ok=...}`, errors `{err=...}`, and RESP3-only types are flattened as
/// they would be for a RESP2 client. After `redis.setresp(3)` the RESP3
/// types keep their identity as single-field tables.
fn frame_to_lua(lua: &Lua, frame: &Frame, resp3: bool) -> mlua::Result<Value> {
    let single = |field: &str, value: Value| -> mlua::Result<Value> {
        let table = lua.create_table()?;
        table.raw_set(field, value)?;
        Ok(Value::Table(table))
    };
    let string =
        |bytes: &[u8]| -> mlua::Result<Value> { Ok(Value::String(lua.create_string(bytes)?)) };
    let array = |items: &mut dyn Iterator<Item = &Frame>| -> mlua::Result<Value> {
        let table = lua.create_table()?;
        for (index, item) in items.enumerate() {
            table.raw_seti(index + 1, frame_to_lua(lua, item, resp3)?)?;
        }
        Ok(Value::Table(table))
    };

    match frame {
        Frame::Integer(number) => Ok(Value::Integer(*number)),
        Frame::Bulk(bytes) => string(bytes),
        Frame::Simple(text) => single("ok", string(text.as_bytes())?),
        Frame::Error(text) => single("err", string(text.as_bytes())?),
        Frame::Null | Frame::NullArray if resp3 => Ok(Value::Nil),
        Frame::Null | Frame::NullArray => Ok(Value::Boolean(false)),
        Frame::Array(items) | Frame::Push(items) | Frame::Sequence(items) => {
            array(&mut items.iter())
        }
        Frame::Double(number) if resp3 => single("double", Value::Number(*number)),
        Frame::Double(number) => string(format_double(*number).as_bytes()),
        Frame::Boolean(flag) if resp3 => Ok(Value::Boolean(*flag)),
        Frame::Boolean(flag) => Ok(Value::Integer(i64::from(*flag))),
        Frame::BigNumber(digits) if resp3 => single("big_number", string(digits.as_bytes())?),
        Frame::BigNumber(digits) => string(digits.as_bytes()),
        Frame::Verbatim { format, data } if resp3 => {
            let inner = lua.create_table()?;
            inner.raw_set("string", lua.create_string(data)?)?;
            inner.raw_set("format", lua.create_string(format)?)?;
            single("verbatim_string", Value::Table(inner))
        }
        Frame::Verbatim { data, .. } => string(data),
        Frame::Map(pairs) if resp3 => {
            let inner = lua.create_table()?;
            for (key, value) in pairs {
                inner.raw_set(
                    frame_to_lua(lua, key, resp3)?,
                    frame_to_lua(lua, value, resp3)?,
                )?;
            }
            single("map", Value::Table(inner))
        }
        Frame::Map(pairs) => array(&mut pairs.iter().flat_map(|(key, value)| [key, value])),
        Frame::Set(items) if resp3 => {
            let inner = lua.create_table()?;
            for item in items {
                inner.raw_set(frame_to_lua(lua, item, resp3)?, true)?;
            }
            single("set", Value::Table(inner))
        }
        Frame::Set(items) => array(&mut items.iter()),
    }
}

/// Converts a script's return value into a reply.
///
/// Numbers are truncated to integers, `true` becomes 1, `false` and `nil`
/// become null (after `redis.setresp(3)`, booleans stay booleans), and a
/// table is an array that ends at its first `nil`, unless it has one of the
/// fields that mark a status, an error or a RESP3 type.
fn lua_to_frame(value: &Value, resp3: bool, depth: usize) -> Frame {
    if depth > MAX_REPLY_DEPTH {
        return Frame::error("ERR reached lua stack limit");
    }
    match value {
        Value::String(text) => Frame::Bulk(Bytes::copy_from_slice(&text.as_bytes())),
        Value::Integer(number) => Frame::Integer(*number),
        // Truncated towards zero. Out-of-range values give what Redis gives.
        Value::Number(number) if number.abs() < 9.2e18 => Frame::Integer(*number as i64),
        Value::Number(_) => Frame::Integer(i64::MIN),
        Value::Boolean(flag) if resp3 => Frame::Boolean(*flag),
        Value::Boolean(true) => Frame::Integer(1),
        Value::Table(table) => {
            let field = |name: &str| table.raw_get::<Value>(name).unwrap_or(Value::Nil);
            if let Value::String(text) = field("err") {
                return Frame::Error(text.to_string_lossy());
            }
            if let Value::String(text) = field("ok") {
                return Frame::Simple(text.to_string_lossy());
            }
            match field("double") {
                Value::Number(number) => return Frame::Double(number),
                Value::Integer(number) => return Frame::Double(number as f64),
                _ => {}
            }
            if let Value::Table(map) = field("map") {
                return Frame::Map(
                    map.pairs::<Value, Value>()
                        .flatten()
                        .map(|(key, value)| {
                            (
                                lua_to_frame(&key, resp3, depth + 1),
                                lua_to_frame(&value, resp3, depth + 1),
                            )
                        })
                        .collect(),
                );
            }
            if let Value::Table(set) = field("set") {
                return Frame::Set(
                    set.pairs::<Value, Value>()
                        .flatten()
                        .map(|(member, _)| lua_to_frame(&member, resp3, depth + 1))
                        .collect(),
                );
            }
            let mut items = Vec::new();
            for index in 1.. {
                match table.raw_get::<Value>(index) {
                    Ok(Value::Nil) | Err(_) => break,
                    Ok(item) => items.push(lua_to_frame(&item, resp3, depth + 1)),
                }
            }
            Frame::Array(items)
        }
        _ => Frame::Null,
    }
}

/// The reply for a script that ended with an error instead of returning.
/// `error` is the table [`uncaught_error`] built.
fn script_failure(sha: &str, error: &Value) -> Frame {
    let Value::Table(table) = error else {
        return Frame::error(format!("ERR Error running script script: {sha}."));
    };
    let text = |name: &str| match table.raw_get::<Value>(name) {
        Ok(Value::String(text)) => Some(text.to_string_lossy()),
        _ => None,
    };
    let message = text("err").unwrap_or_else(|| "ERR unknown error".to_owned());
    let line = match table.raw_get::<Value>("line") {
        Ok(Value::Integer(line)) => Some(line),
        _ => None,
    };
    Frame::error(match (text("source"), line) {
        (Some(source), Some(line)) => {
            format!("{message} script: {sha}, on {source}:{line}.")
        }
        _ => format!("{message} script: {sha}."),
    })
}

// --- commands ---------------------------------------------------------------

/// Runs `action` with the script engine, starting it on first use.
///
/// The engine is taken out of the server state meanwhile, because the
/// commands a script runs need that state for themselves.
fn with_engine(
    ctx: &mut Ctx<'_>,
    action: impl FnOnce(&mut Ctx<'_>, &mut Scripting) -> Frame,
) -> Frame {
    let mut engine = match ctx.server.scripting.take() {
        Some(engine) => engine,
        None => match Scripting::new() {
            Ok(engine) => Box::new(engine),
            Err(error) => {
                return Frame::error(format!("ERR could not start the script engine: {error}"))
            }
        },
    };
    let frame = action(ctx, &mut engine);
    ctx.server.scripting = Some(engine);
    frame
}

/// Splits `numkeys key [key ...] arg [arg ...]`. `not_a_number` is the
/// error for a `numkeys` that is not an integer.
fn keys_and_args<'a>(
    rest: &'a [Bytes],
    not_a_number: &str,
) -> Result<(&'a [Bytes], &'a [Bytes]), Frame> {
    let numkeys = int(&rest[0]).map_err(|_| Frame::error(not_a_number))?;
    if numkeys < 0 {
        return Err(Frame::error("ERR Number of keys can't be negative"));
    }
    if numkeys as u64 > (rest.len() - 1) as u64 {
        return Err(Frame::error(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    Ok(rest[1..].split_at(numkeys as usize))
}

fn eval_generic(ctx: &mut Ctx<'_>, args: &[Bytes], by_sha: bool, read_only: bool) -> Frame {
    reply((|| {
        let (keys, argv) =
            keys_and_args(&args[2..], "ERR value is not an integer or out of range")?;
        Ok(with_engine(ctx, |ctx, engine| {
            let sha = if by_sha {
                lossy(&args[1]).to_ascii_lowercase()
            } else {
                match engine.load(&args[1]) {
                    Ok(sha) => sha,
                    Err(error) => return error,
                }
            };
            engine.eval(ctx, &sha, keys, argv, read_only)
        }))
    })())
}

/// `EVAL script numkeys [key ...] [arg ...]`
pub(crate) fn eval(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    eval_generic(ctx, args, false, false)
}

pub(crate) fn eval_ro(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    eval_generic(ctx, args, false, true)
}

/// `EVALSHA sha1 numkeys [key ...] [arg ...]`
pub(crate) fn evalsha(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    eval_generic(ctx, args, true, false)
}

pub(crate) fn evalsha_ro(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    eval_generic(ctx, args, true, true)
}

/// `SCRIPT LOAD script | EXISTS sha1 [sha1 ...] | FLUSH [ASYNC | SYNC] | KILL`
pub(crate) fn script(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (sub, rest) = (&args[1], &args[2..]);
    if is(sub, "LOAD") {
        let [source] = rest else {
            return wrong_subcommand_arity("script", sub);
        };
        with_engine(ctx, |_, engine| match engine.load(source) {
            Ok(sha) => Frame::bulk(sha),
            Err(error) => error,
        })
    } else if is(sub, "EXISTS") {
        if rest.is_empty() {
            return wrong_subcommand_arity("script", sub);
        }
        let cached = |sha: &Bytes| {
            ctx.server.scripting.as_ref().is_some_and(|engine| {
                engine
                    .scripts
                    .contains_key(&lossy(sha).to_ascii_lowercase())
            })
        };
        Frame::Array(
            rest.iter()
                .map(|sha| Frame::Integer(i64::from(cached(sha))))
                .collect(),
        )
    } else if is(sub, "FLUSH") {
        match rest {
            [] => {}
            [mode] if is(mode, "ASYNC") || is(mode, "SYNC") => {}
            _ => return Frame::error("ERR SCRIPT FLUSH only support SYNC|ASYNC option"),
        }
        // Only the script cache: function libraries live in the same
        // interpreter and are not scripts.
        if let Some(engine) = &mut ctx.server.scripting {
            engine.scripts.clear();
        }
        Frame::ok()
    } else if is(sub, "KILL") {
        if !rest.is_empty() {
            return wrong_subcommand_arity("script", sub);
        }
        // A `SCRIPT KILL` aimed at a running script never gets here: the
        // server answers it while the script is still running. Reaching
        // this handler means nothing is running.
        Frame::error("NOTBUSY No scripts in execution right now.")
    } else {
        unknown_subcommand("SCRIPT", sub)
    }
}

// --- function libraries -------------------------------------------------------

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Reads the `#!lua name=<library>` line every library starts with and
/// returns the library's name.
fn library_name(code: &[u8]) -> Result<String, Frame> {
    if !code.starts_with(b"#!") {
        return Err(Frame::error("ERR Missing library metadata"));
    }
    let end = code
        .iter()
        .position(|&byte| byte == b'\n')
        .unwrap_or(code.len());
    let line = lossy(&code[2..end]);
    let mut words = line.split(' ').filter(|word| !word.is_empty());
    let engine = words.next().unwrap_or_default();
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(Frame::error(format!("ERR Engine '{engine}' not found")));
    }
    let mut name = None;
    for word in words {
        let Some(value) = word.strip_prefix("name=") else {
            return Err(Frame::error(format!(
                "ERR Invalid metadata value given: {word}"
            )));
        };
        if name.replace(value).is_some() {
            return Err(Frame::error(
                "ERR Invalid metadata value, name argument was given multiple times",
            ));
        }
    }
    let Some(name) = name else {
        return Err(Frame::error("ERR Library name was not given"));
    };
    if !valid_name(name) {
        return Err(Frame::error(ERR_NAME));
    }
    Ok(name.to_owned())
}

/// Raises an error from `redis.register_function`.
fn register_error<T>(message: &str) -> mlua::Result<T> {
    Err(LuaError::external(CommandError {
        message: format!("ERR {message}"),
    }))
}

/// Interprets the arguments of `redis.register_function`: either a name and
/// a callback, or one table of named arguments.
fn parse_registration(lua: &Lua, args: &[Value]) -> mlua::Result<LibraryFunction> {
    let text = |value: &Value| -> Option<String> {
        match lua.coerce_string(value.clone()) {
            Ok(Some(text)) => Some(text.to_string_lossy()),
            _ => None,
        }
    };
    let mut function = match args {
        [Value::Table(named)] => {
            let (mut name, mut callback, mut description, mut flags) = (None, None, None, Vec::new());
            for pair in named.pairs::<Value, Value>() {
                let (key, value) = pair?;
                match text(&key).as_deref() {
                    Some("function_name") => match text(&value) {
                        Some(value) => name = Some(value),
                        None => return register_error("function_name argument given to redis.register_function must be a string"),
                    },
                    Some("description") => match text(&value) {
                        Some(value) => description = Some(value),
                        None => return register_error("description argument given to redis.register_function must be a string"),
                    },
                    Some("callback") => match value {
                        Value::Function(function) => callback = Some(function),
                        _ => return register_error("callback argument given to redis.register_function must be a function"),
                    },
                    Some("flags") => {
                        let Value::Table(list) = value else {
                            return register_error("flags argument to redis.register_function must be a table representing function flags");
                        };
                        for flag in list.sequence_values::<Value>() {
                            match text(&flag?).as_deref() {
                                Some(
                                    flag @ ("no-writes" | "allow-oom" | "allow-stale"
                                    | "no-cluster" | "allow-cross-slot-keys"),
                                ) => flags.push(flag.to_owned()),
                                _ => return register_error("unknown flag given"),
                            }
                        }
                    }
                    _ => return register_error("unknown argument given to redis.register_function"),
                }
            }
            let Some(name) = name else {
                return register_error("redis.register_function must get a function name argument");
            };
            let Some(callback) = callback else {
                return register_error("redis.register_function must get a callback argument");
            };
            LibraryFunction {
                name,
                callback,
                description,
                flags,
            }
        }
        [_] => return register_error("calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments)."),
        [name, callback] => {
            let Some(name) = text(name) else {
                return register_error("first argument to redis.register_function must be a string");
            };
            let Value::Function(callback) = callback else {
                return register_error("second argument to redis.register_function must be a function");
            };
            LibraryFunction {
                name,
                callback: callback.clone(),
                description: None,
                flags: Vec::new(),
            }
        }
        _ => return register_error("wrong number of arguments to redis.register_function"),
    };
    if !valid_name(&function.name) {
        // Redis words this error as for a library name.
        return register_error(ERR_NAME.trim_start_matches("ERR "));
    }
    function.flags.dedup();
    Ok(function)
}

impl Scripting {
    /// Compiles and runs a library, which registers its functions, and adds
    /// it to the registry. Returns the library's name.
    ///
    /// While the library's code runs it sees almost nothing: a `redis` table
    /// with `register_function`, logging and the version. This keeps loading
    /// free of side effects. Once it has loaded, its functions (and any
    /// helpers they share) see the same globals scripts do.
    fn load_library(&mut self, code: &Bytes, replace: bool) -> Result<String, Frame> {
        let name = library_name(code)?;
        if self.libraries.contains_key(&name) && !replace {
            return Err(Frame::error(format!("ERR Library '{name}' already exists")));
        }
        let (body, _) = strip_first_line(code);
        let lua_error = |error: LuaError| Frame::error(format!("ERR {}", plain_error_text(&error)));
        let chunk = self
            .lua
            .load(&body[..])
            .set_name(LIBRARY_CHUNK_NAME)
            .set_mode(ChunkMode::Text)
            .into_function()
            .map_err(|error| {
                let text = match &error {
                    LuaError::SyntaxError { message, .. } => message.clone(),
                    other => other.to_string(),
                };
                Frame::error(format!("ERR Error compiling function: {text}"))
            })?;

        // The library's own environment: read-only, and for now backed by
        // the restricted load-time globals.
        let environment = self.lua.create_table().map_err(lua_error)?;
        let meta = self.lua.create_table().map_err(lua_error)?;
        meta.raw_set("__index", self.load_globals.clone())
            .and_then(|()| meta.raw_set("__newindex", self.readonly.clone()))
            .map_err(lua_error)?;
        environment.set_metatable(Some(meta.clone()));
        chunk.set_environment(environment).map_err(lua_error)?;

        let registered: RefCell<Vec<LibraryFunction>> = RefCell::new(Vec::new());
        let outcome = self.lua.scope(|scope| {
            let register = scope.create_function(|lua, args: Variadic<Value>| {
                let function = parse_registration(lua, &args)?;
                let mut registered = registered.borrow_mut();
                if registered
                    .iter()
                    .any(|other| other.name.eq_ignore_ascii_case(&function.name))
                {
                    return register_error("Function already exists in the library");
                }
                registered.push(function);
                Ok(())
            })?;
            self.load_api.raw_set("register_function", register)?;
            self.run.call::<(bool, Value)>(&chunk)
        });
        // Registration is only possible while loading.
        let _ = self.load_api.raw_set("register_function", Value::Nil);

        match outcome.map_err(lua_error)? {
            (true, _) => {}
            (false, error) => {
                let message = match &error {
                    Value::Table(table) => match table.raw_get::<Value>("err") {
                        Ok(Value::String(text)) => text.to_string_lossy(),
                        _ => "ERR unknown error".to_owned(),
                    },
                    _ => "ERR unknown error".to_owned(),
                };
                return Err(Frame::error(format!(
                    "ERR Error registering functions: {message}"
                )));
            }
        }
        let functions = registered.into_inner();
        if functions.is_empty() {
            return Err(Frame::error("ERR No functions registered"));
        }
        for function in &functions {
            let taken = self
                .functions
                .get(&function.name.to_ascii_lowercase())
                .is_some_and(|library| *library != name);
            if taken {
                return Err(Frame::error(format!(
                    "ERR Function {} already exists",
                    function.name
                )));
            }
        }

        // Loaded: from now on the library sees the full set of globals,
        // through the same read-only view scripts get.
        meta.raw_set("__index", self.script_globals.clone())
            .map_err(lua_error)?;
        self.remove_library(&name);
        for function in &functions {
            self.functions
                .insert(function.name.to_ascii_lowercase(), name.clone());
        }
        self.libraries.insert(
            name.clone(),
            Library {
                code: Bytes::copy_from_slice(code),
                functions,
            },
        );
        Ok(name)
    }

    fn remove_library(&mut self, name: &str) -> bool {
        let Some(library) = self.libraries.remove(name) else {
            return false;
        };
        for function in &library.functions {
            self.functions.remove(&function.name.to_ascii_lowercase());
        }
        true
    }

    fn find_function(&self, name: &[u8]) -> Option<&LibraryFunction> {
        let key = lossy(name).to_ascii_lowercase();
        let library = self.libraries.get(self.functions.get(&key)?)?;
        library
            .functions
            .iter()
            .find(|function| function.name.eq_ignore_ascii_case(&key))
    }

    /// One library as `FUNCTION LIST` shows it.
    fn describe_library(name: &str, library: &Library, with_code: bool) -> Frame {
        let functions = library
            .functions
            .iter()
            .map(|function| {
                Frame::Map(vec![
                    (Frame::bulk("name"), Frame::bulk(function.name.clone())),
                    (
                        Frame::bulk("description"),
                        function
                            .description
                            .clone()
                            .map_or(Frame::Null, Frame::bulk),
                    ),
                    (
                        Frame::bulk("flags"),
                        Frame::Set(function.flags.iter().cloned().map(Frame::bulk).collect()),
                    ),
                ])
            })
            .collect();
        let mut fields = vec![
            (Frame::bulk("library_name"), Frame::bulk(name.to_owned())),
            (Frame::bulk("engine"), Frame::bulk("LUA")),
            (Frame::bulk("functions"), Frame::Array(functions)),
        ];
        if with_code {
            fields.push((
                Frame::bulk("library_code"),
                Frame::Bulk(library.code.clone()),
            ));
        }
        Frame::Map(fields)
    }
}

/// Blanks out the first line of `source`, keeping line numbers intact.
fn strip_first_line(source: &[u8]) -> (Vec<u8>, usize) {
    let end = source
        .iter()
        .position(|&byte| byte == b'\n')
        .unwrap_or(source.len());
    let mut body = source.to_vec();
    body[..end].fill(b' ');
    (body, end)
}

fn fcall_generic(ctx: &mut Ctx<'_>, args: &[Bytes], read_only: bool) -> Frame {
    with_engine(ctx, |ctx, engine| {
        let Some(function) = engine.find_function(&args[1]) else {
            return Frame::error("ERR Function not found");
        };
        let (keys, argv) = match keys_and_args(&args[2..], "ERR Bad number of keys provided") {
            Ok(split) => split,
            Err(error) => return error,
        };
        if read_only && !function.no_writes() {
            return Frame::error(
                "ERR Can not execute a script with write flag using *_ro command.",
            );
        }
        engine.execute(
            ctx,
            &function.name,
            &function.callback,
            keys,
            argv,
            function.no_writes(),
            true,
        )
    })
}

/// `FCALL function numkeys [key ...] [arg ...]`
pub(crate) fn fcall(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    fcall_generic(ctx, args, false)
}

pub(crate) fn fcall_ro(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    fcall_generic(ctx, args, true)
}

/// `FUNCTION LOAD [REPLACE] code | DELETE library | FLUSH [ASYNC | SYNC] |
/// LIST [LIBRARYNAME pattern] [WITHCODE] | STATS | KILL`
pub(crate) fn function(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let frame = function_subcommand(ctx, args);
    // Changes to the libraries belong in the append-only file too.
    let changes = ["LOAD", "DELETE", "FLUSH", "RESTORE"]
        .iter()
        .any(|sub| is(&args[1], sub));
    if changes && !matches!(frame, Frame::Error(_)) {
        ctx.log_instead(|| vec![args.to_vec()]);
    }
    frame
}

fn function_subcommand(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (sub, rest) = (&args[1], &args[2..]);
    let arity = || wrong_subcommand_arity("function", sub);
    if is(sub, "LOAD") {
        let Some((code, options)) = rest.split_last() else {
            return arity();
        };
        let mut replace = false;
        for option in options {
            if is(option, "REPLACE") {
                replace = true;
            } else {
                return Frame::error(format!("ERR Unknown option given: {}", lossy(option)));
            }
        }
        with_engine(ctx, |ctx, engine| {
            let outcome = engine.load_library(code, replace);
            ctx.server.logs.append(&mut lock(&engine.watchdog.logs));
            match outcome {
                Ok(name) => Frame::bulk(name),
                Err(error) => error,
            }
        })
    } else if is(sub, "DELETE") {
        let [name] = rest else {
            return arity();
        };
        let removed = ctx
            .server
            .scripting
            .as_mut()
            .is_some_and(|engine| engine.remove_library(&lossy(name)));
        if removed {
            Frame::ok()
        } else {
            Frame::error("ERR Library not found")
        }
    } else if is(sub, "FLUSH") {
        match rest {
            [] => {}
            [mode] if is(mode, "ASYNC") || is(mode, "SYNC") => {}
            [_] => return Frame::error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option"),
            _ => {
                return Frame::error(
                    "ERR unknown subcommand or wrong number of arguments for 'FLUSH'. \
                     Try FUNCTION HELP.",
                )
            }
        }
        if let Some(engine) = &mut ctx.server.scripting {
            engine.libraries.clear();
            engine.functions.clear();
        }
        Frame::ok()
    } else if is(sub, "LIST") {
        let (mut pattern, mut with_code) = (None, false);
        let mut options = rest;
        loop {
            options = match options {
                [] => break,
                [option, tail @ ..] if is(option, "WITHCODE") && !with_code => {
                    with_code = true;
                    tail
                }
                [option] if is(option, "LIBRARYNAME") && pattern.is_none() => {
                    return Frame::error("ERR library name argument was not given");
                }
                [option, value, tail @ ..] if is(option, "LIBRARYNAME") && pattern.is_none() => {
                    pattern = Some(value);
                    tail
                }
                [option, ..] => {
                    return Frame::error(format!("ERR Unknown argument {}", lossy(option)));
                }
            };
        }
        let libraries = ctx.server.scripting.as_ref().map(|engine| {
            engine
                .libraries
                .iter()
                .filter(|(name, _)| {
                    pattern.is_none_or(|pattern| glob_match(pattern, name.as_bytes()))
                })
                .map(|(name, library)| Scripting::describe_library(name, library, with_code))
                .collect()
        });
        Frame::Array(libraries.unwrap_or_default())
    } else if is(sub, "STATS") {
        if !rest.is_empty() {
            return arity();
        }
        let (libraries, functions) = ctx.server.scripting.as_ref().map_or((0, 0), |engine| {
            (engine.libraries.len(), engine.functions.len())
        });
        Frame::Map(vec![
            // Nothing else runs while a function does, so no command can
            // ever see one in progress.
            (Frame::bulk("running_script"), Frame::Null),
            (
                Frame::bulk("engines"),
                Frame::Map(vec![(
                    Frame::bulk("LUA"),
                    Frame::Map(vec![
                        (
                            Frame::bulk("libraries_count"),
                            Frame::Integer(libraries as i64),
                        ),
                        (
                            Frame::bulk("functions_count"),
                            Frame::Integer(functions as i64),
                        ),
                    ]),
                )]),
            ),
        ])
    } else if is(sub, "KILL") {
        if !rest.is_empty() {
            return arity();
        }
        Frame::error("NOTBUSY No scripts in execution right now.")
    } else if is(sub, "DUMP") {
        if !rest.is_empty() {
            return arity();
        }
        let payload = match &ctx.server.scripting {
            Some(engine) => {
                rexyra_persist::rdb::dump_functions(engine.library_sources().map(|code| &code[..]))
            }
            None => rexyra_persist::rdb::dump_functions(std::iter::empty()),
        };
        Frame::Bulk(Bytes::from(payload))
    } else if is(sub, "RESTORE") {
        let (payload, policy) = match rest {
            [payload] => (payload, "APPEND"),
            [payload, policy] if is(policy, "APPEND") => (payload, "APPEND"),
            [payload, policy] if is(policy, "REPLACE") => (payload, "REPLACE"),
            [payload, policy] if is(policy, "FLUSH") => (payload, "FLUSH"),
            [_, _] => {
                return Frame::error(
                    "ERR Wrong restore policy given, value should be either FLUSH, APPEND or \
                     REPLACE.",
                )
            }
            _ => return arity(),
        };
        let Ok(libraries) = rexyra_persist::rdb::restore_functions(payload) else {
            return Frame::error("ERR DUMP payload version or checksum are wrong");
        };
        with_engine(ctx, |_, engine| {
            // Check everything before changing anything.
            let mut names = Vec::new();
            for code in &libraries {
                match library_name(code) {
                    Ok(name) => names.push(name),
                    Err(error) => return error,
                }
            }
            if policy == "APPEND" {
                if let Some(name) = names
                    .iter()
                    .find(|name| engine.libraries.contains_key(*name))
                {
                    return Frame::error(format!("ERR Library {name} already exists"));
                }
            }
            if policy == "FLUSH" {
                engine.libraries.clear();
                engine.functions.clear();
            }
            for code in &libraries {
                if let Err(error) = engine.load_library(code, policy != "APPEND") {
                    return error;
                }
            }
            Frame::ok()
        })
    } else {
        unknown_subcommand("FUNCTION", sub)
    }
}

/// Loads function libraries read from a snapshot, starting the script
/// engine if there are any.
pub(crate) fn restore_libraries(
    server: &mut crate::Server,
    libraries: &[Bytes],
) -> Result<(), String> {
    if libraries.is_empty() {
        return Ok(());
    }
    let frame_text = |frame: Frame| match frame {
        Frame::Error(text) => text,
        other => format!("{other:?}"),
    };
    let mut engine = match server.scripting.take() {
        Some(engine) => engine,
        None => Box::new(
            Scripting::new()
                .map_err(|error| format!("could not start the script engine: {error}"))?,
        ),
    };
    let outcome = libraries.iter().try_for_each(|code| {
        engine
            .load_library(code, true)
            .map(|_| ())
            .map_err(frame_text)
    });
    server.logs.append(&mut lock(&engine.watchdog.logs));
    server.scripting = Some(engine);
    outcome
}

impl Scripting {
    /// The source of every loaded function library, in order of name.
    pub fn library_sources(&self) -> impl Iterator<Item = &Bytes> {
        self.libraries.values().map(|library| &library.code)
    }

    /// The source of every cached script, for persistence later on.
    pub fn sources(&self) -> impl Iterator<Item = &Bytes> {
        self.scripts.values().map(|script| &script.source)
    }
}

const _: fn() = || {
    // The interpreter moves to the execution thread with the server state.
    fn assert_send<T: Send>() {}
    assert_send::<Scripting>();
};

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness};
    use rexyra_resp::{Frame, Protocol};

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    /// Runs `EVAL script 0`.
    fn eval(h: &mut Harness, script: &str) -> Frame {
        h.cmd(&["EVAL", script, "0"])
    }

    fn sha(script: &str) -> String {
        super::sha1_hex(script.as_bytes())
    }

    // The three scripts from the project this server is meant to serve.
    const SET_ADD_IF_ABSENT: &str = "\
        if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then
            return 0
        end
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return 1";
    const INCREMENT_WINDOW: &str = "\
        local total = redis.call('INCRBY', KEYS[1], ARGV[1])
        if total == tonumber(ARGV[1]) then
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        return total";
    const TRY_TAKE: &str = "\
        local current = redis.call('GET', KEYS[1])
        if not current then
            return false
        end
        if tonumber(current) < tonumber(ARGV[1]) then
            return -1
        end
        return redis.call('DECRBY', KEYS[1], ARGV[1])";

    #[test]
    fn set_add_if_absent_script() {
        let mut h = Harness::new();
        let run = |h: &mut Harness, member: &str| {
            h.cmd(&["EVAL", SET_ADD_IF_ABSENT, "1", "seen", member, "60000"])
        };
        assert_eq!(run(&mut h, "a"), int(1));
        assert_eq!(h.run("PTTL seen"), int(60_000));
        h.advance(10_000);
        // Already present: nothing changes, including the TTL.
        assert_eq!(run(&mut h, "a"), int(0));
        assert_eq!(h.run("PTTL seen"), int(50_000));
        assert_eq!(run(&mut h, "b"), int(1));
        assert_eq!(h.run("PTTL seen"), int(60_000));
        assert_eq!(h.run("SCARD seen"), int(2));
    }

    #[test]
    fn increment_window_script() {
        let mut h = Harness::new();
        let run = |h: &mut Harness, by: &str| {
            h.cmd(&["EVAL", INCREMENT_WINDOW, "1", "window", by, "60000"])
        };
        assert_eq!(run(&mut h, "5"), int(5));
        assert_eq!(h.run("PTTL window"), int(60_000));
        h.advance(20_000);
        // Later increments do not move the window.
        assert_eq!(run(&mut h, "5"), int(10));
        assert_eq!(h.run("PTTL window"), int(40_000));
        h.advance(40_000);
        // The window has passed; a new one starts.
        assert_eq!(run(&mut h, "2"), int(2));
        assert_eq!(h.run("PTTL window"), int(60_000));
    }

    #[test]
    fn try_take_script() {
        let mut h = Harness::new();
        let run =
            |h: &mut Harness, amount: &str| h.cmd(&["EVAL", TRY_TAKE, "1", "balance", amount]);
        assert_eq!(run(&mut h, "3"), Frame::Null);
        assert_eq!(h.run("EXISTS balance"), int(0));
        h.run("SET balance 10");
        assert_eq!(run(&mut h, "3"), int(7));
        assert_eq!(run(&mut h, "8"), int(-1));
        assert_eq!(h.run("GET balance"), bulk("7"));
        assert_eq!(run(&mut h, "7"), int(0));
    }

    #[test]
    fn lua_values_become_replies() {
        let mut h = Harness::new();
        for (script, expected) in [
            ("return 1", int(1)),
            ("return 'text'", bulk("text")),
            ("return 3.99", int(3)),
            ("return -3.99", int(-3)),
            ("return true", int(1)),
            ("return false", Frame::Null),
            ("return nil", Frame::Null),
            ("return", Frame::Null),
            ("", Frame::Null),
            ("return {ok='FINE'}", Frame::simple("FINE")),
            ("return redis.status_reply('PONG')", Frame::simple("PONG")),
            ("return {err='My Error'}", err("My Error")),
            ("return redis.error_reply('My Error')", err("My Error")),
            ("return redis.error_reply('Oops')", err("ERR Oops")),
            ("return {double=2.5}", Frame::Double(2.5)),
            ("return {a=1}", Frame::Array(vec![])),
            ("return 1e300", int(i64::MIN)),
        ] {
            assert_eq!(eval(&mut h, script), expected, "{script}");
        }
        assert_eq!(
            eval(&mut h, "return {1, 'two', {3, {}}, nil, 5}"),
            Frame::Array(vec![
                int(1),
                bulk("two"),
                Frame::Array(vec![int(3), Frame::Array(vec![])]),
            ])
        );
    }

    #[test]
    fn replies_become_lua_values() {
        let mut h = Harness::new();
        h.run("SET k v");
        h.run("RPUSH l a b");
        h.run("HSET h f v");
        h.run("ZADD z 1.5 m");
        for (script, expected) in [
            ("return type(redis.call('GET', 'k'))", bulk("string")),
            // Null is `false`, so `if not value` works.
            ("return redis.call('GET', 'nokey') == false", int(1)),
            ("return type(redis.call('INCR', 'n'))", bulk("number")),
            ("return redis.call('SET', 'k', 'v').ok", bulk("OK")),
            (
                "return redis.call('LRANGE', 'l', 0, -1)",
                Frame::Array(bulks(&["a", "b"])),
            ),
            (
                "return redis.call('HGETALL', 'h')",
                Frame::Array(bulks(&["f", "v"])),
            ),
            ("return redis.call('ZSCORE', 'z', 'm')", bulk("1.5")),
            (
                "return redis.call('MGET', 'k', 'nokey')[2] == false",
                int(1),
            ),
        ] {
            assert_eq!(eval(&mut h, script), expected, "{script}");
        }
    }

    #[test]
    fn numbers_keep_their_precision_as_arguments() {
        let mut h = Harness::new();
        eval(&mut h, "redis.call('SET', 'third', 1/3)");
        assert_eq!(h.run("GET third"), bulk("0.3333333333333333"));
        eval(&mut h, "redis.call('SET', 'whole', 2.0)");
        assert_eq!(h.run("GET whole"), bulk("2"));
        eval(&mut h, "redis.call('SET', 'big', 123456789012)");
        assert_eq!(h.run("GET big"), bulk("123456789012"));
    }

    #[test]
    fn keys_and_argv() {
        let mut h = Harness::new();
        assert_eq!(
            h.cmd(&[
                "EVAL",
                "return {KEYS[1], KEYS[2], ARGV[1], #KEYS, #ARGV}",
                "2",
                "k1",
                "k2",
                "a1"
            ]),
            Frame::Array(vec![bulk("k1"), bulk("k2"), bulk("a1"), int(2), int(1)])
        );
        // Nothing is left over from the previous call.
        assert_eq!(
            eval(&mut h, "return {#KEYS, #ARGV}"),
            Frame::Array(vec![int(0), int(0)])
        );
        for (numkeys, message) in [
            ("x", "ERR value is not an integer or out of range"),
            ("-1", "ERR Number of keys can't be negative"),
            (
                "3",
                "ERR Number of keys can't be greater than number of args",
            ),
        ] {
            assert_eq!(
                h.cmd(&["EVAL", "return 1", numkeys, "a"]),
                err(message),
                "{numkeys}"
            );
        }
    }

    #[test]
    fn command_errors_end_the_script_unless_caught() {
        let mut h = Harness::new();
        h.run("SET k text");
        let script = "redis.call('SET', 'before', 1)\nreturn redis.call('INCR', 'k')";
        assert_eq!(
            eval(&mut h, script),
            err(&format!(
                "ERR value is not an integer or out of range script: {}, on @user_script:2.",
                sha(script)
            ))
        );
        // What ran before the error stays done, as in Redis.
        assert_eq!(h.run("GET before"), bulk("1"));

        assert_eq!(
            eval(&mut h, "return redis.pcall('INCR', 'k')"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            eval(&mut h, "local r = redis.pcall('INCR', 'k') return r.err"),
            bulk("ERR value is not an integer or out of range")
        );
        // Lua's own pcall sees the message as a string.
        assert_eq!(
            eval(
                &mut h,
                "local ok, e = pcall(redis.call, 'INCR', 'k') return {tostring(ok), type(e), e}"
            ),
            Frame::Array(bulks(&[
                "false",
                "string",
                "ERR value is not an integer or out of range"
            ]))
        );
    }

    #[test]
    fn commands_a_script_may_not_run() {
        let mut h = Harness::new();
        for (call, message) in [
            (
                "redis.call('NOSUCH')",
                "ERR Unknown Redis command called from script",
            ),
            (
                "redis.call('GET')",
                "ERR Wrong number of args calling Redis command from script",
            ),
            (
                "redis.call()",
                "ERR Please specify at least one argument for this redis lib call",
            ),
            (
                "redis.call('GET', {})",
                "ERR Lua redis lib command arguments must be strings or integers",
            ),
            (
                "redis.call('EVAL', 'return 1', 0)",
                "ERR This Redis command is not allowed from script",
            ),
            (
                "redis.call('MULTI')",
                "ERR This Redis command is not allowed from script",
            ),
        ] {
            let script = format!("return {call}");
            assert_eq!(
                eval(&mut h, &script),
                err(&format!(
                    "{message} script: {}, on @user_script:1.",
                    sha(&script)
                )),
                "{call}"
            );
        }
    }

    #[test]
    fn lua_errors_report_where_they_happened() {
        let mut h = Harness::new();
        for (script, message, line) in [
            ("error('boom')", "ERR user_script:1: boom", 1),
            ("\n\nerror('boom', 0)", "ERR boom", 3),
            ("error({err='MINE custom'})", "MINE custom", 1),
            ("error({1})", "ERR unknown error", 1),
            ("error()", "ERR nil", 1),
            (
                "local t = nil\nreturn t.field",
                "ERR user_script:2: attempt to index local 't' (a nil value)",
                2,
            ),
            (
                "return nosuchvar",
                "ERR user_script:1: Script attempted to access nonexistent global variable 'nosuchvar'",
                1,
            ),
            (
                "\nleak = 1",
                "ERR user_script:2: Attempt to modify a readonly table",
                2,
            ),
        ] {
            assert_eq!(
                eval(&mut h, script),
                err(&format!(
                    "{message} script: {}, on @user_script:{line}.",
                    sha(script)
                )),
                "{script}"
            );
        }
        assert_eq!(
            eval(&mut h, "return 1 +"),
            err("ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'")
        );
        // The interpreter is fine afterwards.
        assert_eq!(eval(&mut h, "return 'ok'"), bulk("ok"));
    }

    /// The error of a script that failed on its first line.
    fn script_error(script: &str, message: &str) -> Frame {
        err(&format!(
            "{message} script: {}, on @user_script:1.",
            sha(script)
        ))
    }

    #[test]
    fn precompiled_chunks_are_refused() {
        // Lua 5.1 does not verify bytecode, so loading a crafted chunk is a
        // way to corrupt memory. Redis parses everything as source text.
        let mut h = Harness::new();
        assert_eq!(
            eval(&mut h, "\u{1b}LuaQ"),
            err("ERR Error compiling script (new function): user_script:1: unexpected symbol near 'char(27)'")
        );
        assert_eq!(
            eval(
                &mut h,
                "local f, e = loadstring(string.dump(function() return 42 end)) \
                 return {tostring(f), e}"
            ),
            Frame::Array(bulks(&[
                "nil",
                "[string \"\u{1b}LuaQ\"]:1: unexpected symbol near 'char(27)'"
            ]))
        );
        assert_eq!(
            eval(
                &mut h,
                "local code = string.dump(function() return 42 end) \
                 local f, e = load(function() local c = code code = nil return c end) \
                 return {tostring(f), e}"
            ),
            Frame::Array(bulks(&[
                "nil",
                "(load):1: unexpected symbol near 'char(27)'"
            ]))
        );
        // Source text still loads, in one piece or several.
        assert_eq!(eval(&mut h, "return loadstring('return 7')()"), int(7));
        assert_eq!(
            eval(
                &mut h,
                "local parts = {'return ', '4', '2'} local i = 0 \
                 return load(function() i = i + 1 return parts[i] end)()"
            ),
            int(42)
        );
        assert_eq!(
            eval(&mut h, "local f, e = loadstring('return 1 +') return e"),
            bulk("[string \"return 1 +\"]:1: unexpected symbol near '<eof>'")
        );
    }

    #[test]
    fn a_script_cannot_change_what_later_scripts_see() {
        // One interpreter serves every client, so anything a script could
        // replace would run later with another caller's permissions.
        let mut h = Harness::new();
        let plant = "function() return 'planted' end";
        for target in [
            "string.upper",
            "tostring",
            "_G.tonumber",
            "table.concat",
            "math.floor",
            "redis.call",
            "redis.sha1hex",
            "cjson.encode",
            "cmsgpack.pack",
            "struct.pack",
            "bit.band",
            "os.clock",
            "coroutine.wrap",
            "getmetatable('').__index.rep",
            "getmetatable('').__index",
            "getmetatable(_G).__index",
            "KEYS",
        ] {
            let script = format!("{target} = {plant} return 1");
            assert_eq!(
                eval(&mut h, &script),
                script_error(
                    &script,
                    "ERR user_script:1: Attempt to modify a readonly table"
                ),
                "{target}"
            );
        }
        for script in [
            "rawset(_G, 'type', 1) return 1",
            "rawset(table, 'concat', 1) return 1",
            "rawset(redis, 'call', 1) return 1",
            "setmetatable(_G, {}) return 1",
            "setmetatable(string, {}) return 1",
        ] {
            assert_eq!(
                eval(&mut h, script),
                script_error(script, "ERR Attempt to modify a readonly table"),
                "{script}"
            );
        }
        // Code compiled by a script is held to the same rules.
        for script in [
            "loadstring('tostring = 1')() return 1",
            "loadstring('leak = 1')() return 1",
            "local done = false \
             load(function() if done then return nil end done = true return 'select = 1' end)() \
             return 1",
        ] {
            assert!(
                matches!(eval(&mut h, script), Frame::Error(text) if text.contains("Attempt to modify a readonly table")),
                "{script}"
            );
        }
        // The ways to reach the real table of globals are gone, as in Redis.
        for name in ["getfenv", "setfenv", "newproxy"] {
            let script = format!("return type({name})");
            assert!(
                matches!(eval(&mut h, &script), Frame::Error(text) if text.contains("nonexistent global")),
                "{name}"
            );
        }

        // Nothing was changed, and reading works as before.
        assert_eq!(
            eval(
                &mut h,
                "return {string.upper('a'), tostring(1), tostring(tonumber('2')), \
                 table.concat({'a', 'b'}), ('a'):rep(2), tostring(('abc'):len()), \
                 cjson.encode(1), type(1), redis.call('PING').ok, tostring(select('#', 1, 2))}"
            ),
            Frame::Array(bulks(&[
                "A", "1", "2", "ab", "aa", "3", "1", "number", "PONG", "2"
            ]))
        );
        assert_eq!(
            eval(&mut h, "return type(nosuchvar)"),
            script_error(
                "return type(nosuchvar)",
                "ERR user_script:1: Script attempted to access nonexistent global variable 'nosuchvar'"
            )
        );
        assert_eq!(
            eval(
                &mut h,
                "local names = {} for name in pairs(string) do names[#names + 1] = name end \
                 table.sort(names) return table.concat(names, ' ')"
            ),
            bulk("byte char dump find format gfind gmatch gsub len lower match rep reverse sub upper")
        );
        assert_eq!(
            eval(
                &mut h,
                "local seen = 0 for name in pairs(_G) do \
                 if name == 'redis' or name == 'KEYS' or name == 'pcall' then seen = seen + 1 end \
                 end return seen"
            ),
            int(3)
        );
        assert_eq!(
            eval(
                &mut h,
                "return {type(rawget(string, 'upper')), type(rawget(_G, 'redis')), \
                 type(next(math)), tostring(getmetatable(string)), tostring(_G._G == _G), \
                 tostring(rawequal(_G.string, string))}"
            ),
            Frame::Array(bulks(&[
                "function", "table", "string", "nil", "true", "true"
            ]))
        );
        // A script's own tables are its own.
        assert_eq!(
            eval(
                &mut h,
                "local t = setmetatable({}, {__index = function() return 5 end}) \
                 rawset(t, 'a', 1) t.b = 2 \
                 return {t.a, t.b, t.c, type(getmetatable(t).__index)}"
            ),
            Frame::Array(vec![int(1), int(2), int(5), bulk("function")])
        );
        assert_eq!(
            h.cmd(&["EVAL", "return {KEYS[1], ARGV[1]}", "1", "k", "a"]),
            Frame::Array(bulks(&["k", "a"]))
        );
    }

    #[test]
    fn functions_cannot_change_what_scripts_see_either() {
        let mut h = Harness::new();
        let code = "#!lua name=lib\n\
                    redis.register_function('plant', function() \
                      string.upper = function() return 'planted' end return 1 end)\n\
                    redis.register_function('plant_global', function() \
                      _G.tostring = function() return 'planted' end return 1 end)";
        assert_eq!(h.cmd(&["FUNCTION", "LOAD", code]), bulk("lib"));
        for name in ["plant", "plant_global"] {
            assert!(
                matches!(h.cmd(&["FCALL", name, "0"]), Frame::Error(text) if text.contains("Attempt to modify a readonly table")),
                "{name}"
            );
        }
        assert_eq!(
            eval(&mut h, "return {string.upper('a'), tostring(1)}"),
            Frame::Array(bulks(&["A", "1"]))
        );
    }

    #[test]
    fn unpacking_deeply_nested_messagepack_is_an_error() {
        let mut h = Harness::new();
        // 0x91 is an array of one element: a hundred of them around a 1.
        assert_eq!(
            eval(
                &mut h,
                "local t = cmsgpack.unpack(string.rep('\\145', 100) .. '\\1') local depth = 0 \
                 while type(t) == 'table' do t = t[1] depth = depth + 1 end return depth"
            ),
            int(100)
        );
        assert_eq!(
            eval(
                &mut h,
                "local ok, e = pcall(cmsgpack.unpack, string.rep('\\145', 200000)) \
                 return {tostring(ok), e}"
            ),
            Frame::Array(bulks(&[
                "false",
                "stack overflow (in function mp_decode_to_lua_array)"
            ]))
        );
        assert_eq!(
            eval(
                &mut h,
                "local ok, e = pcall(cmsgpack.unpack, string.rep('\\129\\1', 200000)) \
                 return {tostring(ok), e}"
            ),
            Frame::Array(bulks(&[
                "false",
                "stack overflow (in function mp_decode_to_lua_hash)"
            ]))
        );
    }

    #[test]
    fn the_sandbox_hides_the_host() {
        let mut h = Harness::new();
        for name in [
            "dofile", "loadfile", "print", "require", "io", "package", "debug",
        ] {
            let script = format!("return type({name})");
            assert!(
                matches!(eval(&mut h, &script), Frame::Error(text) if text.contains("nonexistent global")),
                "{name}"
            );
        }
        assert_eq!(eval(&mut h, "return type(os.clock())"), bulk("number"));
        assert_eq!(
            eval(
                &mut h,
                "return {type(os.execute), type(os.getenv), type(os.remove)}"
            ),
            Frame::Array(bulks(&["nil", "nil", "nil"]))
        );
        // The standard libraries scripts rely on are there.
        assert_eq!(
            eval(
                &mut h,
                "return {string.upper('a'), math.floor(2.5), table.concat({1,2}, '-'), \
                 tostring(tonumber('7') + 1), select('#', 1, 2), #{unpack({1,2,3})}}"
            ),
            Frame::Array(vec![
                bulk("A"),
                int(2),
                bulk("1-2"),
                bulk("8"),
                int(2),
                int(3)
            ])
        );
    }

    #[test]
    fn script_cache() {
        let mut h = Harness::new();
        let script = "return ARGV[1]";
        let digest = sha(script);
        assert_eq!(
            h.cmd(&["EVALSHA", &digest, "0", "x"]),
            err("NOSCRIPT No matching script. Please use EVAL.")
        );
        assert_eq!(h.cmd(&["SCRIPT", "LOAD", script]), bulk(&digest));
        assert_eq!(h.cmd(&["EVALSHA", &digest, "0", "x"]), bulk("x"));
        // The digest is not case-sensitive.
        assert_eq!(
            h.cmd(&["EVALSHA", &digest.to_uppercase(), "0", "y"]),
            bulk("y")
        );
        // EVAL caches too.
        let other = "return 2";
        eval(&mut h, other);
        assert_eq!(
            h.cmd(&["SCRIPT", "EXISTS", &digest, &sha(other), &sha("return 3")]),
            Frame::Array(vec![int(1), int(1), int(0)])
        );
        assert_eq!(h.run("SCRIPT FLUSH"), Frame::ok());
        assert_eq!(
            h.cmd(&["SCRIPT", "EXISTS", &digest]),
            Frame::Array(vec![int(0)])
        );
        assert_eq!(
            h.cmd(&["SCRIPT", "LOAD", "return 1 +"]),
            err("ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'")
        );
        assert_eq!(
            h.run("SCRIPT KILL"),
            err("NOTBUSY No scripts in execution right now.")
        );
        assert_eq!(
            h.run("SCRIPT FLUSH NOW"),
            err("ERR SCRIPT FLUSH only support SYNC|ASYNC option")
        );
    }

    #[test]
    fn read_only_scripts_cannot_write() {
        let mut h = Harness::new();
        h.run("SET k v");
        assert_eq!(
            h.cmd(&["EVAL_RO", "return redis.call('GET', 'k')", "0"]),
            bulk("v")
        );
        let write = "return redis.call('SET', 'k', 'changed')";
        let refused = "ERR Write commands are not allowed from read-only scripts.";
        assert_eq!(
            h.cmd(&["EVAL_RO", write, "0"]),
            err(&format!(
                "{refused} script: {}, on @user_script:1.",
                sha(write)
            ))
        );
        let declared = "#!lua flags=no-writes\nreturn redis.call('SET', 'k', 'changed')";
        assert_eq!(
            eval(&mut h, declared),
            err(&format!(
                "{refused} script: {}, on @user_script:2.",
                sha(declared)
            ))
        );
        assert_eq!(h.run("GET k"), bulk("v"));
        assert_eq!(eval(&mut h, "#!lua\nreturn 1"), int(1));
        assert_eq!(
            eval(&mut h, "#!python\nreturn 1"),
            err("ERR Unexpected engine in script shebang: #!python")
        );
    }

    #[test]
    fn scripts_use_resp2_whatever_the_client_speaks() {
        let mut h = Harness::new();
        h.run("HELLO 3");
        h.run("HSET h f v");
        h.run("ZADD z 1 a");
        // A RESP3 client would get a map from HGETALL; the script gets a
        // flat array, and the client's protocol is untouched afterwards.
        assert_eq!(
            eval(&mut h, "return redis.call('HGETALL', 'h')"),
            Frame::Array(bulks(&["f", "v"]))
        );
        assert_eq!(
            eval(&mut h, "return redis.call('ZPOPMIN', 'z', 1)"),
            Frame::Array(bulks(&["a", "1"]))
        );
        assert_eq!(h.client.proto, Protocol::Resp3);
        // After setresp(3) the script sees the RESP3 shapes.
        assert_eq!(
            eval(
                &mut h,
                "redis.setresp(3) return redis.call('HGETALL', 'h').map.f"
            ),
            bulk("v")
        );
        assert_eq!(
            eval(&mut h, "redis.setresp(3) return {true, false}"),
            Frame::Array(vec![Frame::Boolean(true), Frame::Boolean(false)])
        );
        assert_eq!(h.client.proto, Protocol::Resp3);
        // The switch does not carry over to the next script.
        assert_eq!(eval(&mut h, "return true"), int(1));
    }

    #[test]
    fn scripts_take_part_in_transactions_and_watch() {
        let mut h = Harness::new();
        h.run("MULTI");
        assert_eq!(
            eval(&mut h, "return redis.call('INCR', 'n')"),
            Frame::simple("QUEUED")
        );
        assert_eq!(
            eval(&mut h, "return redis.call('INCR', 'n')"),
            Frame::simple("QUEUED")
        );
        assert_eq!(h.run("EXEC"), Frame::Array(vec![int(1), int(2)]));

        // A write made by a script counts as a change to a watched key.
        h.run("WATCH n");
        eval(&mut h, "redis.call('SET', 'n', 0)");
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
    }

    #[test]
    fn blocking_commands_do_not_wait_inside_a_script() {
        let mut h = Harness::new();
        assert_eq!(
            eval(&mut h, "return redis.call('BLPOP', 'nolist', 0) == false"),
            int(1)
        );
        assert_eq!(h.block, None);
    }

    #[test]
    fn commands_run_by_scripts_are_counted() {
        let mut h = Harness::new();
        eval(
            &mut h,
            "redis.call('SET', 'a', 1) redis.call('SET', 'b', 2) return 1",
        );
        let stats: Vec<_> = h.server.stats.commands().collect();
        let calls = |name: &str| {
            stats
                .iter()
                .find(|(command, _)| *command == name)
                .map(|(_, stat)| stat.calls)
        };
        assert_eq!(calls("set"), Some(2));
        assert_eq!(calls("eval"), Some(1));
    }

    /// Stands in for the server's mailbox: records what the script engine
    /// asked and requests a kill whenever that is allowed.
    #[derive(Default)]
    struct KillWhenAllowed {
        asked: std::sync::Mutex<Vec<bool>>,
    }

    impl crate::BusyPoll for KillWhenAllowed {
        fn poll(&self, killable: bool) -> bool {
            self.asked.lock().unwrap().push(killable);
            killable
        }
    }

    /// A harness whose scripts are over the time limit almost at once.
    fn impatient() -> (Harness, std::sync::Arc<KillWhenAllowed>) {
        let mut h = Harness::new();
        let poll = std::sync::Arc::new(KillWhenAllowed::default());
        h.server.busy_poll = Some(poll.clone());
        h.server.busy_reply_threshold = 1;
        (h, poll)
    }

    #[test]
    fn a_runaway_script_can_be_killed() {
        let (mut h, poll) = impatient();
        let script = "local n = 0\nwhile true do n = n + 1 end";
        assert_eq!(
            eval(&mut h, script),
            err(&format!(
                "ERR Script killed by user with SCRIPT KILL... script: {}, on @user_script:2.",
                sha(script)
            ))
        );
        assert_eq!(*poll.asked.lock().unwrap(), [true]);
        // The interpreter is usable again, and the next script is not
        // affected by the earlier kill.
        assert_eq!(eval(&mut h, "return 'still here'"), bulk("still here"));
    }

    #[test]
    fn a_killed_script_cannot_hide_in_pcall() {
        let (mut h, _) = impatient();
        // Catching the kill once is allowed, as in Redis ...
        assert_eq!(
            eval(
                &mut h,
                "local ok, e = pcall(function() while true do end end) return {tostring(ok), e}"
            ),
            Frame::Array(bulks(&[
                "false",
                "ERR Script killed by user with SCRIPT KILL..."
            ]))
        );
        // ... but looping around the protected call does not keep it alive.
        for script in [
            "while true do pcall(function() while true do end end) end",
            "while true do xpcall(function() while true do end end, function(e) return e end) end",
            "pcall(function() while true do end end) while true do end",
            "pcall(function() while true do end end) return redis.call('PING')",
        ] {
            assert_eq!(
                eval(&mut h, script),
                err(&format!(
                    "ERR Script killed by user with SCRIPT KILL... script: {}, on @user_script:1.",
                    sha(script)
                )),
                "{script}"
            );
        }
    }

    #[test]
    fn a_script_that_has_written_cannot_be_killed() {
        let (mut h, poll) = impatient();
        assert_eq!(
            eval(
                &mut h,
                "redis.call('SET', 'k', 'v') local n = 0 while n < 3000000 do n = n + 1 end return n"
            ),
            int(3_000_000)
        );
        let asked = poll.asked.lock().unwrap();
        assert!(!asked.is_empty());
        assert!(asked.iter().all(|killable| !killable), "{asked:?}");
    }

    #[test]
    fn the_time_limit_can_be_turned_off() {
        let (mut h, poll) = impatient();
        h.run("CONFIG SET busy-reply-threshold 0");
        assert_eq!(
            eval(
                &mut h,
                "local n = 0 while n < 3000000 do n = n + 1 end return n"
            ),
            int(3_000_000)
        );
        assert!(poll.asked.lock().unwrap().is_empty());
        assert_eq!(
            h.run("CONFIG GET lua-time-limit"),
            Frame::Map(vec![(bulk("lua-time-limit"), bulk("0"))])
        );
    }

    #[test]
    fn redis_log_lines_reach_the_server() {
        let mut h = Harness::new();
        eval(
            &mut h,
            "redis.log(redis.LOG_WARNING, 'low', 'stock', 3) redis.log(redis.LOG_DEBUG, 'detail')",
        );
        assert_eq!(
            h.server.logs,
            [(3, "low stock 3".to_owned()), (0, "detail".to_owned())]
        );
        let bad = "redis.log(7, 'x')";
        assert_eq!(
            eval(&mut h, bad),
            err(&format!(
                "ERR Invalid log level. script: {}, on @user_script:1.",
                sha(bad)
            ))
        );
    }

    #[test]
    fn script_libraries() {
        let mut h = Harness::new();
        for (script, expected) in [
            ("return bit.band(0xff, 0x0f)", int(15)),
            ("return bit.tohex(255, 4)", bulk("00ff")),
            ("return bit.bnot(0)", int(-1)),
            (
                "return cjson.encode({1, 'two', {a = true}})",
                bulk(r#"[1,"two",{"a":true}]"#),
            ),
            ("return cjson.encode({})", bulk("{}")),
            ("return cjson.decode('{\"a\":[10,20]}').a[2]", int(20)),
            ("return cjson.decode('null') == cjson.null", int(1)),
            ("return cjson.encode(1/3)", bulk("0.33333333333333")),
            ("return struct.size('<ihb')", int(7)),
            (
                "return {struct.unpack('>I2', struct.pack('>I2', 513))}",
                Frame::Array(vec![int(513), int(3)]),
            ),
            (
                "return {cmsgpack.unpack(cmsgpack.pack(1, 'two', {3}))}",
                Frame::Array(vec![int(1), bulk("two"), Frame::Array(vec![int(3)])]),
            ),
            ("return #cmsgpack.pack({1, 2, 3})", int(4)),
        ] {
            assert_eq!(eval(&mut h, script), expected, "{script}");
        }
        let bad = "return cjson.decode('[1,')";
        assert_eq!(
            eval(&mut h, bad),
            err(&format!(
                "ERR user_script:1: Expected value but found T_END at character 4 script: {}, on @user_script:1.",
                sha(bad)
            ))
        );
    }

    #[test]
    fn random_numbers_follow_redis() {
        let mut h = Harness::new();
        // The sequence real Redis produces from the same seed.
        assert_eq!(
            eval(
                &mut h,
                "math.randomseed(0) return {math.random(100), math.random(100), math.random(1, 6), \
                 tostring(math.random()), math.random(100)}"
            ),
            Frame::Array(vec![
                int(18),
                int(75),
                int(1),
                bulk("0.87046522734243"),
                int(58)
            ])
        );
    }

    #[test]
    fn globals_cannot_be_created_behind_the_scenes() {
        let mut h = Harness::new();
        let script = "rawset(_G, 'leak', 1)";
        assert_eq!(
            eval(&mut h, script),
            err(&format!(
                "ERR Attempt to modify a readonly table script: {}, on @user_script:1.",
                sha(script)
            ))
        );
        assert_eq!(
            eval(&mut h, "local t = {} rawset(t, 'k', 'v') return t.k"),
            bulk("v")
        );
    }

    /// A library in the style of the Redis documentation: two functions
    /// sharing a helper that validates its input.
    const HASH_LIBRARY: &str = "#!lua name=mylib

local function check_keys(keys)
  local error = nil
  local nkeys = table.getn(keys)
  if nkeys == 0 then
    error = 'Hash key name not provided'
  elseif nkeys > 1 then
    error = 'Only one key name is allowed'
  end
  if error ~= nil then
    redis.log(redis.LOG_WARNING, error)
    return redis.error_reply(error)
  end
  return nil
end

local function my_hset(keys, args)
  local error = check_keys(keys)
  if error ~= nil then
    return error
  end
  local time = redis.call('TIME')[1]
  return redis.call('HSET', keys[1], '_last_modified_', time, unpack(args))
end

local function my_hgetall(keys, args)
  local error = check_keys(keys)
  if error ~= nil then
    return error
  end
  redis.setresp(3)
  local hash = redis.call('HGETALL', keys[1])
  hash['map']['_last_modified_'] = nil
  return hash
end

redis.register_function('my_hset', my_hset)
redis.register_function{
  function_name = 'my_hgetall',
  callback = my_hgetall,
  flags = { 'no-writes' },
  description = 'HGETALL without the bookkeeping field',
}";

    fn load(h: &mut Harness, code: &str) -> Frame {
        h.cmd(&["FUNCTION", "LOAD", code])
    }

    #[test]
    fn a_library_with_shared_helpers() {
        let mut h = Harness::new();
        assert_eq!(load(&mut h, HASH_LIBRARY), bulk("mylib"));
        assert_eq!(h.run("FCALL my_hset 1 myhash first John last Doe"), int(3));
        assert_eq!(
            h.run("FCALL_RO my_hgetall 1 myhash"),
            Frame::Map(vec![
                (bulk("first"), bulk("John")),
                (bulk("last"), bulk("Doe"))
            ])
        );
        // The helper runs with the full API although it was defined while
        // the library was loading under the restricted one.
        assert_eq!(
            h.run("FCALL my_hset 0 field value"),
            err("Hash key name not provided")
        );
        assert_eq!(
            h.server.logs.last(),
            Some(&(3, "Hash key name not provided".to_owned()))
        );
        // Function names are not case-sensitive.
        assert_eq!(h.run("FCALL MY_HSET 1 other f v"), int(2));
    }

    #[test]
    fn a_library_sees_almost_nothing_while_loading() {
        let mut h = Harness::new();
        for (body, missing) in [
            ("redis.call('SET', 'k', 'v')", "call"),
            ("local s = string.upper('a')", "string"),
            ("local x = KEYS", "KEYS"),
            ("error('stop')", "error"),
        ] {
            let code =
                format!("#!lua name=probe\n{body}\nredis.register_function('f', function() end)");
            assert_eq!(
                load(&mut h, &code),
                err(&format!(
                    "ERR Error registering functions: ERR user_function:2: Script attempted to \
                     access nonexistent global variable '{missing}'"
                )),
                "{body}"
            );
        }
        assert_eq!(
            load(&mut h, "#!lua name=probe\nleak = 1"),
            err("ERR Error registering functions: ERR user_function:2: Attempt to modify a readonly table")
        );
        // Nothing ran and nothing was registered.
        assert_eq!(h.run("EXISTS k"), int(0));
        assert_eq!(h.run("FUNCTION LIST"), Frame::Array(vec![]));
        // Logging and the version are available.
        assert_eq!(
            load(
                &mut h,
                "#!lua name=probe\nredis.log(redis.LOG_NOTICE, 'loading', redis.REDIS_VERSION)\n\
                 redis.register_function('f', function() return 1 end)"
            ),
            bulk("probe")
        );
        assert_eq!(h.server.logs, [(2, "loading 7.4.0".to_owned())]);
    }

    #[test]
    fn library_metadata_and_registration_errors() {
        let mut h = Harness::new();
        let ok = "redis.register_function('f', function() return 1 end)";
        for (code, message) in [
            (ok.to_owned(), "ERR Missing library metadata"),
            (format!("#!lua\n{ok}"), "ERR Library name was not given"),
            (format!("#!python name=p\n{ok}"), "ERR Engine 'python' not found"),
            (
                format!("#!lua name=bad-name\n{ok}"),
                "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
            ),
            (
                format!("#!lua name=a foo=bar\n{ok}"),
                "ERR Invalid metadata value given: foo=bar",
            ),
            (
                format!("#!lua name=a name=b\n{ok}"),
                "ERR Invalid metadata value, name argument was given multiple times",
            ),
            ("#!lua name=a\nlocal x = 1".to_owned(), "ERR No functions registered"),
            (
                "#!lua name=a\nreturn 1 +".to_owned(),
                "ERR Error compiling function: user_function:2: unexpected symbol near '<eof>'",
            ),
            (
                format!("#!lua name=a\n{ok}\n{ok}"),
                "ERR Error registering functions: ERR Function already exists in the library",
            ),
            (
                "#!lua name=a\nredis.register_function('f')".to_owned(),
                "ERR Error registering functions: ERR calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).",
            ),
            (
                "#!lua name=a\nredis.register_function('f', 'x')".to_owned(),
                "ERR Error registering functions: ERR second argument to redis.register_function must be a function",
            ),
            (
                "#!lua name=a\nredis.register_function{function_name='f', callback=function() end, flags={'bogus'}}".to_owned(),
                "ERR Error registering functions: ERR unknown flag given",
            ),
            (
                "#!lua name=a\nredis.register_function{function_name='f'}".to_owned(),
                "ERR Error registering functions: ERR redis.register_function must get a callback argument",
            ),
        ] {
            assert_eq!(load(&mut h, &code), err(message), "{code}");
        }
        assert_eq!(h.run("FUNCTION LIST"), Frame::Array(vec![]));
    }

    #[test]
    fn replacing_deleting_and_flushing_libraries() {
        let mut h = Harness::new();
        let v1 = "#!lua name=lib\nredis.register_function('f', function() return 1 end)";
        let v2 = "#!lua name=lib\nredis.register_function('g', function() return 2 end)";
        assert_eq!(load(&mut h, v1), bulk("lib"));
        assert_eq!(load(&mut h, v1), err("ERR Library 'lib' already exists"));
        // Another library may not take a name that is in use.
        assert_eq!(
            load(
                &mut h,
                "#!lua name=other\nredis.register_function('F', function() end)"
            ),
            err("ERR Function F already exists")
        );
        // A failed replacement leaves the old library in place.
        assert_eq!(
            h.cmd(&["FUNCTION", "LOAD", "REPLACE", "#!lua name=lib\nlocal x = 1"]),
            err("ERR No functions registered")
        );
        assert_eq!(h.run("FCALL f 0"), int(1));

        assert_eq!(h.cmd(&["FUNCTION", "LOAD", "REPLACE", v2]), bulk("lib"));
        assert_eq!(h.run("FCALL f 0"), err("ERR Function not found"));
        assert_eq!(h.run("FCALL g 0"), int(2));

        // SCRIPT FLUSH is about scripts, not functions.
        assert_eq!(h.run("SCRIPT FLUSH"), Frame::ok());
        assert_eq!(h.run("FCALL g 0"), int(2));

        assert_eq!(h.run("FUNCTION DELETE lib"), Frame::ok());
        assert_eq!(h.run("FUNCTION DELETE lib"), err("ERR Library not found"));
        assert_eq!(h.run("FCALL g 0"), err("ERR Function not found"));

        load(&mut h, v1);
        assert_eq!(h.run("FUNCTION FLUSH"), Frame::ok());
        assert_eq!(h.run("FCALL f 0"), err("ERR Function not found"));
        assert_eq!(
            h.run("FUNCTION FLUSH NOW"),
            err("ERR FUNCTION FLUSH only supports SYNC|ASYNC option")
        );
    }

    #[test]
    fn function_list_and_stats() {
        let mut h = Harness::new();
        let code = "#!lua name=lib\nredis.register_function{function_name='f', \
                    callback=function() return 1 end, flags={'no-writes'}, description='says one'}";
        load(&mut h, code);
        load(
            &mut h,
            "#!lua name=second\nredis.register_function('g', function() end)\n\
             redis.register_function('h', function() end)",
        );
        let described = |with_code: bool| {
            let mut fields = vec![
                (bulk("library_name"), bulk("lib")),
                (bulk("engine"), bulk("LUA")),
                (
                    bulk("functions"),
                    Frame::Array(vec![Frame::Map(vec![
                        (bulk("name"), bulk("f")),
                        (bulk("description"), bulk("says one")),
                        (bulk("flags"), Frame::Set(vec![bulk("no-writes")])),
                    ])]),
                ),
            ];
            if with_code {
                fields.push((bulk("library_code"), bulk(code)));
            }
            Frame::Array(vec![Frame::Map(fields)])
        };
        assert_eq!(h.run("FUNCTION LIST LIBRARYNAME lib"), described(false));
        assert_eq!(
            h.run("FUNCTION LIST WITHCODE LIBRARYNAME l*"),
            described(true)
        );
        assert!(matches!(h.run("FUNCTION LIST"), Frame::Array(all) if all.len() == 2));
        assert_eq!(
            h.run("FUNCTION LIST LIBRARYNAME nope"),
            Frame::Array(vec![])
        );
        assert_eq!(
            h.run("FUNCTION LIST LIBRARYNAME"),
            err("ERR library name argument was not given")
        );
        assert_eq!(
            h.run("FUNCTION LIST BOGUS"),
            err("ERR Unknown argument BOGUS")
        );
        assert_eq!(
            h.run("FUNCTION STATS"),
            Frame::Map(vec![
                (bulk("running_script"), Frame::Null),
                (
                    bulk("engines"),
                    Frame::Map(vec![(
                        bulk("LUA"),
                        Frame::Map(vec![
                            (bulk("libraries_count"), int(2)),
                            (bulk("functions_count"), int(3)),
                        ])
                    )])
                ),
            ])
        );
        assert_eq!(
            h.run("FUNCTION KILL"),
            err("NOTBUSY No scripts in execution right now.")
        );
    }

    #[test]
    fn calling_functions() {
        let mut h = Harness::new();
        load(
            &mut h,
            "#!lua name=lib\n\
             redis.register_function('echo', function(keys, args) return {keys, args} end)\n\
             redis.register_function('set', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n\
             redis.register_function{function_name='peek', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}\n\
             redis.register_function{function_name='sneaky', callback=function(keys) return redis.call('DEL', keys[1]) end, flags={'no-writes'}}\n\
             redis.register_function('fail', function() error('boom') end)\n\
             redis.register_function('globals', function() return KEYS end)",
        );
        assert_eq!(
            h.run("FCALL echo 2 k1 k2 a1"),
            Frame::Array(vec![
                Frame::Array(bulks(&["k1", "k2"])),
                Frame::Array(bulks(&["a1"]))
            ])
        );
        assert_eq!(h.run("FCALL set 1 k v"), Frame::ok());
        assert_eq!(h.run("FCALL_RO peek 1 k"), bulk("v"));
        assert_eq!(
            h.run("FCALL_RO set 1 k other"),
            err("ERR Can not execute a script with write flag using *_ro command.")
        );
        assert_eq!(
            h.run("FCALL sneaky 1 k"),
            err("ERR Write commands are not allowed from read-only scripts. script: sneaky, on @user_function:5.")
        );
        assert_eq!(h.run("GET k"), bulk("v"));
        assert_eq!(
            h.run("FCALL fail 0"),
            err("ERR user_function:6: boom script: fail, on @user_function:6.")
        );
        // A script that ran earlier does not leave KEYS behind for functions.
        h.cmd(&["EVAL", "return KEYS[1]", "1", "left-over"]);
        assert_eq!(
            h.run("FCALL globals 0"),
            err("ERR user_function:7: Script attempted to access nonexistent global variable 'KEYS' script: globals, on @user_function:7.")
        );
        for (command, message) in [
            ("FCALL nosuch 0", "ERR Function not found"),
            ("FCALL echo x", "ERR Bad number of keys provided"),
            ("FCALL echo -1", "ERR Number of keys can't be negative"),
            (
                "FCALL echo 2 a",
                "ERR Number of keys can't be greater than number of args",
            ),
            (
                "FCALL echo",
                "ERR wrong number of arguments for 'fcall' command",
            ),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        // Functions cannot be called or managed from inside scripts.
        let script = "return redis.call('FCALL', 'echo', 0)";
        assert_eq!(
            eval(&mut h, script),
            err(&format!(
                "ERR This Redis command is not allowed from script script: {}, on @user_script:1.",
                sha(script)
            ))
        );
        assert_eq!(
            eval(&mut h, "return type(redis.register_function)"),
            bulk("nil")
        );
        // Inside a transaction.
        h.run("MULTI");
        h.run("FCALL set 1 tx 1");
        h.run("FCALL_RO peek 1 tx");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::ok(), bulk("1")]));
    }

    #[test]
    fn function_dump_and_restore() {
        let mut h = Harness::new();
        load(
            &mut h,
            "#!lua name=lib
redis.register_function('one', function() return 1 end)",
        );
        let Frame::Bulk(payload) = h.run("FUNCTION DUMP") else {
            panic!("no payload")
        };
        let restore = |h: &mut Harness, policy: Option<&str>| {
            let mut args = vec![
                bytes::Bytes::from_static(b"FUNCTION"),
                bytes::Bytes::from_static(b"RESTORE"),
                payload.clone(),
            ];
            args.extend(policy.map(|policy| bytes::Bytes::copy_from_slice(policy.as_bytes())));
            h.raw(&args)
        };
        // The library is already there.
        assert_eq!(restore(&mut h, None), err("ERR Library lib already exists"));
        assert_eq!(restore(&mut h, Some("REPLACE")), Frame::ok());
        h.run("FUNCTION FLUSH");
        assert_eq!(restore(&mut h, Some("append")), Frame::ok());
        assert_eq!(h.run("FCALL one 0"), int(1));
        // FLUSH drops what is there first.
        load(
            &mut h,
            "#!lua name=other
redis.register_function('two', function() return 2 end)",
        );
        assert_eq!(restore(&mut h, Some("FLUSH")), Frame::ok());
        assert_eq!(h.run("FCALL two 0"), err("ERR Function not found"));
        assert_eq!(h.run("FCALL one 0"), int(1));
        assert_eq!(
            restore(&mut h, Some("BOGUS")),
            err("ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.")
        );
        assert_eq!(
            h.run("FUNCTION RESTORE garbage"),
            err("ERR DUMP payload version or checksum are wrong")
        );
        // With no libraries, the dump is still a valid (empty) payload.
        h.run("FUNCTION FLUSH");
        let Frame::Bulk(empty) = h.run("FUNCTION DUMP") else {
            panic!("no payload")
        };
        assert_eq!(
            h.raw(&[
                bytes::Bytes::from_static(b"FUNCTION"),
                bytes::Bytes::from_static(b"RESTORE"),
                empty
            ]),
            Frame::ok()
        );
    }

    #[test]
    fn a_runaway_function_can_be_killed() {
        let (mut h, _) = impatient();
        load(
            &mut h,
            "#!lua name=lib\nredis.register_function('spin', function() while true do end end)",
        );
        assert_eq!(
            h.run("FCALL spin 0"),
            err("ERR Script killed by user with SCRIPT KILL... script: spin, on @user_function:2.")
        );
    }

    #[test]
    fn redis_helpers() {
        let mut h = Harness::new();
        assert_eq!(
            eval(&mut h, "return redis.sha1hex('abc')"),
            bulk("a9993e364706816aba3e25717850c26c9cd0d89d")
        );
        assert_eq!(
            eval(
                &mut h,
                "redis.log(redis.LOG_WARNING, 'hello') return redis.LOG_WARNING"
            ),
            int(3)
        );
        assert_eq!(eval(&mut h, "return redis.REDIS_VERSION"), bulk("7.4.0"));
        assert_eq!(
            eval(&mut h, "return redis.REDIS_VERSION_NUM"),
            int(0x0007_0400)
        );
    }
}
