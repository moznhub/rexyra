//! Server administration: `INFO`, `CONFIG`, `SLOWLOG`, `MEMORY`, `TIME`.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use rexyra_engine::{
    glob_match, Set, Value, COMPACT_MAX_ENTRIES, COMPACT_MAX_VALUE, HASH_COMPACT_MAX_ENTRIES,
    INTSET_MAX_ENTRIES,
};
use rexyra_resp::Frame;

use crate::{
    int, is, lossy, reply, unknown_subcommand, wrong_subcommand_arity, Ctx, COMPAT_VERSION,
    ERR_SYNTAX,
};

// --- INFO -------------------------------------------------------------------

/// Sections in the order they are printed. The flag says whether a plain
/// `INFO` includes the section.
const SECTIONS: &[(&str, bool)] = &[
    ("server", true),
    ("clients", true),
    ("memory", true),
    ("persistence", true),
    ("stats", true),
    ("replication", true),
    ("commandstats", false),
    ("errorstats", true),
    ("cluster", true),
    ("keyspace", true),
];

/// Formats a byte count the way Redis does: `512B`, `1.50K`, `2.00M`.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("T", 1 << 40),
        ("G", 1 << 30),
        ("M", 1 << 20),
        ("K", 1 << 10),
    ];
    for (unit, size) in UNITS {
        if bytes >= size {
            return format!("{:.2}{unit}", bytes as f64 / size as f64);
        }
    }
    format!("{bytes}B")
}

/// Memory held by the process, as the operating system reports it.
fn process_memory() -> u64 {
    memory_stats::memory_stats().map_or(0, |stats| stats.physical_mem as u64)
}

fn section(ctx: &mut Ctx<'_>, name: &str, out: &mut String) {
    // Writing to a `String` cannot fail.
    let server = &*ctx.server;
    match name {
        "server" => {
            let uptime = ctx.now.saturating_sub(server.started_at) / 1000;
            let _ = write!(
                out,
                "# Server\r\n\
                 redis_version:{COMPAT_VERSION}\r\n\
                 server_name:rexyra\r\n\
                 server_version:{}\r\n\
                 redis_mode:standalone\r\n\
                 os:{} {}\r\n\
                 arch_bits:{}\r\n\
                 process_id:{}\r\n\
                 run_id:{}\r\n\
                 tcp_port:{}\r\n\
                 uptime_in_seconds:{uptime}\r\n\
                 uptime_in_days:{}\r\n\
                 hz:10\r\n\
                 configured_hz:10\r\n\
                 config_file:{}\r\n",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH,
                usize::BITS,
                std::process::id(),
                server.run_id,
                server.port,
                uptime / 86_400,
                server.config_file.as_deref().unwrap_or_default(),
            );
        }
        "clients" => {
            let _ = write!(
                out,
                "# Clients\r\n\
                 connected_clients:{}\r\n\
                 maxclients:{}\r\n\
                 blocked_clients:{}\r\n",
                ctx.clients.len() + 1,
                server.maxclients.load(Ordering::Relaxed),
                server.blocked_clients,
            );
        }
        "memory" => {
            let used = crate::memory::used_memory(server, ctx.keyspace);
            let rss = process_memory();
            let _ = write!(
                out,
                "# Memory\r\n\
                 used_memory:{used}\r\n\
                 used_memory_human:{}\r\n\
                 used_memory_rss:{rss}\r\n\
                 used_memory_rss_human:{}\r\n\
                 maxmemory:{}\r\n\
                 maxmemory_human:{}\r\n\
                 maxmemory_policy:{}\r\n",
                human_bytes(used),
                human_bytes(rss),
                server.maxmemory,
                human_bytes(server.maxmemory),
                server.maxmemory_policy.name(),
            );
        }
        "persistence" => {
            let rewriting = server.aof.as_ref().is_some_and(|aof| aof.is_rewriting());
            let status = |ok: bool| if ok { "ok" } else { "err" };
            let _ = write!(
                out,
                "# Persistence\r\n\
                 loading:0\r\n\
                 rdb_changes_since_last_save:{}\r\n\
                 rdb_bgsave_in_progress:{}\r\n\
                 rdb_last_save_time:{}\r\n\
                 rdb_last_bgsave_status:{}\r\n\
                 aof_enabled:{}\r\n\
                 aof_rewrite_in_progress:{}\r\n\
                 aof_rewrite_scheduled:{}\r\n\
                 aof_last_bgrewrite_status:{}\r\n\
                 aof_last_write_status:{}\r\n",
                server.dirty,
                u8::from(server.bgsave.is_some() && !rewriting),
                server.last_save,
                if server.last_save_ok { "ok" } else { "err" },
                u8::from(server.aof.is_some()),
                u8::from(rewriting),
                u8::from(server.aof_rewrite_scheduled),
                status(server.aof.as_ref().is_none_or(|aof| aof.last_rewrite_ok)),
                status(server.aof.as_ref().is_none_or(|aof| aof.last_write_ok)),
            );
            if let Some(aof) = &server.aof {
                let _ = write!(
                    out,
                    "aof_current_size:{}\r\naof_base_size:{}\r\n",
                    aof.size(),
                    aof.base_size(),
                );
            }
        }
        "stats" => {
            let stats = &server.stats;
            let _ = write!(
                out,
                "# Stats\r\n\
                 total_connections_received:{}\r\n\
                 total_commands_processed:{}\r\n\
                 instantaneous_ops_per_sec:{}\r\n\
                 expired_keys:{}\r\n\
                 evicted_keys:{}\r\n\
                 pubsub_channels:0\r\n\
                 pubsub_patterns:0\r\n\
                 total_error_replies:{}\r\n",
                stats.connections,
                stats.commands,
                stats.ops_per_sec(),
                stats.expired_keys,
                stats.evicted_keys,
                stats.error_replies,
            );
        }
        "replication" => {
            out.push_str(
                "# Replication\r\n\
                 role:master\r\n\
                 connected_slaves:0\r\n",
            );
        }
        "commandstats" => {
            out.push_str("# Commandstats\r\n");
            for (name, stat) in server.stats.commands() {
                let per_call = if stat.calls == 0 {
                    0.0
                } else {
                    stat.micros as f64 / stat.calls as f64
                };
                let _ = write!(
                    out,
                    "cmdstat_{name}:calls={},usec={},usec_per_call={per_call:.2},\
                     rejected_calls={},failed_calls={}\r\n",
                    stat.calls, stat.micros, stat.rejected, stat.failed,
                );
            }
        }
        "errorstats" => {
            out.push_str("# Errorstats\r\n");
            for (code, count) in &server.stats.errors_by_code {
                let _ = write!(out, "errorstat_{code}:count={count}\r\n");
            }
        }
        "cluster" => out.push_str("# Cluster\r\ncluster_enabled:0\r\n"),
        "keyspace" => {
            out.push_str("# Keyspace\r\n");
            for index in 0..server.databases {
                let db = ctx.keyspace.db(index);
                if !db.is_empty() {
                    let _ = write!(
                        out,
                        "db{index}:keys={},expires={},avg_ttl=0,subexpiry=0\r\n",
                        db.len(),
                        db.volatile_len(),
                    );
                }
            }
        }
        _ => {}
    }
}

/// `INFO [section ...]`
pub(crate) fn info(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let wanted = &args[1..];
    let everything = wanted
        .iter()
        .any(|arg| is(arg, "all") || is(arg, "everything"));
    let defaults = wanted.is_empty() || wanted.iter().any(|arg| is(arg, "default"));

    let mut out = String::new();
    for &(name, by_default) in SECTIONS {
        let selected =
            everything || (defaults && by_default) || wanted.iter().any(|arg| is(arg, name));
        if selected {
            if !out.is_empty() {
                out.push_str("\r\n");
            }
            section(ctx, name, &mut out);
        }
    }
    Frame::Verbatim {
        format: *b"txt",
        data: Bytes::from(out),
    }
}

// --- CONFIG -----------------------------------------------------------------

/// How `CONFIG SET` treats a parameter.
#[derive(Clone, Copy, PartialEq)]
enum Access {
    /// Can be changed while running.
    Mutable,
    /// Fixed at start-up, as in Redis.
    Immutable,
    /// Describes behaviour this server does not let you change yet.
    Fixed,
}

const PARAMETERS: &[(&str, Access)] = &[
    ("bind", Access::Fixed),
    ("port", Access::Fixed),
    ("databases", Access::Immutable),
    ("maxclients", Access::Mutable),
    ("slowlog-log-slower-than", Access::Mutable),
    ("slowlog-max-len", Access::Mutable),
    ("dir", Access::Fixed),
    ("dbfilename", Access::Fixed),
    ("busy-reply-threshold", Access::Mutable),
    // The older name of the same setting.
    ("lua-time-limit", Access::Mutable),
    ("hz", Access::Fixed),
    ("timeout", Access::Fixed),
    ("maxmemory", Access::Mutable),
    ("maxmemory-policy", Access::Mutable),
    ("maxmemory-samples", Access::Mutable),
    ("lfu-log-factor", Access::Mutable),
    ("lfu-decay-time", Access::Mutable),
    ("appendonly", Access::Mutable),
    ("appendfsync", Access::Mutable),
    ("appenddirname", Access::Immutable),
    ("appendfilename", Access::Immutable),
    ("auto-aof-rewrite-percentage", Access::Mutable),
    ("auto-aof-rewrite-min-size", Access::Mutable),
    ("aof-load-truncated", Access::Fixed),
    ("save", Access::Mutable),
    ("rdb-key-save-delay", Access::Mutable),
    ("requirepass", Access::Mutable),
    ("protected-mode", Access::Mutable),
    ("proto-max-bulk-len", Access::Fixed),
    ("hash-max-listpack-entries", Access::Fixed),
    ("hash-max-listpack-value", Access::Fixed),
    ("set-max-intset-entries", Access::Fixed),
    ("set-max-listpack-entries", Access::Fixed),
    ("set-max-listpack-value", Access::Fixed),
    ("zset-max-listpack-entries", Access::Fixed),
    ("zset-max-listpack-value", Access::Fixed),
];

fn yes_no(flag: bool) -> String {
    if flag { "yes" } else { "no" }.to_owned()
}

fn parameter_value(ctx: &Ctx<'_>, name: &str) -> String {
    let server = &*ctx.server;
    match name {
        "bind" => server.bind.clone(),
        "dir" => server.dir.clone(),
        "dbfilename" => server.dbfilename.clone(),
        "port" => server.port.to_string(),
        "databases" => server.databases.to_string(),
        "maxclients" => server.maxclients.load(Ordering::Relaxed).to_string(),
        "slowlog-log-slower-than" => server.slowlog_slower_than.to_string(),
        "slowlog-max-len" => server.slowlog_max_len.to_string(),
        "busy-reply-threshold" | "lua-time-limit" => server.busy_reply_threshold.to_string(),
        "hz" => "10".to_owned(),
        // No idle timeout.
        "timeout" => "0".to_owned(),
        "maxmemory" => server.maxmemory.to_string(),
        "maxmemory-policy" => server.maxmemory_policy.name().to_owned(),
        "maxmemory-samples" => server.maxmemory_samples.to_string(),
        "lfu-log-factor" => server.lfu_log_factor.to_string(),
        "lfu-decay-time" => server.lfu_decay_time.to_string(),
        "appendonly" => yes_no(server.aof.is_some()),
        "appendfsync" => server
            .aof
            .as_ref()
            .map_or(server.aof_settings.fsync, |aof| aof.fsync)
            .name()
            .to_owned(),
        "appenddirname" => server.aof_settings.dirname.clone(),
        "appendfilename" => server.aof_settings.filename.clone(),
        "auto-aof-rewrite-percentage" => server.aof_settings.rewrite_percentage.to_string(),
        "auto-aof-rewrite-min-size" => server.aof_settings.rewrite_min_size.to_string(),
        "aof-load-truncated" => yes_no(server.aof_settings.load_truncated),
        "save" => crate::persistence::format_save_points(&server.save_points),
        "rdb-key-save-delay" => server.rdb_key_save_delay.to_string(),
        "requirepass" => server.requirepass.clone(),
        "protected-mode" => yes_no(server.protected_mode),
        "proto-max-bulk-len" => (512 * 1024 * 1024).to_string(),
        "hash-max-listpack-entries" => HASH_COMPACT_MAX_ENTRIES.to_string(),
        "set-max-intset-entries" => INTSET_MAX_ENTRIES.to_string(),
        "set-max-listpack-entries" | "zset-max-listpack-entries" => COMPACT_MAX_ENTRIES.to_string(),
        "hash-max-listpack-value" | "set-max-listpack-value" | "zset-max-listpack-value" => {
            COMPACT_MAX_VALUE.to_string()
        }
        _ => String::new(),
    }
}

fn config_get(ctx: &mut Ctx<'_>, asked: &[Bytes]) -> Frame {
    let patterns: Vec<Vec<u8>> = asked
        .iter()
        .map(|pattern| pattern.to_ascii_lowercase())
        .collect();
    Frame::Map(
        PARAMETERS
            .iter()
            .filter_map(|(name, _)| {
                let index = patterns
                    .iter()
                    .position(|pattern| glob_match(pattern, name.as_bytes()))?;
                // Redis echoes an exact name the way it was asked for.
                let shown = if patterns[index] == name.as_bytes() {
                    Frame::Bulk(asked[index].clone())
                } else {
                    Frame::bulk(*name)
                };
                Some((shown, Frame::bulk(parameter_value(ctx, name))))
            })
            .collect(),
    )
}

/// A validated change, ready to apply.
enum Change {
    MaxClients(usize),
    SlowerThan(i64),
    SlowlogLen(usize),
    BusyThreshold(u64),
    SavePoints(Vec<(u64, u64)>),
    AppendOnly(bool),
    Fsync(crate::aof::Fsync),
    RewriteMinSize(u64),
    RewritePercentage(u64),
    MaxMemory(u64),
    Policy(crate::memory::Policy),
    Samples(usize),
    LfuLogFactor(u32),
    LfuDecayTime(u32),
    RequirePass(String),
    ProtectedMode(bool),
    SaveDelay(u64),
}

fn config_set(ctx: &mut Ctx<'_>, pairs: &[Bytes]) -> Result<Frame, Frame> {
    let failed = |name: &str, why: &str| {
        Frame::error(format!(
            "ERR CONFIG SET failed (possibly related to argument '{name}') - {why}"
        ))
    };
    // Validate everything first: either all changes apply or none.
    let mut changes = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for pair in pairs.chunks(2) {
        let name = lossy(&pair[0]).to_ascii_lowercase();
        let Some(&(_, access)) = PARAMETERS.iter().find(|(known, _)| *known == name) else {
            return Err(Frame::error(format!(
                "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
                lossy(&pair[0])
            )));
        };
        if seen.contains(&name) {
            return Err(failed(&name, "duplicate parameter"));
        }
        match access {
            Access::Mutable => {}
            Access::Immutable => return Err(failed(&name, "can't set immutable config")),
            Access::Fixed => return Err(failed(&name, "not supported by this server yet")),
        }
        if name == "save" {
            let points = crate::persistence::parse_save_points(&lossy(&pair[1]))
                .ok_or_else(|| failed(&name, "Invalid save parameters"))?;
            changes.push(Change::SavePoints(points));
            seen.push(name);
            continue;
        }
        if name == "requirepass" {
            // Taken as it is: a password is not text to be normalised.
            changes.push(Change::RequirePass(lossy(&pair[1]).into_owned()));
            seen.push(name);
            continue;
        }
        let text = lossy(&pair[1]).to_ascii_lowercase();
        let special = match name.as_str() {
            "protected-mode" => Some(match text.as_str() {
                "yes" => Change::ProtectedMode(true),
                "no" => Change::ProtectedMode(false),
                _ => return Err(failed(&name, "argument must be 'yes' or 'no'")),
            }),
            "appendonly" => Some(match text.as_str() {
                "yes" => Change::AppendOnly(true),
                "no" => Change::AppendOnly(false),
                _ => return Err(failed(&name, "argument must be 'yes' or 'no'")),
            }),
            "appendfsync" => Some(Change::Fsync(crate::aof::Fsync::parse(&text).ok_or_else(
                || {
                    failed(
                        &name,
                        "argument(s) must be one of the following: everysec, always, no",
                    )
                },
            )?)),
            "auto-aof-rewrite-min-size" => Some(Change::RewriteMinSize(
                crate::aof::parse_size(&text)
                    .ok_or_else(|| failed(&name, "argument must be a memory value"))?,
            )),
            "maxmemory" => Some(Change::MaxMemory(
                crate::aof::parse_size(&text)
                    .ok_or_else(|| failed(&name, "argument must be a memory value"))?,
            )),
            "maxmemory-policy" => Some(Change::Policy(
                crate::memory::Policy::parse(&text).ok_or_else(|| {
                    failed(
                        &name,
                        "argument(s) must be one of the following: volatile-lru, volatile-lfu, \
                         volatile-random, volatile-ttl, allkeys-lru, allkeys-lfu, \
                         allkeys-random, noeviction",
                    )
                })?,
            )),
            _ => None,
        };
        if let Some(change) = special {
            changes.push(change);
            seen.push(name);
            continue;
        }
        let number = int(&pair[1])
            .map_err(|_| failed(&name, "argument couldn't be parsed into an integer"))?;
        let out_of_range = |low: i64, high: i64| {
            failed(
                &name,
                &format!("argument must be between {low} and {high} inclusive"),
            )
        };
        changes.push(match name.as_str() {
            "maxclients" if (1..=i64::from(u32::MAX)).contains(&number) => {
                Change::MaxClients(number as usize)
            }
            "maxclients" => return Err(out_of_range(1, i64::from(u32::MAX))),
            "slowlog-log-slower-than" => Change::SlowerThan(number),
            "slowlog-max-len" if number >= 0 => Change::SlowlogLen(number as usize),
            "busy-reply-threshold" | "lua-time-limit" if number >= 0 => {
                Change::BusyThreshold(number as u64)
            }
            "auto-aof-rewrite-percentage" if number >= 0 => {
                Change::RewritePercentage(number as u64)
            }
            "rdb-key-save-delay" if number >= 0 => Change::SaveDelay(number as u64),
            "maxmemory-samples" if (1..=64).contains(&number) => Change::Samples(number as usize),
            "maxmemory-samples" => return Err(out_of_range(1, 64)),
            "lfu-log-factor" if (0..=i64::from(i32::MAX)).contains(&number) => {
                Change::LfuLogFactor(number as u32)
            }
            "lfu-decay-time" if (0..=i64::from(i32::MAX)).contains(&number) => {
                Change::LfuDecayTime(number as u32)
            }
            "lfu-log-factor" | "lfu-decay-time" => {
                return Err(out_of_range(0, i64::from(i32::MAX)))
            }
            _ => return Err(out_of_range(0, i64::MAX)),
        });
        seen.push(name);
    }
    for change in changes {
        match change {
            Change::MaxClients(max) => ctx.server.maxclients.store(max, Ordering::Relaxed),
            Change::SlowerThan(micros) => ctx.server.slowlog_slower_than = micros,
            Change::SlowlogLen(len) => ctx.server.set_slowlog_max_len(len),
            Change::BusyThreshold(millis) => ctx.server.busy_reply_threshold = millis,
            Change::SavePoints(points) => ctx.server.save_points = points,
            Change::AppendOnly(true) => {
                crate::aof::enable(ctx.server, ctx.keyspace, ctx.now).map_err(|error| {
                    Frame::error(format!(
                        "ERR CONFIG SET failed (possibly related to argument 'appendonly') - {error}"
                    ))
                })?;
            }
            Change::AppendOnly(false) => crate::aof::disable(ctx.server, ctx.keyspace, ctx.now),
            Change::Fsync(policy) => {
                ctx.server.aof_settings.fsync = policy;
                if let Some(aof) = &mut ctx.server.aof {
                    aof.fsync = policy;
                }
            }
            Change::RewriteMinSize(bytes) => ctx.server.aof_settings.rewrite_min_size = bytes,
            Change::RewritePercentage(percent) => {
                ctx.server.aof_settings.rewrite_percentage = percent;
            }
            Change::RequirePass(password) => {
                ctx.server.set_requirepass(&password);
            }
            Change::ProtectedMode(on) => {
                ctx.server.protected_mode = on;
                ctx.server.refresh_protection();
            }
            Change::SaveDelay(micros) => ctx.server.rdb_key_save_delay = micros,
            Change::MaxMemory(bytes) => ctx.server.maxmemory = bytes,
            Change::Policy(policy) => ctx.server.maxmemory_policy = policy,
            Change::Samples(samples) => ctx.server.maxmemory_samples = samples,
            Change::LfuLogFactor(factor) => {
                ctx.server.lfu_log_factor = factor;
                ctx.keyspace.set_lfu(factor, ctx.server.lfu_decay_time);
            }
            Change::LfuDecayTime(minutes) => {
                ctx.server.lfu_decay_time = minutes;
                ctx.keyspace.set_lfu(ctx.server.lfu_log_factor, minutes);
            }
        }
    }
    Ok(Frame::ok())
}

/// `CONFIG GET pattern [pattern ...] | SET name value [name value ...] |
/// RESETSTAT | REWRITE`
pub(crate) fn config(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (sub, rest) = (&args[1], &args[2..]);
    if is(sub, "GET") {
        if rest.is_empty() {
            return wrong_subcommand_arity("config", sub);
        }
        config_get(ctx, rest)
    } else if is(sub, "SET") {
        if rest.is_empty() || rest.len() % 2 != 0 {
            return wrong_subcommand_arity("config", sub);
        }
        reply(config_set(ctx, rest))
    } else if is(sub, "RESETSTAT") {
        if !rest.is_empty() {
            return wrong_subcommand_arity("config", sub);
        }
        ctx.server.stats.reset();
        Frame::ok()
    } else if is(sub, "REWRITE") {
        if !rest.is_empty() {
            return wrong_subcommand_arity("config", sub);
        }
        Frame::error(match ctx.server.config_file {
            None => "ERR The server is running without a config file",
            Some(_) => "ERR Rewriting the config file is not supported yet",
        })
    } else {
        unknown_subcommand("CONFIG", sub)
    }
}

// --- SLOWLOG ----------------------------------------------------------------

/// `SLOWLOG GET [count] | LEN | RESET`
pub(crate) fn slowlog(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (sub, rest) = (&args[1], &args[2..]);
    if is(sub, "GET") {
        let count = match rest {
            [] => 10,
            [count] => match int(count) {
                // -1 means everything.
                Ok(-1) => usize::MAX,
                Ok(count) if count >= 0 => count as usize,
                _ => return Frame::error("ERR count should be greater than or equal to -1"),
            },
            _ => {
                return Frame::error(
                    "ERR unknown subcommand or wrong number of arguments for 'GET'. \
                     Try SLOWLOG HELP.",
                )
            }
        };
        Frame::Array(
            ctx.server
                .slowlog
                .iter()
                .take(count)
                .map(|entry| {
                    Frame::Array(vec![
                        Frame::Integer(entry.id as i64),
                        Frame::Integer(entry.time as i64),
                        Frame::Integer(entry.micros as i64),
                        Frame::Array(entry.args.iter().cloned().map(Frame::Bulk).collect()),
                        Frame::bulk(entry.addr.clone()),
                        Frame::Bulk(entry.name.clone()),
                    ])
                })
                .collect(),
        )
    } else if is(sub, "LEN") {
        if !rest.is_empty() {
            return wrong_subcommand_arity("slowlog", sub);
        }
        Frame::Integer(ctx.server.slowlog.len() as i64)
    } else if is(sub, "RESET") {
        if !rest.is_empty() {
            return wrong_subcommand_arity("slowlog", sub);
        }
        ctx.server.slowlog.clear();
        Frame::ok()
    } else {
        unknown_subcommand("SLOWLOG", sub)
    }
}

// --- MEMORY -----------------------------------------------------------------

/// Bookkeeping bytes assumed per key and per collection element. The real
/// figures depend on the allocator; these keep the estimate in a realistic
/// range.
pub(crate) const KEY_OVERHEAD: usize = 56;
const ELEMENT_OVERHEAD: usize = 16;

/// Estimates the size of a collection from its first `samples` elements
/// (all of them when `samples` is zero).
fn sampled(len: usize, samples: usize, sizes: impl Iterator<Item = usize>) -> usize {
    let take = if samples == 0 { len } else { samples.min(len) };
    if take == 0 {
        return 0;
    }
    let total: usize = sizes.take(take).map(|size| size + ELEMENT_OVERHEAD).sum();
    (total as u128 * len as u128 / take as u128) as usize
}

pub(crate) fn value_size(value: &Value, samples: usize) -> usize {
    match value {
        Value::String(text) => text.len(),
        Value::List(list) => sampled(list.len(), samples, list.iter().map(Bytes::len)),
        Value::Hash(hash) => sampled(
            hash.len(),
            samples,
            hash.iter().map(|(field, value)| field.len() + value.len()),
        ),
        // Not through `Set::iter`, which hands out shared copies of the
        // members: sharing a member leaves a reference count on it.
        Value::Set(set) => match set {
            Set::Ints(ints) => sampled(ints.len(), samples, ints.iter().map(|_| 8)),
            Set::Compact(members) => {
                sampled(members.len(), samples, members.iter().map(Bytes::len))
            }
            Set::Table(dict) => sampled(
                dict.len(),
                samples,
                dict.iter().map(|(member, _)| member.len()),
            ),
        },
        // A score is eight bytes.
        Value::ZSet(zset) => sampled(
            zset.len(),
            samples,
            zset.iter().map(|(member, _)| member.len() + 8),
        ),
    }
}

/// `MEMORY USAGE key [SAMPLES count]` — an estimate of the bytes a key
/// takes, or null if it does not exist.
pub(crate) fn memory(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let sub = &args[1];
    if !is(sub, "USAGE") {
        return unknown_subcommand("MEMORY", sub);
    }
    let samples = match &args[2..] {
        [_key] => 5,
        [_key, opt, count] if is(opt, "SAMPLES") => match int(count) {
            Ok(count) if count >= 0 => count as usize,
            Ok(_) => return Frame::error(ERR_SYNTAX),
            Err(error) => return error,
        },
        [] => return wrong_subcommand_arity("memory", sub),
        _ => return Frame::error(ERR_SYNTAX),
    };
    let key = &args[2];
    let (db, now) = ctx.db();
    match db.get(key, now) {
        Some(value) => {
            Frame::Integer((KEY_OVERHEAD + key.len() + value_size(value, samples)) as i64)
        }
        None => Frame::Null,
    }
}

// --- TIME -------------------------------------------------------------------

/// `TIME` — seconds since the Unix epoch and the microseconds within the
/// current second.
pub(crate) fn time(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    Frame::Array(vec![
        Frame::bulk((ctx.now / 1000).to_string()),
        Frame::bulk((ctx.now % 1000 * 1000).to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::human_bytes;
    use crate::testing::{bulk, err, Harness};
    use rexyra_resp::Frame;

    fn text(frame: Frame) -> String {
        match frame {
            Frame::Verbatim { data, .. } | Frame::Bulk(data) => {
                String::from_utf8(data.to_vec()).unwrap()
            }
            other => panic!("not text: {other:?}"),
        }
    }

    fn field(info: &str, name: &str) -> String {
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name} in:\n{info}"))
            .to_owned()
    }

    fn map(frame: Frame) -> Vec<(String, String)> {
        let Frame::Map(pairs) = frame else {
            panic!("not a map: {frame:?}")
        };
        pairs
            .into_iter()
            .map(|(key, value)| (text(key), text(value)))
            .collect()
    }

    #[test]
    fn info_default_sections() {
        let mut h = Harness::new();
        h.run("SET a 1");
        h.run("SET b 1 EX 100");
        h.run("SELECT 3");
        h.run("SET c 1");
        h.run("GET");
        let info = text(h.run("INFO"));
        let headers: Vec<&str> = info.lines().filter(|line| line.starts_with('#')).collect();
        assert_eq!(
            headers,
            [
                "# Server",
                "# Clients",
                "# Memory",
                "# Persistence",
                "# Stats",
                "# Replication",
                "# Errorstats",
                "# Cluster",
                "# Keyspace"
            ]
        );
        assert!(info
            .lines()
            .all(|line| line.is_empty() || line.starts_with('#') || line.contains(':')));
        assert_eq!(field(&info, "redis_mode"), "standalone");
        assert_eq!(field(&info, "role"), "master");
        assert_eq!(field(&info, "connected_clients"), "1");
        assert_eq!(field(&info, "blocked_clients"), "0");
        assert_eq!(field(&info, "total_commands_processed"), "4");
        assert_eq!(field(&info, "total_error_replies"), "1");
        assert_eq!(field(&info, "errorstat_ERR"), "count=1");
        assert_eq!(
            field(&info, "db0"),
            "keys=2,expires=1,avg_ttl=0,subexpiry=0"
        );
        assert_eq!(
            field(&info, "db3"),
            "keys=1,expires=0,avg_ttl=0,subexpiry=0"
        );
        assert_eq!(field(&info, "run_id").len(), 40);
    }

    #[test]
    fn info_selected_sections() {
        let mut h = Harness::new();
        h.run("SET a 1");
        h.run("GET a");
        h.run("GET a");
        h.run("INCR a b");

        let info = text(h.run("INFO keyspace CLIENTS nonsense"));
        assert!(info.starts_with("# Clients\r\n"));
        assert!(info.contains("\r\n\r\n# Keyspace\r\n"));
        assert!(!info.contains("# Server"));
        assert_eq!(text(h.run("INFO nonsense")), "");

        let stats = text(h.run("INFO commandstats"));
        assert!(field(&stats, "cmdstat_get").starts_with("calls=2,usec="));
        assert!(field(&stats, "cmdstat_get").ends_with("rejected_calls=0,failed_calls=0"));
        assert!(field(&stats, "cmdstat_incr").starts_with("calls=0,"));
        assert!(field(&stats, "cmdstat_incr").ends_with("rejected_calls=1,failed_calls=0"));
        assert!(!text(h.run("INFO")).contains("# Commandstats"));
        assert!(text(h.run("INFO everything")).contains("# Commandstats"));
        assert!(text(h.run("INFO all")).contains("# Server"));
    }

    #[test]
    fn human_readable_sizes() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(1023), "1023B");
        assert_eq!(human_bytes(1536), "1.50K");
        assert_eq!(human_bytes(5 << 20), "5.00M");
        assert_eq!(human_bytes(3 << 30), "3.00G");
    }

    #[test]
    fn config_get() {
        let mut h = Harness::new();
        assert_eq!(
            map(h.run("CONFIG GET databases")),
            [("databases".to_owned(), "16".to_owned())]
        );
        assert_eq!(
            map(h.run("CONFIG GET SLOWLOG-*")),
            [
                ("slowlog-log-slower-than".to_owned(), "10000".to_owned()),
                ("slowlog-max-len".to_owned(), "128".to_owned())
            ]
        );
        // Several patterns; a parameter is listed once.
        assert_eq!(map(h.run("CONFIG GET port po* nothing")).len(), 1);
        assert_eq!(map(h.run("CONFIG GET nothing")), []);
        assert_eq!(
            map(h.run("CONFIG GET DataBases")),
            [("DataBases".to_owned(), "16".to_owned())]
        );
        assert_eq!(
            map(h.run("CONFIG GET hash-max-listpack-entries")),
            [("hash-max-listpack-entries".to_owned(), "512".to_owned())]
        );
        assert!(map(h.run("CONFIG GET *")).len() >= 20);
        assert_eq!(
            map(h.run("CONFIG GET maxmemory-policy")),
            [("maxmemory-policy".to_owned(), "noeviction".to_owned())]
        );
    }

    #[test]
    fn config_set() {
        let mut h = Harness::new();
        assert_eq!(
            h.run("CONFIG SET maxclients 50 slowlog-max-len 3"),
            Frame::ok()
        );
        assert_eq!(
            map(h.run("CONFIG GET maxclients")),
            [("maxclients".to_owned(), "50".to_owned())]
        );
        for (command, message) in [
            (
                "CONFIG SET nope 1",
                "ERR Unknown option or number of arguments for CONFIG SET - 'nope'",
            ),
            (
                "CONFIG SET port 1",
                "ERR CONFIG SET failed (possibly related to argument 'port') - not supported by this server yet",
            ),
            (
                "CONFIG SET databases 4",
                "ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config",
            ),
            (
                "CONFIG SET hz 100",
                "ERR CONFIG SET failed (possibly related to argument 'hz') - not supported by this server yet",
            ),
            (
                "CONFIG SET maxclients x",
                "ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument couldn't be parsed into an integer",
            ),
            (
                "CONFIG SET maxclients 0",
                "ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument must be between 1 and 4294967295 inclusive",
            ),
            (
                "CONFIG SET maxclients 5 maxclients 6",
                "ERR CONFIG SET failed (possibly related to argument 'maxclients') - duplicate parameter",
            ),
            (
                "CONFIG SET maxclients",
                "ERR wrong number of arguments for 'config|set' command",
            ),
            (
                "CONFIG GET",
                "ERR wrong number of arguments for 'config|get' command",
            ),
            (
                "CONFIG REWRITE",
                "ERR The server is running without a config file",
            ),
            (
                "CONFIG NOPE",
                "ERR unknown subcommand 'NOPE'. Try CONFIG HELP.",
            ),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        // A failed SET changes nothing, even the valid part.
        assert_eq!(
            h.run("CONFIG SET maxclients 7 slowlog-max-len x"),
            err("ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument couldn't be parsed into an integer")
        );
        assert_eq!(
            map(h.run("CONFIG GET maxclients")),
            [("maxclients".to_owned(), "50".to_owned())]
        );
    }

    #[test]
    fn config_resetstat() {
        let mut h = Harness::new();
        h.run("GET");
        h.run("PING");
        assert_eq!(h.run("CONFIG RESETSTAT"), Frame::ok());
        let info = text(h.run("INFO stats errorstats commandstats"));
        // Only the RESETSTAT itself has been counted since.
        assert_eq!(field(&info, "total_commands_processed"), "1");
        assert_eq!(field(&info, "total_error_replies"), "0");
        assert!(!info.contains("cmdstat_ping"));
        assert!(!info.contains("errorstat_ERR"));
    }

    #[test]
    fn slowlog() {
        let mut h = Harness::new();
        assert_eq!(h.run("SLOWLOG LEN"), Frame::Integer(0));
        // A threshold of zero logs everything.
        h.run("CONFIG SET slowlog-log-slower-than 0");
        h.run("CLIENT SETNAME worker");
        h.run("SET k v");
        let long = "x".repeat(200);
        h.cmd(&["SET", "big", &long]);
        // The CONFIG SET itself was logged too.
        assert_eq!(h.run("SLOWLOG LEN"), Frame::Integer(4));

        let Frame::Array(entries) = h.run("SLOWLOG GET 2") else {
            panic!("not an array")
        };
        assert_eq!(entries.len(), 2);
        // Newest first: `SLOWLOG LEN`, then the long SET.
        let Frame::Array(entry) = &entries[1] else {
            panic!("not an array")
        };
        assert_eq!(entry[0], Frame::Integer(3));
        assert_eq!(entry[1], Frame::Integer((h.now / 1000) as i64));
        assert!(matches!(entry[2], Frame::Integer(micros) if micros >= 0));
        let cut = format!("{}... (72 more bytes)", "x".repeat(128));
        assert_eq!(
            entry[3],
            Frame::Array(vec![bulk("SET"), bulk("big"), bulk(&cut)])
        );
        assert_eq!(entry[4], bulk("127.0.0.1:50000"));
        assert_eq!(entry[5], bulk("worker"));

        // The log is capped, and turning it off stops logging.
        h.run("CONFIG SET slowlog-max-len 2");
        assert_eq!(h.run("SLOWLOG LEN"), Frame::Integer(2));
        h.run("CONFIG SET slowlog-log-slower-than -1");
        assert_eq!(h.run("SLOWLOG RESET"), Frame::ok());
        h.run("SET k v");
        assert_eq!(h.run("SLOWLOG LEN"), Frame::Integer(0));
        assert_eq!(h.run("SLOWLOG GET -1"), Frame::Array(vec![]));
        assert_eq!(
            h.run("SLOWLOG GET -2"),
            err("ERR count should be greater than or equal to -1")
        );
        assert_eq!(
            h.run("SLOWLOG GET x"),
            err("ERR count should be greater than or equal to -1")
        );
    }

    #[test]
    fn slowlog_keeps_at_most_32_arguments() {
        let mut h = Harness::new();
        h.run("CONFIG SET slowlog-log-slower-than 0");
        let mut command = vec!["RPUSH", "l"];
        command.extend(std::iter::repeat_n("e", 40));
        h.cmd(&command);
        let Frame::Array(entries) = h.run("SLOWLOG GET 1") else {
            panic!("not an array")
        };
        let Frame::Array(entry) = &entries[0] else {
            panic!("not an array")
        };
        let Frame::Array(args) = &entry[3] else {
            panic!("not an array")
        };
        assert_eq!(args.len(), 32);
        assert_eq!(args[31], bulk("... (11 more arguments)"));
    }

    #[test]
    fn memory_usage() {
        let mut h = Harness::new();
        assert_eq!(h.run("MEMORY USAGE nokey"), Frame::Null);
        h.run("SET small v");
        h.cmd(&["SET", "large", &"x".repeat(10_000)]);
        let size = |h: &mut Harness, command: &str| match h.run(command) {
            Frame::Integer(bytes) => bytes,
            other => panic!("{command}: {other:?}"),
        };
        let small = size(&mut h, "MEMORY USAGE small");
        let large = size(&mut h, "MEMORY USAGE large");
        assert!(small > 0 && small < 200, "{small}");
        assert!((10_000..11_000).contains(&large), "{large}");

        for n in 0..100 {
            h.cmd(&["RPUSH", "list", &format!("element-{n:04}")]);
        }
        // Every element has the same size, so sampling is exact here.
        let sampled = size(&mut h, "MEMORY USAGE list");
        assert_eq!(sampled, size(&mut h, "MEMORY USAGE list SAMPLES 0"));
        assert!(sampled > 100 * 12, "{sampled}");

        assert_eq!(h.run("MEMORY USAGE list SAMPLES"), err("ERR syntax error"));
        assert_eq!(
            h.run("MEMORY USAGE"),
            err("ERR wrong number of arguments for 'memory|usage' command")
        );
        assert_eq!(
            h.run("MEMORY NOPE"),
            err("ERR unknown subcommand 'NOPE'. Try MEMORY HELP.")
        );
    }

    #[test]
    fn time() {
        let mut h = Harness::new();
        h.now = 1_700_000_000_123;
        assert_eq!(
            h.run("TIME"),
            Frame::Array(vec![bulk("1700000000"), bulk("123000")])
        );
    }
}
