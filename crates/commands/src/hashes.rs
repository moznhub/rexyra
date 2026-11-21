//! Hash commands, including the field-expiry family of Redis 7.4
//! (`HEXPIRE`, `HTTL`, `HPERSIST` ...).
//!
//! A field whose TTL has passed stays stored until the database reclaims it
//! (see [`Db::active_expire_fields`]), and the commands treat it as Redis
//! 7.4 does in the meantime: reads leave it out, and those that look up
//! single fields (`HGET`, `HEXISTS`, `HINCRBY` ...) delete it on the spot,
//! which the append-only file records as an `HDEL`. `HLEN` still counts
//! it, `HSET` and `HDEL` count it as there, and `HEXPIRE` can give it a new
//! TTL.

use bytes::Bytes;
use rexyra_engine::{glob_match, Db, Hash, Value};
use rexyra_resp::{parse_i64, Frame, Protocol};

use crate::notify::{notify, GENERIC, HASH};
use crate::strings::parse_float;
use crate::{
    copied, int, is, parse_cursor, parse_scan_options, reply, sample_distinct, wrong_arity,
    wrong_type, Ctx, ERR_SYNTAX,
};

/// The hash at `key`: `None` if the key does not exist, an error if it holds
/// another type.
fn hash_ref<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a Hash>, Frame> {
    match db.get(key, now) {
        None => Ok(None),
        Some(Value::Hash(hash)) => Ok(Some(hash)),
        Some(_) => Err(wrong_type()),
    }
}

/// The hash at `key`, looked at without counting as a read: no `keymiss`
/// and no expiry. For a second look within one command, after the first
/// has done both.
fn hash_peek<'a>(db: &'a Db, key: &[u8], now: u64) -> Result<Option<&'a Hash>, Frame> {
    match db.peek(key, now) {
        None => Ok(None),
        Some(Value::Hash(hash)) => Ok(Some(hash)),
        Some(_) => Err(wrong_type()),
    }
}

fn hash_mut<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a mut Hash>, Frame> {
    match db.get_mut(key, now) {
        None => Ok(None),
        Some(Value::Hash(hash)) => Ok(Some(hash)),
        Some(_) => Err(wrong_type()),
    }
}

/// The hash at `key`, created empty if the key does not exist. Callers must
/// have validated everything that could fail before calling this, so that an
/// empty hash is never left behind.
fn hash_or_create<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<&'a mut Hash, Frame> {
    match db.get_or_insert_with(key, now, || Value::Hash(Hash::new())) {
        Value::Hash(hash) => Ok(hash),
        _ => Err(wrong_type()),
    }
}

/// Runs `f` on the hash at `args[1]`, replying with `missing` if there is none.
fn with_hash(
    ctx: &mut Ctx<'_>,
    args: &[Bytes],
    missing: Frame,
    f: impl FnOnce(&Hash, u64) -> Frame,
) -> Frame {
    let (db, now) = ctx.db();
    match hash_ref(db, &args[1], now) {
        Ok(Some(hash)) => f(hash, now),
        Ok(None) => missing,
        Err(e) => e,
    }
}

/// Like [`with_hash`], for a command that looks up `fields`: any of them
/// whose TTL has passed is deleted first.
fn with_fields(
    ctx: &mut Ctx<'_>,
    args: &[Bytes],
    fields: &[Bytes],
    missing: &dyn Fn() -> Frame,
    f: impl FnOnce(&Hash, u64) -> Frame,
) -> Frame {
    let (db, now) = ctx.db();
    match hash_ref(db, &args[1], now) {
        Ok(Some(hash)) => {
            if !fields.iter().any(|field| hash.is_field_expired(field, now)) {
                return f(hash, now);
            }
        }
        Ok(None) => return missing(),
        Err(e) => return e,
    }
    db.expire_fields(&args[1], Some(fields), now);
    // The key was found above, even if its last fields have now gone.
    match hash_peek(db, &args[1], now) {
        Ok(Some(hash)) => f(hash, now),
        Ok(None) => missing(),
        Err(e) => e,
    }
}

// --- writes -----------------------------------------------------------------

fn set_fields(ctx: &mut Ctx<'_>, args: &[Bytes], command: &str) -> Result<usize, Frame> {
    let (pairs, leftover) = args[2..].as_chunks::<2>();
    if !leftover.is_empty() {
        return Err(wrong_arity(command));
    }
    let (db, now) = ctx.db();
    let hash = hash_or_create(db, &args[1], now)?;
    // A field whose TTL has passed but which is still stored counts as
    // there, as in Redis; writing it removes the TTL.
    let added = pairs
        .iter()
        .filter(|[field, value]| hash.insert(field, copied(value)))
        .count();
    notify(ctx, HASH, "hset", &args[1]);
    Ok(added)
}

/// `HSET key field value [field value ...]` — replies with the number of
/// fields that were new.
pub(crate) fn hset(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(set_fields(ctx, args, "hset").map(|added| Frame::Integer(added as i64)))
}

/// `HMSET key field value [field value ...]` — the older form; replies `OK`.
pub(crate) fn hmset(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(set_fields(ctx, args, "hmset").map(|_| Frame::ok()))
}

pub(crate) fn hsetnx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        {
            let hash = hash_or_create(db, &args[1], now)?;
            if !hash.is_field_expired(&args[2], now) {
                if hash.contains_stored(&args[2]) {
                    db.unchanged(&args[1]);
                    return Ok(Frame::Integer(0));
                }
                hash.insert(&args[2], copied(&args[3]));
                notify(ctx, HASH, "hset", &args[1]);
                return Ok(Frame::Integer(1));
            }
        }
        // The field is there but its TTL has passed: it goes first.
        db.expire_fields(&args[1], Some(&args[2..3]), now);
        hash_or_create(db, &args[1], now)?.insert(&args[2], copied(&args[3]));
        notify(ctx, HASH, "hset", &args[1]);
        Ok(Frame::Integer(1))
    })())
}

pub(crate) fn hdel(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        let Some(hash) = hash_mut(db, &args[1], now)? else {
            return Ok(Frame::Integer(0));
        };
        let removed = args[2..].iter().filter(|field| hash.remove(field)).count();
        if removed == 0 {
            db.unchanged(&args[1]);
            return Ok(Frame::Integer(0));
        }
        let emptied = db.remove_if_empty(&args[1], now);
        notify(ctx, HASH, "hdel", &args[1]);
        if emptied {
            notify(ctx, GENERIC, "del", &args[1]);
        }
        Ok(Frame::Integer(removed as i64))
    })())
}

/// The current value of `field`, read without creating the hash, so that a
/// failed increment leaves nothing behind. A field whose TTL has passed is
/// deleted, and reads as missing.
fn current_field(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Option<Bytes>, Frame> {
    let (db, now) = ctx.db();
    let (value, due) = match hash_ref(db, &args[1], now)? {
        None => (None, false),
        Some(hash) => (
            hash.get(&args[2], now).cloned(),
            hash.is_field_expired(&args[2], now),
        ),
    };
    if due {
        db.expire_fields(&args[1], Some(&args[2..3]), now);
    }
    Ok(value)
}

/// Stores the result of an increment. The field keeps any TTL it has.
/// Returns the TTL.
fn store_field(ctx: &mut Ctx<'_>, args: &[Bytes], value: Bytes) -> Result<Option<u64>, Frame> {
    let (db, now) = ctx.db();
    let hash = hash_or_create(db, &args[1], now)?;
    hash.set_value(&args[2], value);
    Ok(hash.field_expire_at(&args[2]))
}

pub(crate) fn hincrby(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let delta = int(&args[3])?;
        let current = match current_field(ctx, args)? {
            Some(value) => {
                parse_i64(&value).ok_or_else(|| Frame::error("ERR hash value is not an integer"))?
            }
            None => 0,
        };
        let updated = current
            .checked_add(delta)
            .ok_or_else(|| Frame::error("ERR increment or decrement would overflow"))?;
        store_field(ctx, args, Bytes::from(updated.to_string()))?;
        notify(ctx, HASH, "hincrby", &args[1]);
        Ok(Frame::Integer(updated))
    })())
}

pub(crate) fn hincrbyfloat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let delta =
            parse_float(&args[3]).ok_or_else(|| Frame::error("ERR value is not a valid float"))?;
        // An infinite increment is rejected up front, with its own message.
        if !delta.is_finite() {
            return Err(Frame::error("ERR value is NaN or Infinity"));
        }
        let current = match current_field(ctx, args)? {
            Some(value) => {
                parse_float(&value).ok_or_else(|| Frame::error("ERR hash value is not a float"))?
            }
            None => 0.0,
        };
        let updated = current + delta;
        if !updated.is_finite() {
            return Err(Frame::error("ERR increment would produce NaN or Infinity"));
        }
        let text = Bytes::from(updated.to_string());
        let ttl = store_field(ctx, args, text.clone())?;
        // Logged as its result: another server might round differently.
        // `HSET` removes a TTL, so one the field keeps is logged again, as
        // Redis does.
        ctx.log_instead(|| {
            let mut commands = vec![crate::aof::command(&[b"HSET", &args[1], &args[2], &text])];
            if let Some(at) = ttl {
                commands.push(crate::aof::command(&[
                    b"HPEXPIREAT",
                    &args[1],
                    at.to_string().as_bytes(),
                    b"FIELDS",
                    b"1",
                    &args[2],
                ]));
            }
            commands
        });
        notify(ctx, HASH, "hincrbyfloat", &args[1]);
        Ok(Frame::Bulk(text))
    })())
}

// --- reads ------------------------------------------------------------------

pub(crate) fn hget(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_fields(ctx, args, &args[2..3], &|| Frame::Null, |hash, now| {
        hash.get(&args[2], now)
            .cloned()
            .map_or(Frame::Null, Frame::Bulk)
    })
}

pub(crate) fn hmget(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let fields = &args[2..];
    let all_missing = || Frame::Array(vec![Frame::Null; fields.len()]);
    with_fields(ctx, args, fields, &all_missing, |hash, now| {
        Frame::Array(
            fields
                .iter()
                .map(|field| {
                    hash.get(field, now)
                        .cloned()
                        .map_or(Frame::Null, Frame::Bulk)
                })
                .collect(),
        )
    })
}

pub(crate) fn hgetall(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Map(Vec::new()), |hash, now| {
        Frame::Map(
            hash.iter(now)
                .map(|(field, value)| (Frame::Bulk(field.clone()), Frame::Bulk(value.clone())))
                .collect(),
        )
    })
}

pub(crate) fn hkeys(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Array(Vec::new()), |hash, now| {
        Frame::Array(
            hash.iter(now)
                .map(|(field, _)| Frame::Bulk(field.clone()))
                .collect(),
        )
    })
}

pub(crate) fn hvals(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Array(Vec::new()), |hash, now| {
        Frame::Array(
            hash.iter(now)
                .map(|(_, value)| Frame::Bulk(value.clone()))
                .collect(),
        )
    })
}

/// `HLEN key` — counts fields whose TTL has passed but which have not been
/// reclaimed yet, as Redis 7.4 does.
pub(crate) fn hlen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Integer(0), |hash, _| {
        Frame::Integer(hash.len() as i64)
    })
}

pub(crate) fn hexists(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_fields(
        ctx,
        args,
        &args[2..3],
        &|| Frame::Integer(0),
        |hash, now| Frame::Integer(i64::from(hash.contains(&args[2], now))),
    )
}

pub(crate) fn hstrlen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_fields(
        ctx,
        args,
        &args[2..3],
        &|| Frame::Integer(0),
        |hash, now| {
            Frame::Integer(
                hash.get(&args[2], now)
                    .map_or(0, |value| value.len() as i64),
            )
        },
    )
}

/// `HSCAN key cursor [MATCH pattern] [COUNT count]`
pub(crate) fn hscan(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let mut cursor = parse_cursor(&args[2])?;
        let options = parse_scan_options(&args[3..])?;
        let (db, now) = ctx.db();
        let mut found = Vec::new();
        if let Some(hash) = hash_ref(db, &args[1], now)? {
            let mut budget = options.count.saturating_mul(10);
            loop {
                cursor = hash.scan(cursor, now, |field, value| {
                    if options.pattern.is_none_or(|p| glob_match(p, field)) {
                        found.push(Frame::Bulk(field.clone()));
                        found.push(Frame::Bulk(value.clone()));
                    }
                });
                budget -= 1;
                if cursor == 0 || budget == 0 || found.len() / 2 >= options.count {
                    break;
                }
            }
        } else {
            cursor = 0;
        }
        Ok(Frame::Array(vec![
            Frame::bulk(cursor.to_string()),
            Frame::Array(found),
        ]))
    })())
}

/// `HRANDFIELD key [count [WITHVALUES]]`
///
/// A positive count returns distinct fields (at most the whole hash); a
/// negative count returns exactly that many, possibly repeating. Fields
/// whose TTL has passed are deleted first, as in Redis.
pub(crate) fn hrandfield(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (count, with_values) = match &args[2..] {
            [] => (None, false),
            [count] => (Some(int(count)?), false),
            [count, opt] if is(opt, "WITHVALUES") => (Some(int(count)?), true),
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        if count.is_some_and(|count| count < -(i64::MAX / 2)) {
            return Err(Frame::error("ERR value is out of range"));
        }
        let resp3 = ctx.client.proto == Protocol::Resp3;
        let (db, now) = ctx.db();
        if hash_ref(db, &args[1], now)?.is_some_and(|hash| hash.has_expired_fields(now)) {
            db.expire_fields(&args[1], None, now);
        }
        let hash = hash_peek(db, &args[1], now)?;

        let Some(count) = count else {
            return Ok(hash
                .and_then(Hash::random)
                .map_or(Frame::Null, |(field, _)| Frame::Bulk(field.clone())));
        };
        let Some(hash) = hash else {
            return Ok(Frame::Array(Vec::new()));
        };
        let picked: Vec<(&Bytes, &Bytes)> = if count >= 0 {
            sample_distinct(hash.iter(now).collect(), count as usize)
        } else {
            (0..count.unsigned_abs())
                .filter_map(|_| hash.random())
                .collect()
        };

        let bulk = |bytes: &Bytes| Frame::Bulk(bytes.clone());
        Ok(Frame::Array(if !with_values {
            picked.into_iter().map(|(field, _)| bulk(field)).collect()
        } else if resp3 {
            // RESP3 groups each field with its value.
            picked
                .into_iter()
                .map(|(field, value)| Frame::Array(vec![bulk(field), bulk(value)]))
                .collect()
        } else {
            picked
                .into_iter()
                .flat_map(|(field, value)| [bulk(field), bulk(value)])
                .collect()
        }))
    })())
}

// --- field expiry -----------------------------------------------------------

/// The latest field TTL Redis accepts, in milliseconds since the epoch.
const MAX_FIELD_EXPIRE_AT: u64 = (1 << 46) - 1;

const ERR_FIELDS_POSITION: &str =
    "ERR Mandatory argument FIELDS is missing or not at the right position";
const ERR_NUMFIELDS_MISMATCH: &str =
    "ERR The `numfields` parameter must match the number of arguments";

/// What `HEXPIRE` and its relatives do to a field's TTL only if it holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Condition {
    Always,
    /// The field has no TTL.
    Nx,
    /// The field has a TTL.
    Xx,
    /// The new TTL is later than the current one (no TTL counts as later
    /// than any).
    Gt,
    /// The new TTL is earlier than the current one.
    Lt,
}

impl Condition {
    fn holds(self, current: Option<u64>, new: u64) -> bool {
        match self {
            Condition::Always => true,
            Condition::Nx => current.is_none(),
            Condition::Xx => current.is_some(),
            Condition::Gt => current.is_some_and(|current| new > current),
            Condition::Lt => current.is_none_or(|current| new < current),
        }
    }
}

/// The fields named after `FIELDS numfields` at `args[at]`. `bad_count` is
/// the error for a `numfields` that is not a positive integer, which is
/// worded differently for the two families of commands.
fn field_list<'a>(args: &'a [Bytes], at: usize, bad_count: &str) -> Result<&'a [Bytes], Frame> {
    if !args.get(at).is_some_and(|arg| is(arg, "FIELDS")) {
        return Err(Frame::error(ERR_FIELDS_POSITION));
    }
    let count = args
        .get(at + 1)
        .and_then(|arg| parse_i64(arg))
        .filter(|&count| count > 0)
        .ok_or_else(|| Frame::error(bad_count))?;
    let fields = args.get(at + 2..).unwrap_or_default();
    if fields.len() as i64 != count {
        return Err(Frame::error(ERR_NUMFIELDS_MISMATCH));
    }
    Ok(fields)
}

/// Fails if `key` holds something other than a hash: the field-expiry
/// commands check that before anything else, as Redis does.
fn check_hash_type(ctx: &mut Ctx<'_>, key: &[u8]) -> Result<(), Frame> {
    let (db, now) = ctx.db();
    hash_peek(db, key, now).map(|_| ())
}

fn all_missing(count: usize) -> Frame {
    Frame::Array(vec![Frame::Integer(-2); count])
}

/// `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT` and `HPEXPIREAT`:
/// `key time [NX | XX | GT | LT] FIELDS numfields field [field ...]`.
/// `unit` is the milliseconds in one unit of `time`.
fn set_field_ttls(
    ctx: &mut Ctx<'_>,
    args: &[Bytes],
    name: &str,
    unit: u64,
    absolute: bool,
) -> Frame {
    reply((|| {
        let key = &args[1];
        check_hash_type(ctx, key)?;
        let time = int(&args[2])?;
        if time < 0 {
            return Err(Frame::error("ERR invalid expire time, must be >= 0"));
        }
        let now = ctx.now;
        let at = (time as u64)
            .checked_mul(unit)
            .and_then(|ms| {
                if absolute {
                    Some(ms)
                } else {
                    ms.checked_add(now)
                }
            })
            .filter(|&at| at <= MAX_FIELD_EXPIRE_AT)
            .ok_or_else(|| Frame::error(format!("ERR invalid expire time in '{name}' command")))?;
        let condition = match &args[3] {
            arg if is(arg, "NX") => Condition::Nx,
            arg if is(arg, "XX") => Condition::Xx,
            arg if is(arg, "GT") => Condition::Gt,
            arg if is(arg, "LT") => Condition::Lt,
            _ => Condition::Always,
        };
        let fields_at = if condition == Condition::Always { 3 } else { 4 };
        let fields = field_list(
            args,
            fields_at,
            "ERR Parameter `numFields` should be greater than 0",
        )?;

        // Nothing to log unless something changes (see below).
        ctx.log_instead(Vec::new);
        let (db, now) = ctx.db();
        let Some(hash) = hash_mut(db, key, now)? else {
            return Ok(all_missing(fields.len()));
        };
        let mut replies = Vec::with_capacity(fields.len());
        let (mut set, mut deleted): (Vec<&Bytes>, Vec<&Bytes>) = (Vec::new(), Vec::new());
        for field in fields {
            // A field whose TTL has passed but which is still stored counts
            // as there, as in Redis.
            let code = if !hash.contains_stored(field) {
                -2
            } else if !condition.holds(hash.field_expire_at(field), at) {
                0
            } else if at <= now {
                hash.remove(field);
                deleted.push(field);
                2
            } else {
                hash.set_field_expire(field, at);
                set.push(field);
                1
            };
            replies.push(Frame::Integer(code));
        }
        if set.is_empty() && deleted.is_empty() {
            db.unchanged(key);
        }
        if !set.is_empty() {
            db.index_field_ttls(key);
        }
        let emptied = db.remove_if_empty(key, now);
        // As in Redis: `hexpire` for TTLs set, `hdel` for fields the time
        // had already passed for, and `del` if that left the hash empty.
        if !set.is_empty() {
            notify(ctx, HASH, "hexpire", key);
        }
        if !deleted.is_empty() {
            notify(ctx, HASH, "hdel", key);
        }
        if emptied {
            notify(ctx, GENERIC, "del", key);
        }
        // Logged with an absolute time, and only for what changed: a field
        // the time had already passed for, as deleted.
        ctx.log_instead(|| {
            let mut commands = Vec::new();
            if !set.is_empty() {
                let at = at.to_string();
                let count = set.len().to_string();
                let mut command: Vec<&[u8]> = vec![
                    b"HPEXPIREAT",
                    key,
                    at.as_bytes(),
                    b"FIELDS",
                    count.as_bytes(),
                ];
                command.extend(set.iter().map(|field| &field[..]));
                commands.push(crate::aof::command(&command));
            }
            if !deleted.is_empty() {
                let mut command: Vec<&[u8]> = vec![b"HDEL", key];
                command.extend(deleted.iter().map(|field| &field[..]));
                commands.push(crate::aof::command(&command));
            }
            commands
        });
        Ok(Frame::Array(replies))
    })())
}

/// `HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field ...`
pub(crate) fn hexpire(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_field_ttls(ctx, args, "hexpire", 1000, false)
}

/// `HPEXPIRE key milliseconds [NX | XX | GT | LT] FIELDS numfields field ...`
pub(crate) fn hpexpire(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_field_ttls(ctx, args, "hpexpire", 1, false)
}

/// `HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields
/// field ...`
pub(crate) fn hexpireat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_field_ttls(ctx, args, "hexpireat", 1000, true)
}

/// `HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS
/// numfields field ...`
pub(crate) fn hpexpireat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_field_ttls(ctx, args, "hpexpireat", 1, true)
}

/// What `HTTL` and its relatives report about a field's TTL.
#[derive(Clone, Copy)]
enum Report {
    /// Seconds left, rounded up.
    Seconds,
    Milliseconds,
    /// The time itself in seconds, rounded up.
    At,
    AtMilliseconds,
}

const ERR_NUMFIELDS_POSITIVE: &str = "ERR Number of fields must be a positive integer";

/// `HTTL`, `HPTTL`, `HEXPIRETIME` and `HPEXPIRETIME`: `key FIELDS numfields
/// field [field ...]`. Per field: -2 if it does not exist (or its TTL has
/// passed), -1 if it has no TTL.
fn field_ttls(ctx: &mut Ctx<'_>, args: &[Bytes], report: Report) -> Frame {
    reply((|| {
        check_hash_type(ctx, &args[1])?;
        let fields = field_list(args, 2, ERR_NUMFIELDS_POSITIVE)?;
        let (db, now) = ctx.db();
        let Some(hash) = hash_ref(db, &args[1], now)? else {
            return Ok(all_missing(fields.len()));
        };
        Ok(Frame::Array(
            fields
                .iter()
                .map(|field| {
                    Frame::Integer(if !hash.contains(field, now) {
                        -2
                    } else {
                        match (hash.field_expire_at(field), report) {
                            (None, _) => -1,
                            (Some(at), Report::Seconds) => (at - now).div_ceil(1000) as i64,
                            (Some(at), Report::Milliseconds) => (at - now) as i64,
                            (Some(at), Report::At) => at.div_ceil(1000) as i64,
                            (Some(at), Report::AtMilliseconds) => at as i64,
                        }
                    })
                })
                .collect(),
        ))
    })())
}

/// `HTTL key FIELDS numfields field ...`
pub(crate) fn httl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    field_ttls(ctx, args, Report::Seconds)
}

/// `HPTTL key FIELDS numfields field ...`
pub(crate) fn hpttl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    field_ttls(ctx, args, Report::Milliseconds)
}

/// `HEXPIRETIME key FIELDS numfields field ...`
pub(crate) fn hexpiretime(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    field_ttls(ctx, args, Report::At)
}

/// `HPEXPIRETIME key FIELDS numfields field ...`
pub(crate) fn hpexpiretime(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    field_ttls(ctx, args, Report::AtMilliseconds)
}

/// `HPERSIST key FIELDS numfields field ...` — per field: 1 if its TTL was
/// removed, -1 if it had none, -2 if it does not exist.
pub(crate) fn hpersist(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let key = &args[1];
        check_hash_type(ctx, key)?;
        let fields = field_list(args, 2, ERR_NUMFIELDS_POSITIVE)?;
        // Nothing to log unless something changes (see below).
        ctx.log_instead(Vec::new);
        let (db, now) = ctx.db();
        let Some(hash) = hash_mut(db, key, now)? else {
            return Ok(all_missing(fields.len()));
        };
        let mut persisted: Vec<&Bytes> = Vec::new();
        let replies = fields
            .iter()
            .map(|field| {
                Frame::Integer(if !hash.contains(field, now) {
                    -2
                } else if hash.persist_field(field) {
                    persisted.push(field);
                    1
                } else {
                    -1
                })
            })
            .collect();
        if persisted.is_empty() {
            db.unchanged(key);
        } else {
            notify(ctx, HASH, "hpersist", key);
        }
        // Logged for the fields it changed only: on replay, when nothing
        // has expired, a field whose TTL had passed here would be there.
        ctx.log_instead(|| {
            if persisted.is_empty() {
                return Vec::new();
            }
            let count = persisted.len().to_string();
            let mut command: Vec<&[u8]> = vec![b"HPERSIST", key, b"FIELDS", count.as_bytes()];
            command.extend(persisted.iter().map(|field| &field[..]));
            vec![crate::aof::command(&command)]
        });
        Ok(Frame::Array(replies))
    })())
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness, WRONGTYPE};
    use bytes::Bytes;
    use rexyra_resp::Frame;
    use std::collections::HashSet;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn map(pairs: &[(&str, &str)]) -> Frame {
        Frame::Map(pairs.iter().map(|(f, v)| (bulk(f), bulk(v))).collect())
    }

    #[test]
    fn hset_hget_hdel() {
        let mut h = Harness::new();
        assert_eq!(h.run("HSET user name ann age 30"), int(2));
        assert_eq!(
            h.run("HSET user age 31 city oslo"),
            int(1),
            "only new fields count"
        );
        assert_eq!(h.run("HGET user age"), bulk("31"));
        assert_eq!(h.run("HGET user nope"), Frame::Null);
        assert_eq!(h.run("HGET missing f"), Frame::Null);
        assert_eq!(h.run("HLEN user"), int(3));
        assert_eq!(h.run("HEXISTS user name"), int(1));
        assert_eq!(h.run("HEXISTS user nope"), int(0));
        assert_eq!(h.run("HSTRLEN user name"), int(3));
        assert_eq!(h.run("HSTRLEN user nope"), int(0));
        assert_eq!(h.run("TYPE user"), Frame::simple("hash"));

        assert_eq!(h.run("HDEL user age nope city"), int(2));
        assert_eq!(h.run("HDEL missing f"), int(0));
        assert_eq!(h.run("HDEL user name"), int(1));
        assert_eq!(h.run("EXISTS user"), int(0), "an emptied hash is deleted");
    }

    #[test]
    fn bulk_reads_keep_insertion_order() {
        let mut h = Harness::new();
        h.run("HSET h c 3 a 1 b 2");
        assert_eq!(
            h.run("HGETALL h"),
            map(&[("c", "3"), ("a", "1"), ("b", "2")])
        );
        assert_eq!(h.run("HKEYS h"), Frame::Array(bulks(&["c", "a", "b"])));
        assert_eq!(h.run("HVALS h"), Frame::Array(bulks(&["3", "1", "2"])));
        assert_eq!(
            h.run("HMGET h a nope c"),
            Frame::Array(vec![bulk("1"), Frame::Null, bulk("3")])
        );
        assert_eq!(h.run("HGETALL missing"), map(&[]));
        assert_eq!(h.run("HKEYS missing"), Frame::Array(vec![]));
        assert_eq!(
            h.run("HMGET missing a b"),
            Frame::Array(vec![Frame::Null, Frame::Null])
        );
    }

    #[test]
    fn hmset_and_hsetnx() {
        let mut h = Harness::new();
        assert_eq!(h.run("HMSET h a 1 b 2"), Frame::ok());
        assert_eq!(h.run("HSETNX h a 9"), int(0));
        assert_eq!(h.run("HSETNX h c 3"), int(1));
        assert_eq!(
            h.run("HGETALL h"),
            map(&[("a", "1"), ("b", "2"), ("c", "3")])
        );
        assert_eq!(
            h.run("HSET h a"),
            err("ERR wrong number of arguments for 'hset' command")
        );
        assert_eq!(
            h.run("HSET h a 1 b"),
            err("ERR wrong number of arguments for 'hset' command")
        );
        assert_eq!(
            h.run("HMSET h a 1 b"),
            err("ERR wrong number of arguments for 'hmset' command")
        );
    }

    #[test]
    fn hincrby() {
        let mut h = Harness::new();
        assert_eq!(h.run("HINCRBY h n 5"), int(5));
        assert_eq!(h.run("HINCRBY h n -7"), int(-2));
        assert_eq!(h.run("HGET h n"), bulk("-2"));
        h.run("HSET h s abc max 9223372036854775807");
        assert_eq!(
            h.run("HINCRBY h s 1"),
            err("ERR hash value is not an integer")
        );
        assert_eq!(
            h.run("HINCRBY h max 1"),
            err("ERR increment or decrement would overflow")
        );
        assert_eq!(
            h.run("HINCRBY h n x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            h.run("HINCRBY fresh n x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            h.run("EXISTS fresh"),
            int(0),
            "a failed command creates nothing"
        );
    }

    #[test]
    fn hincrbyfloat() {
        let mut h = Harness::new();
        assert_eq!(h.run("HINCRBYFLOAT h f 10.5"), bulk("10.5"));
        assert_eq!(h.run("HINCRBYFLOAT h f -0.5"), bulk("10"));
        h.run("HSET h s abc");
        assert_eq!(
            h.run("HINCRBYFLOAT h s 1"),
            err("ERR hash value is not a float")
        );
        assert_eq!(
            h.run("HINCRBYFLOAT h f x"),
            err("ERR value is not a valid float")
        );
        assert_eq!(
            h.run("HINCRBYFLOAT fresh f inf"),
            err("ERR value is NaN or Infinity")
        );
        assert_eq!(h.run("EXISTS fresh"), int(0));
        h.run("HSET h big 1e308");
        assert_eq!(
            h.run("HINCRBYFLOAT h big 1e308"),
            err("ERR increment would produce NaN or Infinity")
        );
    }

    #[test]
    fn large_hashes_still_work() {
        let mut h = Harness::new();
        for i in 0..600 {
            h.cmd(&["HSET", "big", &format!("f{i}"), &i.to_string()]);
        }
        assert_eq!(h.run("HLEN big"), int(600));
        assert_eq!(h.run("HGET big f321"), bulk("321"));
        assert_eq!(h.run("OBJECT ENCODING big"), bulk("hashtable"));
        h.run("HSET small a 1");
        assert_eq!(h.run("OBJECT ENCODING small"), bulk("listpack"));

        let mut cursor = "0".to_owned();
        let mut seen = HashSet::new();
        loop {
            let Frame::Array(reply) = h.run(&format!("HSCAN big {cursor} COUNT 50")) else {
                panic!("expected array");
            };
            let Frame::Bulk(next) = &reply[0] else {
                panic!("cursor")
            };
            let Frame::Array(items) = &reply[1] else {
                panic!("items")
            };
            assert_eq!(items.len() % 2, 0);
            for pair in items.chunks(2) {
                seen.insert(format!("{:?}", pair[0]));
            }
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), 600);
    }

    #[test]
    fn hscan_small_and_filtered() {
        let mut h = Harness::new();
        h.run("HSET h a1 1 b1 2 a2 3");
        assert_eq!(
            h.run("HSCAN h 0"),
            Frame::Array(vec![
                bulk("0"),
                Frame::Array(bulks(&["a1", "1", "b1", "2", "a2", "3"]))
            ])
        );
        assert_eq!(
            h.run("HSCAN h 0 MATCH a*"),
            Frame::Array(vec![
                bulk("0"),
                Frame::Array(bulks(&["a1", "1", "a2", "3"]))
            ])
        );
        assert_eq!(
            h.run("HSCAN missing 0"),
            Frame::Array(vec![bulk("0"), Frame::Array(vec![])])
        );
        assert_eq!(h.run("HSCAN h x"), err("ERR invalid cursor"));
        assert_eq!(h.run("HSCAN h 0 COUNT 0"), err("ERR syntax error"));
    }

    #[test]
    fn hrandfield() {
        let mut h = Harness::new();
        assert_eq!(h.run("HRANDFIELD missing"), Frame::Null);
        assert_eq!(h.run("HRANDFIELD missing 3"), Frame::Array(vec![]));
        h.run("HSET h a 1 b 2 c 3");

        let one = h.run("HRANDFIELD h");
        assert!(bulks(&["a", "b", "c"]).contains(&one));

        let Frame::Array(distinct) = h.run("HRANDFIELD h 2") else {
            panic!()
        };
        assert_eq!(distinct.len(), 2);
        assert_ne!(distinct[0], distinct[1]);

        assert_eq!(
            h.run("HRANDFIELD h 10"),
            Frame::Array(bulks(&["a", "b", "c"]))
        );
        assert_eq!(h.run("HRANDFIELD h 0"), Frame::Array(vec![]));

        let Frame::Array(repeating) = h.run("HRANDFIELD h -10") else {
            panic!()
        };
        assert_eq!(repeating.len(), 10);

        assert_eq!(
            h.run("HRANDFIELD h 10 WITHVALUES"),
            Frame::Array(bulks(&["a", "1", "b", "2", "c", "3"]))
        );
        h.run("HELLO 3");
        assert_eq!(
            h.run("HRANDFIELD h 10 WITHVALUES"),
            Frame::Array(vec![
                Frame::Array(bulks(&["a", "1"])),
                Frame::Array(bulks(&["b", "2"])),
                Frame::Array(bulks(&["c", "3"])),
            ])
        );
        assert_eq!(h.run("HRANDFIELD h 1 BOGUS"), err("ERR syntax error"));
    }

    #[test]
    fn wrong_type() {
        let mut h = Harness::new();
        h.run("SET s v");
        for command in [
            "HSET s f v",
            "HGET s f",
            "HDEL s f",
            "HGETALL s",
            "HLEN s",
            "HINCRBY s f 1",
            "HINCRBYFLOAT s f 1",
            "HSETNX s f v",
            "HMGET s f",
            "HSCAN s 0",
            "HRANDFIELD s",
            "HEXISTS s f",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
        assert_eq!(h.run("GET s"), bulk("v"));

        h.run("HSET h f v");
        for command in [
            "GET h",
            "APPEND h x",
            "INCR h",
            "STRLEN h",
            "GETRANGE h 0 1",
            "SETBIT h 0 1",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
        // SET replaces any type; MGET treats other types as missing.
        assert_eq!(
            h.run("MGET h s"),
            Frame::Array(vec![Frame::Null, bulk("v")])
        );
        assert_eq!(h.run("SET h now-a-string"), Frame::ok());
        assert_eq!(h.run("TYPE h"), Frame::simple("string"));
    }

    // --- field expiry ---------------------------------------------------------
    //
    // Replies and errors compared with Redis 7.4.12, in RESP2 and RESP3.

    fn ints(values: &[i64]) -> Frame {
        Frame::Array(values.iter().map(|&n| Frame::Integer(n)).collect())
    }

    fn keys_of(h: &mut Harness, key: &str) -> Frame {
        h.cmd(&["HKEYS", key])
    }

    fn info(h: &mut Harness, section: &str) -> String {
        match h.cmd(&["INFO", section]) {
            Frame::Bulk(data) | Frame::Verbatim { data, .. } => {
                String::from_utf8(data.to_vec()).unwrap()
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hexpire_replies_per_field() {
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2 c 3");
        assert_eq!(h.run("HEXPIRE h 100 FIELDS 2 a zz"), ints(&[1, -2]));
        assert_eq!(h.run("HEXPIRE nokey 100 FIELDS 2 a b"), ints(&[-2, -2]));
        assert_eq!(h.run("EXISTS nokey"), int(0));
        assert_eq!(h.run("HTTL h FIELDS 3 a b nope"), ints(&[100, -1, -2]));
        assert_eq!(h.run("HPTTL h FIELDS 1 a"), ints(&[100_000]));
        let at = (h.now + 100_000) as i64;
        assert_eq!(h.run("HPEXPIRETIME h FIELDS 2 a b"), ints(&[at, -1]));
        assert_eq!(h.run("HEXPIRETIME h FIELDS 1 a"), ints(&[at / 1000]));
        assert_eq!(h.run("HTTL nokey FIELDS 2 a b"), ints(&[-2, -2]));
        assert_eq!(h.run("HPERSIST h FIELDS 3 a b nope"), ints(&[1, -1, -2]));
        assert_eq!(h.run("HPERSIST nokey FIELDS 1 a"), ints(&[-2]));
        assert_eq!(h.run("HTTL h FIELDS 1 a"), ints(&[-1]));
        // A field named twice is answered twice.
        assert_eq!(h.run("HEXPIRE h 100 FIELDS 2 a a"), ints(&[1, 1]));
        assert_eq!(h.run("HTTL h FIELDS 2 a a"), ints(&[100, 100]));
        // RESP3 replies are the same arrays.
        h.run("HELLO 3");
        assert_eq!(h.run("HTTL h FIELDS 2 a nope"), ints(&[100, -2]));
        assert_eq!(h.run("HPERSIST h FIELDS 1 nope"), ints(&[-2]));
    }

    #[test]
    fn units_and_rounding() {
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2 c 3");
        assert_eq!(h.run("HPEXPIRE h 10300 FIELDS 1 a"), ints(&[1]));
        assert_eq!(h.run("HPEXPIRE h 10700 FIELDS 1 b"), ints(&[1]));
        // Seconds left are rounded up.
        assert_eq!(h.run("HTTL h FIELDS 2 a b"), ints(&[11, 11]));
        assert_eq!(h.run("HPTTL h FIELDS 2 a b"), ints(&[10_300, 10_700]));
        for (at, seconds) in [
            (1_800_000_000_000i64, 1_800_000_000i64),
            (1_800_000_000_001, 1_800_000_001),
            (1_800_000_000_499, 1_800_000_001),
            (1_800_000_000_999, 1_800_000_001),
        ] {
            h.cmd(&["HPEXPIREAT", "h", &at.to_string(), "FIELDS", "1", "c"]);
            assert_eq!(h.run("HEXPIRETIME h FIELDS 1 c"), ints(&[seconds]));
            assert_eq!(h.run("HPEXPIRETIME h FIELDS 1 c"), ints(&[at]));
        }
        assert_eq!(h.run("HEXPIREAT h 1800000000 FIELDS 1 c"), ints(&[1]));
        assert_eq!(
            h.run("HPEXPIRETIME h FIELDS 1 c"),
            ints(&[1_800_000_000_000])
        );
    }

    #[test]
    fn conditions() {
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2");
        let t = |offset: i64| (1_800_000_000_000 + offset).to_string();
        let set = |h: &mut Harness, at: &str, condition: &str, field: &str| {
            let mut args = vec!["HPEXPIREAT", "h", at];
            if !condition.is_empty() {
                args.push(condition);
            }
            args.extend(["FIELDS", "1", field]);
            h.cmd(&args)
        };
        // Without a TTL: NX and LT hold, XX and GT do not.
        assert_eq!(set(&mut h, &t(0), "XX", "a"), ints(&[0]));
        assert_eq!(set(&mut h, &t(0), "GT", "a"), ints(&[0]));
        assert_eq!(set(&mut h, &t(0), "LT", "a"), ints(&[1]));
        assert_eq!(set(&mut h, &t(0), "NX", "b"), ints(&[1]));
        // With one.
        assert_eq!(set(&mut h, &t(5), "NX", "a"), ints(&[0]));
        assert_eq!(set(&mut h, &t(5), "XX", "a"), ints(&[1]));
        assert_eq!(set(&mut h, &t(5), "GT", "a"), ints(&[0]), "not later");
        assert_eq!(set(&mut h, &t(6), "gt", "a"), ints(&[1]));
        assert_eq!(set(&mut h, &t(6), "LT", "a"), ints(&[0]), "not earlier");
        assert_eq!(set(&mut h, &t(4), "lt", "a"), ints(&[1]));
        assert_eq!(
            h.run("HPEXPIRETIME h FIELDS 1 a"),
            ints(&[1_800_000_000_004])
        );
        // A time already passed deletes the field, if the condition holds.
        assert_eq!(set(&mut h, "1", "GT", "a"), ints(&[0]));
        assert_eq!(set(&mut h, "1", "NX", "a"), ints(&[0]));
        assert_eq!(set(&mut h, "1", "LT", "a"), ints(&[2]));
        assert_eq!(h.run("HEXISTS h a"), int(0));
        assert_eq!(h.run("HPEXPIRE h 0 FIELDS 2 b b"), ints(&[2, -2]));
        assert_eq!(h.run("EXISTS h"), int(0), "its last field went");
    }

    #[test]
    fn argument_errors_in_redis_order() {
        let mut h = Harness::new();
        h.run("HSET h a 1");
        h.run("SET s v");
        let cases: &[(&str, &str)] = &[
            // The type is checked first.
            ("HEXPIRE s -1 FIELDS 1 a", WRONGTYPE),
            ("HEXPIRE s 100 foo 1 a", WRONGTYPE),
            ("HTTL s foo 0 a", WRONGTYPE),
            ("HPERSIST s FIELDS 2 a", WRONGTYPE),
            // Then the time.
            (
                "HEXPIRE h abc FIELDS 1 a",
                "ERR value is not an integer or out of range",
            ),
            (
                "HEXPIRE h 00100 FIELDS 1 a",
                "ERR value is not an integer or out of range",
            ),
            (
                "HEXPIRE h 1.5 FIELDS 1 a",
                "ERR value is not an integer or out of range",
            ),
            (
                "HEXPIRE h 9223372036854775808 FIELDS 1 a",
                "ERR value is not an integer or out of range",
            ),
            (
                "HEXPIRE h -1 FIELDS 2 a",
                "ERR invalid expire time, must be >= 0",
            ),
            (
                "HEXPIRE nokey -1 foo 1 a",
                "ERR invalid expire time, must be >= 0",
            ),
            (
                "HEXPIRE h 9223372036854775807 foo 1 a",
                "ERR invalid expire time in 'hexpire' command",
            ),
            (
                "HEXPIRE h 9223372036854775 FIELDS 1 a",
                "ERR invalid expire time in 'hexpire' command",
            ),
            (
                "HPEXPIRE h 9223372036854775807 FIELDS 1 a",
                "ERR invalid expire time in 'hpexpire' command",
            ),
            (
                "HEXPIREAT h 70368744178 FIELDS 1 a",
                "ERR invalid expire time in 'hexpireat' command",
            ),
            (
                "HPEXPIREAT h 70368744177664 FIELDS 1 a",
                "ERR invalid expire time in 'hpexpireat' command",
            ),
            // Then the place of FIELDS, after at most one condition.
            (
                "HEXPIRE h 100 NX XX FIELDS 1 a",
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ),
            (
                "HEXPIRE h 100 bogus FIELDS 1 a",
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ),
            (
                "HEXPIRE nokey 100 foo 1 a",
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ),
            (
                "HTTL nokey foo 1 a",
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ),
            // Then the count, with two wordings.
            (
                "HEXPIRE h 100 FIELDS 0 a",
                "ERR Parameter `numFields` should be greater than 0",
            ),
            (
                "HEXPIRE h 100 FIELDS -1 a",
                "ERR Parameter `numFields` should be greater than 0",
            ),
            (
                "HEXPIRE h 100 FIELDS +1 a",
                "ERR Parameter `numFields` should be greater than 0",
            ),
            (
                "HEXPIRE h 100 NX FIELDS x",
                "ERR Parameter `numFields` should be greater than 0",
            ),
            (
                "HEXPIRE h 100 FIELDS 99999999999999999999 a",
                "ERR Parameter `numFields` should be greater than 0",
            ),
            (
                "HTTL h FIELDS 0 a",
                "ERR Number of fields must be a positive integer",
            ),
            (
                "HTTL h FIELDS abc a",
                "ERR Number of fields must be a positive integer",
            ),
            (
                "HPERSIST h FIELDS 0 a",
                "ERR Number of fields must be a positive integer",
            ),
            // Then the number of fields.
            (
                "HEXPIRE h 100 FIELDS 2 a",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HEXPIRE h 100 FIELDS 1 a b",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HEXPIRE h 100 FIELDS 1 a NX",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HEXPIRE h 100 NX FIELDS 1",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HEXPIRE nokey 100 FIELDS 2147483648 a",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HTTL nokey FIELDS 2 a",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            (
                "HPERSIST h FIELDS 2 a",
                "ERR The `numfields` parameter must match the number of arguments",
            ),
            // Arity.
            (
                "HEXPIRE h 100 FIELDS 1",
                "ERR wrong number of arguments for 'hexpire' command",
            ),
            (
                "HPEXPIREAT h 1",
                "ERR wrong number of arguments for 'hpexpireat' command",
            ),
            (
                "HTTL h FIELDS 1",
                "ERR wrong number of arguments for 'httl' command",
            ),
            (
                "HPERSIST h FIELDS 1",
                "ERR wrong number of arguments for 'hpersist' command",
            ),
        ];
        for (command, error) in cases {
            assert_eq!(h.run(command), err(error), "{command}");
        }
        // The largest time accepted.
        assert_eq!(h.run("HPEXPIREAT h 70368744177663 FIELDS 1 a"), ints(&[1]));
        assert_eq!(h.run("HEXPIREAT h 70368744177 FIELDS 1 a"), ints(&[1]));
        // Nothing failed changed anything.
        assert_eq!(h.run("HGETALL h"), map(&[("a", "1")]));
        assert_eq!(h.run("EXISTS nokey"), int(0));
    }

    #[test]
    fn fields_with_a_ttl_come_first_in_a_small_hash() {
        // The orders Redis 7.4 shows for the same commands.
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2 c 3 d 4 e 5");
        h.run("HPEXPIREAT h 1800000000100 FIELDS 1 c");
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["c", "a", "b", "d", "e"]))
        );
        h.run("HPEXPIREAT h 1800000000100 FIELDS 1 e");
        h.run("HPEXPIREAT h 1800000000050 FIELDS 1 d");
        h.run("HPEXPIREAT h 1800000000100 FIELDS 1 a");
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["d", "a", "e", "c", "b"]))
        );
        h.run("HPEXPIREAT h 1800000000200 FIELDS 3 b a c");
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["d", "e", "c", "a", "b"]))
        );
        h.run("HPERSIST h FIELDS 1 a");
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["d", "e", "c", "b", "a"]))
        );
        // HSET removes a TTL; HINCRBY and HINCRBYFLOAT keep it.
        h.run("HSET h b 22");
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["d", "e", "c", "a", "b"]))
        );
        assert_eq!(h.run("HPEXPIRETIME h FIELDS 1 b"), ints(&[-1]));
        h.run("HPEXPIREAT h 1800000000300 FIELDS 1 b");
        assert_eq!(h.run("HINCRBY h b 1"), int(23));
        assert_eq!(h.run("HINCRBYFLOAT h b 1.5"), bulk("24.5"));
        assert_eq!(
            h.run("HPEXPIRETIME h FIELDS 1 b"),
            ints(&[1_800_000_000_300])
        );
        assert_eq!(
            keys_of(&mut h, "h"),
            Frame::Array(bulks(&["d", "e", "c", "b", "a"]))
        );
        assert_eq!(
            h.run("HGETALL h"),
            map(&[
                ("d", "4"),
                ("e", "5"),
                ("c", "3"),
                ("b", "24.5"),
                ("a", "1")
            ])
        );
        assert_eq!(h.run("OBJECT ENCODING h"), bulk("listpackex"));
        h.run("HPERSIST h FIELDS 3 b c d");
        h.run("HPERSIST h FIELDS 1 e");
        assert_eq!(h.run("OBJECT ENCODING h"), bulk("listpackex"), "for good");
        h.cmd(&["HSET", "h", "long", &"x".repeat(65)]);
        assert_eq!(h.run("OBJECT ENCODING h"), bulk("hashtable"));
        h.run("HSET plain a 1");
        assert_eq!(h.run("OBJECT ENCODING plain"), bulk("listpack"));
    }

    /// Fields past their TTL that nothing has reclaimed yet, as Redis 7.4
    /// treats them (`DEBUG SET-ACTIVE-EXPIRE 0` there).
    #[test]
    fn expired_fields_before_they_are_reclaimed() {
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2 c 3 d 4 e 5 f 6 k 9");
        assert_eq!(h.run("HPEXPIRE h 20 FIELDS 6 a b c d e f"), ints(&[1; 6]));
        h.advance(100);
        // Bulk reads leave them out but do not delete them, and HLEN counts
        // them.
        assert_eq!(h.run("HGETALL h"), map(&[("k", "9")]));
        assert_eq!(h.run("HKEYS h"), Frame::Array(bulks(&["k"])));
        assert_eq!(h.run("HVALS h"), Frame::Array(bulks(&["9"])));
        assert_eq!(
            h.run("HSCAN h 0"),
            Frame::Array(vec![bulk("0"), Frame::Array(bulks(&["k", "9"]))])
        );
        assert_eq!(h.run("HLEN h"), int(7));
        assert_eq!(h.run("HTTL h FIELDS 2 a k"), ints(&[-2, -1]));
        assert_eq!(h.run("HPERSIST h FIELDS 1 a"), ints(&[-2]));
        assert_eq!(h.run("HLEN h"), int(7));
        // A lookup of a single field deletes it.
        assert_eq!(h.run("HGET h a"), Frame::Null);
        assert_eq!(h.run("HLEN h"), int(6));
        assert_eq!(h.run("HEXISTS h b"), int(0));
        assert_eq!(h.run("HSTRLEN h c"), int(0));
        assert_eq!(h.run("HLEN h"), int(4));
        // HSET counts it as there, HSETNX does not; both clear its TTL.
        assert_eq!(h.run("HSET h d 40"), int(0));
        assert_eq!(h.run("HSETNX h e 50"), int(1));
        assert_eq!(h.run("HPEXPIRETIME h FIELDS 2 d e"), ints(&[-1, -1]));
        // An increment starts from nothing.
        h.run("HSET g x 5 y 1");
        h.run("HPEXPIRE g 20 FIELDS 1 x");
        h.advance(100);
        assert_eq!(h.run("HINCRBY g x 1"), int(1));
        assert_eq!(h.run("HPEXPIRETIME g FIELDS 1 x"), ints(&[-1]));
        // HDEL counts it as deleted; HEXPIRE can give it a new TTL.
        h.run("HSET p a 1 b 2 c 3");
        h.run("HPEXPIRE p 20 FIELDS 2 a b");
        h.advance(100);
        assert_eq!(h.run("HDEL p a"), int(1));
        assert_eq!(h.run("HEXPIRE p 100 FIELDS 1 b"), ints(&[1]));
        assert_eq!(h.run("HGET p b"), bulk("2"));
        // HRANDFIELD deletes them all first.
        h.run("HSET r a 1 b 2 c 3");
        h.run("HPEXPIRE r 20 FIELDS 2 a b");
        h.advance(100);
        assert_eq!(h.run("HLEN r"), int(3));
        assert_eq!(h.run("HRANDFIELD r"), bulk("c"));
        assert_eq!(h.run("HLEN r"), int(1));
        // A hash none of whose fields is left still exists until reclaimed
        // or looked at.
        h.run("HSET gone a 1 b 2");
        h.run("HPEXPIRE gone 20 FIELDS 2 a b");
        h.advance(100);
        assert_eq!(h.run("EXISTS gone"), int(1));
        assert_eq!(h.run("TYPE gone"), Frame::simple("hash"));
        assert_eq!(h.run("HGETALL gone"), map(&[]));
        assert_eq!(h.run("HMGET gone a b"), Frame::Array(vec![Frame::Null; 2]));
        assert_eq!(h.run("EXISTS gone"), int(0), "the lookup deleted both");
        h.run("HSET gone2 a 1");
        h.run("HPEXPIRE gone2 20 FIELDS 1 a");
        h.advance(100);
        assert_eq!(h.run("HRANDFIELD gone2"), Frame::Null);
        assert_eq!(h.run("HRANDFIELD gone2 5"), Frame::Array(vec![]));
        assert_eq!(h.run("EXISTS gone2"), int(0));
    }

    #[test]
    fn expired_fields_are_reclaimed_in_the_background() {
        let mut h = Harness::new();
        h.run("CONFIG RESETSTAT");
        h.run("HSET h a 1 b 2 c 3");
        h.run("HSET whole x 1");
        h.run("HPEXPIRE h 50 FIELDS 2 a b");
        h.run("HPEXPIRE whole 70 FIELDS 1 x");

        assert!(info(&mut h, "keyspace").contains("db0:keys=2,expires=0,avg_ttl=0,subexpiry=2"));
        h.advance(60);
        h.keyspace.cron(h.now, 100);
        assert_eq!(h.run("HLEN h"), int(1));
        assert_eq!(h.run("EXISTS whole"), int(1));
        assert!(info(&mut h, "keyspace").contains("subexpiry=1"));
        h.advance(60);
        h.keyspace.cron(h.now, 100);
        assert_eq!(
            h.run("EXISTS whole"),
            int(0),
            "a hash whose last field expired"
        );
        assert!(info(&mut h, "stats").contains("expired_subkeys:3\r\n"));
        assert!(info(&mut h, "keyspace").contains("db0:keys=1,expires=0,avg_ttl=0,subexpiry=0"));
        h.run("CONFIG RESETSTAT");
        assert!(info(&mut h, "stats").contains("expired_subkeys:0\r\n"));
    }

    #[test]
    fn a_hash_whose_ttls_were_removed_is_counted_until_reclaimed() {
        // As in Redis: `subexpiry` follows the reclaimer, not HPERSIST.
        let mut h = Harness::new();
        h.run("HSET h a 1");
        h.run("HPEXPIRE h 100 FIELDS 1 a");
        h.run("HPERSIST h FIELDS 1 a");
        assert!(info(&mut h, "keyspace").ends_with("subexpiry=1\r\n"));
        h.advance(200);
        h.keyspace.cron(h.now, 100);
        assert!(info(&mut h, "keyspace").ends_with("subexpiry=0\r\n"));
        assert_eq!(h.run("HGET h a"), bulk("1"));
    }

    #[test]
    fn a_hash_of_many_expiring_fields_is_reclaimed_a_part_at_a_time() {
        let mut h = Harness::new();
        let mut args = vec!["HSET".to_owned(), "big".to_owned()];
        for n in 0..5_000 {
            args.push(format!("f{n}"));
            args.push("v".to_owned());
        }
        h.cmd(&args.iter().map(String::as_str).collect::<Vec<_>>());
        let mut expire = vec!["HPEXPIRE", "big", "10", "FIELDS", "4999"];
        let names: Vec<String> = (1..5_000).map(|n| format!("f{n}")).collect();
        expire.extend(names.iter().map(String::as_str));
        assert_eq!(h.cmd(&expire), ints(&[1; 4999]));
        h.advance(20);
        for left in [4_000, 3_000, 2_000, 1_000, 1] {
            h.keyspace.cron(h.now, 1_000);
            assert_eq!(h.run("HLEN big"), int(left));
        }
        assert_eq!(h.run("HGETALL big"), map(&[("f0", "v")]));
    }

    #[test]
    fn watchers_see_fields_expire() {
        let mut h = Harness::new();
        let transaction = |h: &mut Harness, key: &str| {
            h.cmd(&["WATCH", key]);
            h.run("MULTI");
            h.run("PING");
            h.run("EXEC")
        };
        // Reclaimed in the background, the hash staying ...
        h.run("HSET w a 1 b 2");
        h.run("HPEXPIRE w 20 FIELDS 1 a");
        h.run("WATCH w");
        h.advance(50);
        h.keyspace.cron(h.now, 100);
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
        // ... or going; or deleted by a lookup.
        h.run("HSET w2 a 1");
        h.run("HPEXPIRE w2 20 FIELDS 1 a");
        h.run("WATCH w2");
        h.advance(50);
        h.keyspace.cron(h.now, 100);
        h.run("MULTI");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
        h.run("HSET w3 a 1 b 2");
        h.run("HPEXPIRE w3 20 FIELDS 1 a");
        h.advance(50);
        h.run("WATCH w3");
        h.run("HGET w3 a");
        h.run("MULTI");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
        // Conditions that change nothing are no change.
        h.run("HSET w4 a 1");
        h.run("WATCH w4");
        h.run("HEXPIRE w4 100 XX FIELDS 1 a");
        h.run("HPERSIST w4 FIELDS 1 a");
        h.run("HTTL w4 FIELDS 1 a");
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::simple("PONG")]));
        assert_eq!(
            transaction(&mut h, "w4"),
            Frame::Array(vec![Frame::simple("PONG")])
        );
    }

    #[test]
    fn field_ttls_move_with_the_key() {
        let mut h = Harness::new();
        h.run("HSET k a 1 b 2");
        h.run("HPEXPIREAT k 1800000000000 FIELDS 1 a");
        h.run("RENAME k k2");
        assert_eq!(
            h.run("HPEXPIRETIME k2 FIELDS 2 a b"),
            ints(&[1_800_000_000_000, -1])
        );
        assert_eq!(h.run("COPY k2 k3"), int(1));
        assert_eq!(h.run("OBJECT ENCODING k3"), bulk("listpackex"));
        assert_eq!(h.run("MOVE k3 1"), int(1));
        h.run("SELECT 1");
        assert_eq!(
            h.run("HPEXPIRETIME k3 FIELDS 1 a"),
            ints(&[1_800_000_000_000])
        );
        h.run("SWAPDB 0 1");
        assert_eq!(
            h.run("HPEXPIRETIME k2 FIELDS 1 a"),
            ints(&[1_800_000_000_000])
        );
        h.run("SELECT 0");
        // The copies each go when their time comes.
        h.now = 1_800_000_000_000;
        h.keyspace.cron(h.now, 100);
        assert_eq!(h.run("HGETALL k3"), map(&[("b", "2")]));
        assert_eq!(h.run("HLEN k3"), int(1));
        h.run("SELECT 1");
        assert_eq!(h.run("HLEN k2"), int(1));
        // A key replaced by another value takes its field TTLs with it.
        h.run("HSET r a 1");
        h.run("HPEXPIRE r 10 FIELDS 1 a");
        h.run("SET r now-a-string");
        h.run("DEL k2");
        let keyspace = info(&mut h, "keyspace");
        assert!(!keyspace.contains("subexpiry=1"), "{keyspace}");
        h.advance(100);
        h.keyspace.cron(h.now, 100);
        assert_eq!(h.run("GET r"), bulk("now-a-string"));
    }

    #[test]
    fn dump_and_restore_keep_field_ttls() {
        let mut h = Harness::new();
        h.run("HSET h a 1 b 2 c 3");
        h.run("HPEXPIREAT h 1800000000000 FIELDS 1 a");
        let Frame::Bulk(dumped) = h.run("DUMP h") else {
            panic!("no payload")
        };
        // The version Redis 7.4 writes for such a value.
        assert_eq!(&dumped[dumped.len() - 10..][..2], &[12, 0]);
        let restore = |h: &mut Harness, key: &str, payload: &[u8]| {
            h.raw(&[
                Bytes::from_static(b"RESTORE"),
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::from_static(b"0"),
                Bytes::copy_from_slice(payload),
            ])
        };
        assert_eq!(restore(&mut h, "copy", &dumped), Frame::ok());
        assert_eq!(
            h.run("HPEXPIRETIME copy FIELDS 3 a b c"),
            ints(&[1_800_000_000_000, -1, -1])
        );
        assert_eq!(h.run("HKEYS copy"), Frame::Array(bulks(&["a", "b", "c"])));
        // Restored later, the field whose time has passed is left out ...
        h.now = 1_800_000_000_000;
        assert_eq!(restore(&mut h, "later", &dumped), Frame::ok());
        assert_eq!(h.run("HGETALL later"), map(&[("b", "2"), ("c", "3")]));
        assert_eq!(h.run("HLEN later"), int(2));
        // ... and a hash with none left is not created.
        h.now = 1_700_000_000_000;
        h.run("HSET only a 1");
        h.run("HPEXPIREAT only 1800000000000 FIELDS 1 a");
        let Frame::Bulk(only) = h.run("DUMP only") else {
            panic!()
        };
        h.now = 1_800_000_000_000;
        assert_eq!(restore(&mut h, "none", &only), Frame::ok());
        assert_eq!(h.run("EXISTS none"), int(0));
    }

    #[test]
    fn sort_skips_expired_fields() {
        let mut h = Harness::new();
        h.run("RPUSH l 1 2 3");
        h.run("HSET w_1 f 30");
        h.run("HSET w_2 f 10");
        h.run("HSET w_3 f 20");
        assert_eq!(
            h.run("SORT l BY w_*->f"),
            Frame::Array(bulks(&["2", "3", "1"]))
        );
        h.run("HPEXPIRE w_2 10 FIELDS 1 f");
        h.advance(20);
        assert_eq!(
            h.run("SORT l BY w_*->f GET w_*->f"),
            Frame::Array(vec![Frame::Null, bulk("20"), bulk("30")])
        );
    }

    #[test]
    fn field_expiry_from_scripts_and_transactions() {
        let mut h = Harness::new();
        h.run("HSET h a 1");
        assert_eq!(
            h.cmd(&[
                "EVAL",
                "return redis.call('HEXPIRE', KEYS[1], 100, 'FIELDS', 1, 'a')",
                "1",
                "h"
            ]),
            ints(&[1])
        );
        h.run("MULTI");
        h.run("HPERSIST h FIELDS 1 a");
        h.run("HTTL h FIELDS 1 a");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![ints(&[1]), ints(&[-1])]));
    }

    #[test]
    fn memory_usage_counts_field_ttls() {
        let mut h = Harness::new();
        h.run("HSET plain a 1 b 2");
        h.run("HSET ttls a 1 b 2");
        h.run("HPEXPIRE ttls 1000 FIELDS 2 a b");
        let (Frame::Integer(plain), Frame::Integer(ttls)) =
            (h.run("MEMORY USAGE plain"), h.run("MEMORY USAGE ttls"))
        else {
            panic!("not numbers")
        };
        assert!(ttls > plain, "{ttls} > {plain}");
    }

    #[test]
    fn field_expiry_wrong_type() {
        let mut h = Harness::new();
        h.run("SET s v");
        for command in [
            "HEXPIRE s 100 FIELDS 1 a",
            "HPEXPIRE s 100 FIELDS 1 a",
            "HEXPIREAT s 100 FIELDS 1 a",
            "HPEXPIREAT s 100 FIELDS 1 a",
            "HTTL s FIELDS 1 a",
            "HPTTL s FIELDS 1 a",
            "HEXPIRETIME s FIELDS 1 a",
            "HPEXPIRETIME s FIELDS 1 a",
            "HPERSIST s FIELDS 1 a",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
    }
}
