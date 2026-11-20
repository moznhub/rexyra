//! String commands.

use bytes::{Bytes, BytesMut};
use rexyra_engine::{Db, SetTtl, Value};
use rexyra_resp::{parse_i64, Frame, MAX_BULK_LEN};

use crate::{int, is, reply, wrong_arity, wrong_type, Ctx, ERR_NOT_INTEGER, ERR_SYNTAX};

const ERR_NOT_FLOAT: &str = "ERR value is not a valid float";
const ERR_OVERFLOW: &str = "ERR increment or decrement would overflow";
const ERR_TOO_LONG: &str = "ERR string exceeds maximum allowed size (proto-max-bulk-len)";

/// Copies an argument into its own allocation before it is stored.
///
/// Arguments are slices of the connection's read buffer. Storing one directly
/// would pin that whole buffer (often many kilobytes of unrelated pipelined
/// commands) for as long as the value lives.
fn owned(arg: &Bytes) -> Value {
    Value::String(Bytes::copy_from_slice(arg))
}

/// The string stored at `key`: `None` if the key does not exist, an error if
/// it holds another type. Cloning `Bytes` only bumps a reference count, so
/// replies share the stored allocation.
pub(crate) fn read(ctx: &mut Ctx<'_>, key: &[u8]) -> Result<Option<Bytes>, Frame> {
    let (db, now) = ctx.db();
    match db.get(key, now) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(wrong_type()),
    }
}

/// Like [`read`], but for editing the stored string in place.
pub(crate) fn string_mut<'a>(
    db: &'a mut Db,
    key: &[u8],
    now: u64,
) -> Result<Option<&'a mut Bytes>, Frame> {
    match db.get_mut(key, now) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(wrong_type()),
    }
}

fn bulk_or_null(value: Option<Bytes>) -> Frame {
    value.map_or(Frame::Null, Frame::Bulk)
}

/// Parses `EX seconds | PX millis | EXAT unix-seconds | PXAT unix-millis`
/// into an absolute expiry in milliseconds.
fn expiry(unit: &[u8], amount: &[u8], now: u64, command: &str) -> Result<u64, Frame> {
    let amount = int(amount)?;
    let at = if amount <= 0 {
        None
    } else if is(unit, "EX") {
        amount
            .checked_mul(1000)
            .and_then(|ms| ms.checked_add(now as i64))
    } else if is(unit, "PX") {
        amount.checked_add(now as i64)
    } else if is(unit, "EXAT") {
        amount.checked_mul(1000)
    } else {
        Some(amount)
    };
    at.map(|at| at as u64)
        .ok_or_else(|| Frame::error(format!("ERR invalid expire time in '{command}' command")))
}

fn is_expiry_unit(arg: &[u8]) -> bool {
    is(arg, "EX") || is(arg, "PX") || is(arg, "EXAT") || is(arg, "PXAT")
}

// --- reads ------------------------------------------------------------------

pub(crate) fn get(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(read(ctx, &args[1]).map(bulk_or_null))
}

/// `MGET key [key ...]` — a key holding another type reads as missing
/// rather than failing the whole command.
pub(crate) fn mget(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    Frame::Array(
        args[1..]
            .iter()
            .map(|key| bulk_or_null(read(ctx, key).unwrap_or(None)))
            .collect(),
    )
}

pub(crate) fn strlen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(read(ctx, &args[1]).map(|s| Frame::Integer(s.map_or(0, |s| s.len() as i64))))
}

/// `GETRANGE key start end` (and its old name `SUBSTR`). Both ends are
/// inclusive; negative indexes count from the end of the string.
pub(crate) fn getrange(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (start, end) = (int(&args[2])?, int(&args[3])?);
        let value = read(ctx, &args[1])?.unwrap_or_default();
        let len = value.len() as i64;
        if len == 0 || (start < 0 && end < 0 && start > end) {
            return Ok(Frame::Bulk(Bytes::new()));
        }
        let start = if start < 0 {
            len.saturating_add(start).max(0)
        } else {
            start
        };
        let end = if end < 0 {
            len.saturating_add(end).max(0)
        } else {
            end
        }
        .min(len - 1);
        if start > end {
            return Ok(Frame::Bulk(Bytes::new()));
        }
        Ok(Frame::Bulk(value.slice(start as usize..=end as usize)))
    })())
}

// --- writes -----------------------------------------------------------------

/// `SET key value [NX | XX] [GET] [EX s | PX ms | EXAT ts | PXAT ms-ts | KEEPTTL]`
pub(crate) fn set(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(set_impl(ctx, args))
}

fn set_impl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let (key, value) = (&args[1], &args[2]);
    let (mut nx, mut xx, mut return_old) = (false, false, false);
    let mut ttl: Option<SetTtl> = None;

    let mut options = &args[3..];
    loop {
        match options {
            [] => break,
            [opt, rest @ ..] if is(opt, "NX") && !xx => {
                nx = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "XX") && !nx => {
                xx = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "GET") => {
                return_old = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "KEEPTTL") && ttl.is_none() => {
                ttl = Some(SetTtl::Keep);
                options = rest;
            }
            [unit, amount, rest @ ..] if is_expiry_unit(unit) && ttl.is_none() => {
                ttl = Some(SetTtl::At(expiry(unit, amount, ctx.now, "set")?));
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }

    // SET replaces a value of any type, but GET can only return a string.
    let old = if return_old { read(ctx, key)? } else { None };
    let (db, now) = ctx.db();
    let exists = db.exists(key, now);
    let allowed = match (nx, xx) {
        (true, _) => !exists,
        (_, true) => exists,
        _ => true,
    };
    if allowed {
        db.set(key, owned(value), ttl.unwrap_or(SetTtl::Clear), now);
    }
    // Logged without its conditions, and with any expiry made absolute.
    ctx.log_instead(|| {
        if !allowed {
            return Vec::new();
        }
        vec![match ttl {
            Some(SetTtl::At(at)) => {
                crate::aof::command(&[b"SET", key, value, b"PXAT", at.to_string().as_bytes()])
            }
            Some(SetTtl::Keep) => crate::aof::command(&[b"SET", key, value, b"KEEPTTL"]),
            _ => crate::aof::command(&[b"SET", key, value]),
        }]
    });
    Ok(if return_old {
        bulk_or_null(old)
    } else if allowed {
        Frame::ok()
    } else {
        Frame::Null
    })
}

pub(crate) fn setnx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    if db.exists(&args[1], now) {
        return Frame::Integer(0);
    }
    db.set(&args[1], owned(&args[2]), SetTtl::Clear, now);
    Frame::Integer(1)
}

/// `SETEX key seconds value`
pub(crate) fn setex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_with_expiry(ctx, args, b"EX", "setex")
}

/// `PSETEX key milliseconds value`
pub(crate) fn psetex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    set_with_expiry(ctx, args, b"PX", "psetex")
}

fn set_with_expiry(ctx: &mut Ctx<'_>, args: &[Bytes], unit: &[u8], command: &str) -> Frame {
    let (db, now) = ctx.db();
    match expiry(unit, &args[2], now, command) {
        Ok(at) => {
            db.set(&args[1], owned(&args[3]), SetTtl::At(at), now);
            ctx.log_instead(|| {
                vec![crate::aof::command(&[
                    b"SET",
                    &args[1],
                    &args[3],
                    b"PXAT",
                    at.to_string().as_bytes(),
                ])]
            });
            Frame::ok()
        }
        Err(e) => e,
    }
}

pub(crate) fn getset(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let old = read(ctx, &args[1])?;
        let (db, now) = ctx.db();
        db.set(&args[1], owned(&args[2]), SetTtl::Clear, now);
        Ok(bulk_or_null(old))
    })())
}

pub(crate) fn getdel(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let value = read(ctx, &args[1])?;
        if value.is_some() {
            let (db, now) = ctx.db();
            db.remove(&args[1], now);
        }
        Ok(bulk_or_null(value))
    })())
}

/// `GETEX key [EX s | PX ms | EXAT ts | PXAT ms-ts | PERSIST]`
pub(crate) fn getex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        // `None`: leave the TTL alone. `Some(None)`: remove it.
        let change: Option<Option<u64>> = match &args[2..] {
            [] => None,
            [opt] if is(opt, "PERSIST") => Some(None),
            [unit, amount] if is_expiry_unit(unit) => {
                Some(Some(expiry(unit, amount, ctx.now, "getex")?))
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        // Only a change of TTL is worth logging, and as an absolute time.
        ctx.log_instead(Vec::new);
        let Some(value) = read(ctx, &args[1])? else {
            return Ok(Frame::Null);
        };
        if let Some(expire_at) = change {
            let (db, now) = ctx.db();
            db.set_expire(&args[1], expire_at, now);
            ctx.log_instead(|| {
                vec![match expire_at {
                    Some(at) => {
                        crate::aof::command(&[b"PEXPIREAT", &args[1], at.to_string().as_bytes()])
                    }
                    None => crate::aof::command(&[b"PERSIST", &args[1]]),
                }]
            });
        }
        Ok(Frame::Bulk(value))
    })())
}

/// The `key value` pairs following the command name. Callers have already
/// rejected an odd number of them.
fn pairs(args: &[Bytes]) -> &[[Bytes; 2]] {
    args[1..].as_chunks::<2>().0
}

/// `MSET key value [key value ...]`
pub(crate) fn mset(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len().is_multiple_of(2) {
        return wrong_arity("mset");
    }
    let (db, now) = ctx.db();
    for [key, value] in pairs(args) {
        db.set(key, owned(value), SetTtl::Clear, now);
    }
    Frame::ok()
}

/// `MSETNX key value [key value ...]` — all or nothing.
pub(crate) fn msetnx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len().is_multiple_of(2) {
        return wrong_arity("msetnx");
    }
    let (db, now) = ctx.db();
    if pairs(args).iter().any(|[key, _]| db.exists(key, now)) {
        return Frame::Integer(0);
    }
    for [key, value] in pairs(args) {
        db.set(key, owned(value), SetTtl::Clear, now);
    }
    Frame::Integer(1)
}

// --- counters ---------------------------------------------------------------

fn incr_by(ctx: &mut Ctx<'_>, key: &[u8], delta: i64) -> Result<Frame, Frame> {
    let current = match read(ctx, key)? {
        Some(s) => parse_i64(&s).ok_or_else(|| Frame::error(ERR_NOT_INTEGER))?,
        None => 0,
    };
    let updated = current
        .checked_add(delta)
        .ok_or_else(|| Frame::error(ERR_OVERFLOW))?;
    let (db, now) = ctx.db();
    db.set(
        key,
        Value::String(Bytes::from(updated.to_string())),
        SetTtl::Keep,
        now,
    );
    Ok(Frame::Integer(updated))
}

pub(crate) fn incr(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(incr_by(ctx, &args[1], 1))
}

pub(crate) fn decr(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(incr_by(ctx, &args[1], -1))
}

pub(crate) fn incrby(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(int(&args[2]).and_then(|delta| incr_by(ctx, &args[1], delta)))
}

pub(crate) fn decrby(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        // Negating i64::MIN is not representable.
        let delta = int(&args[2])?
            .checked_neg()
            .ok_or_else(|| Frame::error("ERR decrement would overflow"))?;
        incr_by(ctx, &args[1], delta)
    })())
}

pub(crate) fn parse_float(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|f| !f.is_nan())
}

pub(crate) fn incrbyfloat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let not_float = || Frame::error(ERR_NOT_FLOAT);
        let current = match read(ctx, &args[1])? {
            Some(s) => parse_float(&s).ok_or_else(not_float)?,
            None => 0.0,
        };
        let delta = parse_float(&args[2]).ok_or_else(not_float)?;
        let updated = current + delta;
        if !updated.is_finite() {
            return Err(Frame::error("ERR increment would produce NaN or Infinity"));
        }
        // Shortest text that parses back to exactly this value.
        let text = Bytes::from(updated.to_string());
        let (db, now) = ctx.db();
        db.set(&args[1], Value::String(text.clone()), SetTtl::Keep, now);
        // Logged as its result: another server might round differently.
        ctx.log_instead(|| vec![crate::aof::command(&[b"SET", &args[1], &text, b"KEEPTTL"])]);
        Ok(Frame::Bulk(text))
    })())
}

// --- in-place edits ---------------------------------------------------------

/// Takes the stored string for in-place editing. When nothing else holds a
/// reference to the allocation this reuses it (so repeated `APPEND` is
/// amortised O(1) per byte); otherwise it copies.
pub(crate) fn editable(stored: &mut Bytes, extra_capacity: usize) -> BytesMut {
    match std::mem::take(stored).try_into_mut() {
        Ok(mut unique) => {
            unique.reserve(extra_capacity);
            unique
        }
        Err(shared) => {
            let mut copy = BytesMut::with_capacity(shared.len() + extra_capacity);
            copy.extend_from_slice(&shared);
            copy
        }
    }
}

pub(crate) fn append(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (key, suffix) = (&args[1], &args[2]);
        let (db, now) = ctx.db();
        match string_mut(db, key, now)? {
            Some(stored) => {
                if stored.len() + suffix.len() > MAX_BULK_LEN {
                    return Err(Frame::error(ERR_TOO_LONG));
                }
                let mut buf = editable(stored, suffix.len());
                buf.extend_from_slice(suffix);
                *stored = buf.freeze();
                Ok(Frame::Integer(stored.len() as i64))
            }
            None => {
                db.set(key, owned(suffix), SetTtl::Clear, now);
                Ok(Frame::Integer(suffix.len() as i64))
            }
        }
    })())
}

/// `SETRANGE key offset value` — overwrites part of the string, zero-padding
/// if the offset is past the end.
pub(crate) fn setrange(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (key, patch) = (&args[1], &args[3]);
        let offset = match int(&args[2])? {
            offset if offset >= 0 => offset as u64,
            _ => return Err(Frame::error("ERR offset is out of range")),
        };
        let (db, now) = ctx.db();
        let existing = string_mut(db, key, now)?;

        if patch.is_empty() {
            // Nothing to write: report the current length and create nothing.
            let length = existing.map_or(0, |s| s.len() as i64);
            db.unchanged(key);
            return Ok(Frame::Integer(length));
        }
        if offset + patch.len() as u64 > MAX_BULK_LEN as u64 {
            return Err(Frame::error(ERR_TOO_LONG));
        }
        let (offset, end) = (offset as usize, offset as usize + patch.len());

        match existing {
            Some(stored) => {
                let mut buf = editable(stored, end.saturating_sub(stored.len()));
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[offset..end].copy_from_slice(patch);
                *stored = buf.freeze();
                Ok(Frame::Integer(stored.len() as i64))
            }
            None => {
                let mut buf = BytesMut::zeroed(end);
                buf[offset..].copy_from_slice(patch);
                db.set(key, Value::String(buf.freeze()), SetTtl::Clear, now);
                Ok(Frame::Integer(end as i64))
            }
        }
    })())
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, err, Harness};
    use bytes::Bytes;
    use rexyra_resp::Frame;

    const NOT_INT: &str = "ERR value is not an integer or out of range";

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    #[test]
    fn set_and_get() {
        let mut h = Harness::new();
        assert_eq!(h.run("GET a"), Frame::Null);
        assert_eq!(h.run("SET a hello"), Frame::ok());
        assert_eq!(h.run("GET a"), bulk("hello"));
        assert_eq!(h.run("SET a world"), Frame::ok());
        assert_eq!(h.run("GET a"), bulk("world"));
        assert_eq!(h.run("STRLEN a"), int(5));
        assert_eq!(h.run("STRLEN missing"), int(0));
    }

    #[test]
    fn values_are_binary_safe_and_may_be_empty() {
        let mut h = Harness::new();
        let binary = Bytes::from_static(b"\x00\r\n\xff");
        let set = [
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            binary.clone(),
        ];
        assert_eq!(h.raw(&set), Frame::ok());
        assert_eq!(h.run("GET k"), Frame::Bulk(binary));

        assert_eq!(h.cmd(&["SET", "e", ""]), Frame::ok());
        assert_eq!(h.run("GET e"), bulk(""));
        assert_eq!(h.run("EXISTS e"), int(1));
    }

    #[test]
    fn set_nx_xx() {
        let mut h = Harness::new();
        assert_eq!(h.run("SET a 1 XX"), Frame::Null);
        assert_eq!(h.run("GET a"), Frame::Null);
        assert_eq!(h.run("SET a 1 NX"), Frame::ok());
        assert_eq!(h.run("SET a 2 NX"), Frame::Null);
        assert_eq!(h.run("GET a"), bulk("1"));
        assert_eq!(h.run("SET a 3 xx"), Frame::ok());
        assert_eq!(h.run("GET a"), bulk("3"));
    }

    #[test]
    fn set_get_option() {
        let mut h = Harness::new();
        assert_eq!(h.run("SET a 1 GET"), Frame::Null);
        assert_eq!(h.run("SET a 2 GET"), bulk("1"));
        // With NX the old value is returned and the write is skipped.
        assert_eq!(h.run("SET a 3 NX GET"), bulk("2"));
        assert_eq!(h.run("GET a"), bulk("2"));
    }

    #[test]
    fn set_expiry_options() {
        let mut h = Harness::new();
        let now = h.now as i64;

        h.run("SET a 1 EX 10");
        assert_eq!(h.run("PTTL a"), int(10_000));
        h.run("SET a 1 PX 1500");
        assert_eq!(h.run("PTTL a"), int(1500));
        h.cmd(&["SET", "a", "1", "EXAT", &(now / 1000 + 20).to_string()]);
        assert_eq!(h.run("EXPIRETIME a"), int(now / 1000 + 20));
        h.cmd(&["SET", "a", "1", "PXAT", &(now + 250).to_string()]);
        assert_eq!(h.run("PTTL a"), int(250));

        h.run("SET a 2 KEEPTTL");
        assert_eq!(h.run("PTTL a"), int(250));
        h.run("SET a 3");
        assert_eq!(h.run("PTTL a"), int(-1));

        h.run("SET b 1 PX 100");
        h.advance(100);
        assert_eq!(h.run("GET b"), Frame::Null);
    }

    #[test]
    fn set_errors_leave_the_key_untouched() {
        let mut h = Harness::new();
        h.run("SET a keep");
        for (command, message) in [
            ("SET a 1 NX XX", "ERR syntax error"),
            ("SET a 1 EX 10 PX 10", "ERR syntax error"),
            ("SET a 1 EX 10 KEEPTTL", "ERR syntax error"),
            ("SET a 1 BOGUS", "ERR syntax error"),
            ("SET a 1 EX", "ERR syntax error"),
            ("SET a 1 EX abc", NOT_INT),
            ("SET a 1 EX 0", "ERR invalid expire time in 'set' command"),
            ("SET a 1 PX -5", "ERR invalid expire time in 'set' command"),
            (
                "SET a 1 EX 9223372036854775807",
                "ERR invalid expire time in 'set' command",
            ),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        assert_eq!(h.run("GET a"), bulk("keep"));
    }

    #[test]
    fn setnx_setex_psetex() {
        let mut h = Harness::new();
        assert_eq!(h.run("SETNX a 1"), int(1));
        assert_eq!(h.run("SETNX a 2"), int(0));
        assert_eq!(h.run("GET a"), bulk("1"));

        assert_eq!(h.run("SETEX b 10 v"), Frame::ok());
        assert_eq!(h.run("TTL b"), int(10));
        assert_eq!(h.run("PSETEX c 500 v"), Frame::ok());
        assert_eq!(h.run("PTTL c"), int(500));

        assert_eq!(
            h.run("SETEX b 0 v"),
            err("ERR invalid expire time in 'setex' command")
        );
        assert_eq!(
            h.run("PSETEX b -1 v"),
            err("ERR invalid expire time in 'psetex' command")
        );
        assert_eq!(h.run("SETEX b x v"), err(NOT_INT));
    }

    #[test]
    fn getset_and_getdel() {
        let mut h = Harness::new();
        assert_eq!(h.run("GETSET a 1"), Frame::Null);
        h.run("EXPIRE a 100");
        assert_eq!(h.run("GETSET a 2"), bulk("1"));
        assert_eq!(h.run("TTL a"), int(-1), "GETSET clears the TTL");

        assert_eq!(h.run("GETDEL a"), bulk("2"));
        assert_eq!(h.run("GETDEL a"), Frame::Null);
        assert_eq!(h.run("EXISTS a"), int(0));
    }

    #[test]
    fn getex() {
        let mut h = Harness::new();
        assert_eq!(h.run("GETEX missing EX 10"), Frame::Null);
        h.run("SET a 1");
        assert_eq!(h.run("GETEX a"), bulk("1"));
        assert_eq!(h.run("TTL a"), int(-1));
        assert_eq!(h.run("GETEX a EX 10"), bulk("1"));
        assert_eq!(h.run("TTL a"), int(10));
        assert_eq!(h.run("GETEX a PX 300"), bulk("1"));
        assert_eq!(h.run("PTTL a"), int(300));
        assert_eq!(h.run("GETEX a PERSIST"), bulk("1"));
        assert_eq!(h.run("TTL a"), int(-1));

        assert_eq!(h.run("GETEX a EX 10 PERSIST"), err("ERR syntax error"));
        assert_eq!(h.run("GETEX a BOGUS"), err("ERR syntax error"));
        assert_eq!(
            h.run("GETEX a EX 0"),
            err("ERR invalid expire time in 'getex' command")
        );
    }

    #[test]
    fn mget_mset_msetnx() {
        let mut h = Harness::new();
        assert_eq!(h.run("MSET a 1 b 2"), Frame::ok());
        assert_eq!(
            h.run("MGET a missing b"),
            Frame::Array(vec![bulk("1"), Frame::Null, bulk("2")])
        );
        assert_eq!(
            h.run("MSET a 1 b"),
            err("ERR wrong number of arguments for 'mset' command")
        );

        assert_eq!(h.run("MSETNX c 3 a 9"), int(0));
        assert_eq!(h.run("EXISTS c"), int(0), "MSETNX is all or nothing");
        assert_eq!(h.run("MSETNX c 3 d 4"), int(1));
        assert_eq!(h.run("MGET c d"), Frame::Array(vec![bulk("3"), bulk("4")]));
        assert_eq!(
            h.run("MSETNX a 1 b"),
            err("ERR wrong number of arguments for 'msetnx' command")
        );
    }

    #[test]
    fn counters() {
        let mut h = Harness::new();
        assert_eq!(h.run("INCR n"), int(1));
        assert_eq!(h.run("INCR n"), int(2));
        assert_eq!(h.run("INCRBY n 40"), int(42));
        assert_eq!(h.run("DECR n"), int(41));
        assert_eq!(h.run("DECRBY n 50"), int(-9));
        assert_eq!(h.run("INCRBY n -1"), int(-10));
        assert_eq!(h.run("GET n"), bulk("-10"));
        assert_eq!(h.run("DECR fresh"), int(-1));
    }

    #[test]
    fn counter_errors() {
        let mut h = Harness::new();
        h.run("SET s abc");
        assert_eq!(h.run("INCR s"), err(NOT_INT));
        h.cmd(&["SET", "padded", " 1"]);
        assert_eq!(h.run("INCR padded"), err(NOT_INT));
        h.run("SET zeros 007");
        assert_eq!(h.run("INCR zeros"), err(NOT_INT));
        assert_eq!(h.run("INCRBY n abc"), err(NOT_INT));
        assert_eq!(h.run("INCRBY n 1.5"), err(NOT_INT));

        h.run("SET max 9223372036854775807");
        assert_eq!(
            h.run("INCR max"),
            err("ERR increment or decrement would overflow")
        );
        assert_eq!(h.run("GET max"), bulk("9223372036854775807"));
        h.run("SET min -9223372036854775808");
        assert_eq!(
            h.run("DECR min"),
            err("ERR increment or decrement would overflow")
        );
        assert_eq!(
            h.run("DECRBY n -9223372036854775808"),
            err("ERR decrement would overflow")
        );
    }

    #[test]
    fn incrbyfloat() {
        let mut h = Harness::new();
        assert_eq!(h.run("INCRBYFLOAT f 10.5"), bulk("10.5"));
        assert_eq!(h.run("INCRBYFLOAT f 0.1"), bulk("10.6"));
        assert_eq!(h.run("INCRBYFLOAT f -5"), bulk("5.6"));
        assert_eq!(h.run("INCRBYFLOAT f 4.4"), bulk("10"));
        assert_eq!(h.run("GET f"), bulk("10"));
        assert_eq!(h.run("INCRBYFLOAT f 1e3"), bulk("1010"));
        assert_eq!(
            h.run("INCR f"),
            int(1011),
            "integral result is a valid integer"
        );

        h.run("SET s abc");
        assert_eq!(
            h.run("INCRBYFLOAT s 1"),
            err("ERR value is not a valid float")
        );
        assert_eq!(
            h.run("INCRBYFLOAT f abc"),
            err("ERR value is not a valid float")
        );
        assert_eq!(
            h.run("INCRBYFLOAT f nan"),
            err("ERR value is not a valid float")
        );
        assert_eq!(
            h.run("INCRBYFLOAT f inf"),
            err("ERR increment would produce NaN or Infinity")
        );
        h.run("SET big 1e308");
        assert_eq!(
            h.run("INCRBYFLOAT big 1e308"),
            err("ERR increment would produce NaN or Infinity")
        );
    }

    #[test]
    fn append() {
        let mut h = Harness::new();
        assert_eq!(h.run("APPEND a Hello"), int(5));
        assert_eq!(h.cmd(&["APPEND", "a", " World"]), int(11));
        assert_eq!(h.run("GET a"), bulk("Hello World"));
        assert_eq!(h.cmd(&["APPEND", "a", ""]), int(11));
    }

    #[test]
    fn append_does_not_disturb_a_value_already_handed_out() {
        let mut h = Harness::new();
        h.run("SET a abc");
        // The reply shares the stored allocation; the edit must copy.
        let before = h.run("GET a");
        h.run("APPEND a def");
        assert_eq!(before, bulk("abc"));
        assert_eq!(h.run("GET a"), bulk("abcdef"));

        // And many appends in a row still produce the right content.
        for i in 0..200 {
            h.cmd(&["APPEND", "log", &format!("{i},")]);
        }
        let expected: String = (0..200).map(|i| format!("{i},")).collect();
        assert_eq!(h.run("GET log"), bulk(&expected));
    }

    #[test]
    fn getrange() {
        let mut h = Harness::new();
        h.cmd(&["SET", "s", "This is a string"]);
        assert_eq!(h.run("GETRANGE s 0 3"), bulk("This"));
        assert_eq!(h.run("GETRANGE s -3 -1"), bulk("ing"));
        assert_eq!(h.run("GETRANGE s 0 -1"), bulk("This is a string"));
        assert_eq!(h.run("GETRANGE s 10 100"), bulk("string"));
        assert_eq!(h.run("GETRANGE s 5 3"), bulk(""));
        assert_eq!(h.run("GETRANGE s -1 -5"), bulk(""));
        assert_eq!(h.run("GETRANGE s -100 3"), bulk("This"));
        assert_eq!(h.run("GETRANGE s 100 200"), bulk(""));
        assert_eq!(h.run("GETRANGE missing 0 -1"), bulk(""));
        assert_eq!(h.run("SUBSTR s 0 3"), bulk("This"));
        assert_eq!(h.run("GETRANGE s a 3"), err(NOT_INT));
        assert_eq!(
            h.run("GETRANGE s -9223372036854775808 9223372036854775807"),
            bulk("This is a string")
        );
    }

    #[test]
    fn setrange() {
        let mut h = Harness::new();
        h.cmd(&["SET", "s", "Hello World"]);
        assert_eq!(h.run("SETRANGE s 6 Redis"), int(11));
        assert_eq!(h.run("GET s"), bulk("Hello Redis"));
        assert_eq!(h.run("SETRANGE s 9 xyz!"), int(13), "grows when needed");
        assert_eq!(h.run("GET s"), bulk("Hello Redxyz!"));

        assert_eq!(h.run("SETRANGE pad 3 ab"), int(5));
        assert_eq!(
            h.run("GET pad"),
            Frame::Bulk(Bytes::from_static(b"\0\0\0ab"))
        );

        assert_eq!(h.cmd(&["SETRANGE", "none", "5", ""]), int(0));
        assert_eq!(h.run("EXISTS none"), int(0), "empty patch creates nothing");
        assert_eq!(h.cmd(&["SETRANGE", "s", "0", ""]), int(13));

        assert_eq!(h.run("SETRANGE s -1 x"), err("ERR offset is out of range"));
        assert_eq!(h.run("SETRANGE s a x"), err(NOT_INT));
        assert_eq!(
            h.run("SETRANGE s 536870912 x"),
            err("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
        );
    }

    #[test]
    fn setrange_keeps_the_ttl() {
        let mut h = Harness::new();
        h.run("SET s abc EX 100");
        h.run("SETRANGE s 1 Z");
        assert_eq!(h.run("TTL s"), int(100));
        assert_eq!(h.run("GET s"), bulk("aZc"));
    }
}
