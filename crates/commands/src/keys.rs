//! Type-independent key commands: deletion, renaming, iteration, expiry, and
//! the database-level `DBSIZE` / `FLUSHDB` / `FLUSHALL`.

use bytes::Bytes;
use rexyra_engine::{glob_match, SetTtl};
use rexyra_resp::Frame;

use crate::{int, is, lossy, reply, Ctx, ERR_SYNTAX};

const ERR_NO_SUCH_KEY: &str = "ERR no such key";
const ERR_DB_RANGE: &str = "ERR DB index is out of range";

// --- database-level ---------------------------------------------------------

pub(crate) fn dbsize(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    let (db, _) = ctx.db();
    Frame::Integer(db.len() as i64)
}

/// `FLUSHDB [ASYNC | SYNC]`
pub(crate) fn flushdb(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let Some(lazy) = flush_mode(args) else {
        return Frame::error(ERR_SYNTAX);
    };
    crate::persistence::interrupt_for_flush(ctx.server, ctx.keyspace);
    if lazy {
        let contents = ctx.db().0.take_all();
        crate::memory::lazy_free(ctx.server, contents);
    } else {
        ctx.db().0.clear();
    }
    Frame::ok()
}

/// `FLUSHALL [ASYNC | SYNC]`
pub(crate) fn flushall(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let Some(lazy) = flush_mode(args) else {
        return Frame::error(ERR_SYNTAX);
    };
    crate::persistence::interrupt_for_flush(ctx.server, ctx.keyspace);
    if lazy {
        let contents = ctx.keyspace.take_all();
        crate::memory::lazy_free(ctx.server, Box::new(contents));
    } else {
        ctx.keyspace.flush_all();
    }
    Frame::ok()
}

/// Whether a flush should free memory in the background (`ASYNC`) or
/// before replying (`SYNC`, the default). `None` if the arguments are not
/// valid.
fn flush_mode(args: &[Bytes]) -> Option<bool> {
    match args {
        [_] => Some(false),
        [_, mode] if is(mode, "ASYNC") => Some(true),
        [_, mode] if is(mode, "SYNC") => Some(false),
        _ => None,
    }
}

// --- existence and deletion -------------------------------------------------

/// `DEL key [key ...]`
pub(crate) fn del(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    let removed = args[1..].iter().filter(|key| db.remove(key, now)).count();
    Frame::Integer(removed as i64)
}

/// `UNLINK key [key ...]` — like `DEL`, but the memory of large values is
/// freed in the background.
pub(crate) fn unlink(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let mut removed = 0;
    for key in &args[1..] {
        let (db, now) = ctx.db();
        if let Some((value, _)) = db.take(key, now) {
            removed += 1;
            if crate::memory::worth_lazy_free(&value) {
                crate::memory::lazy_free(ctx.server, Box::new(value));
            }
        }
    }
    Frame::Integer(removed)
}

/// `EXISTS key [key ...]` and `TOUCH key [key ...]`. A key named twice is
/// counted twice.
pub(crate) fn exists(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    let found = args[1..].iter().filter(|key| db.exists(key, now)).count();
    Frame::Integer(found as i64)
}

pub(crate) fn type_(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    Frame::simple(
        db.get(&args[1], now)
            .map_or("none", |value| value.type_name()),
    )
}

// --- rename and copy --------------------------------------------------------

pub(crate) fn rename(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    match rename_generic(ctx, args, false) {
        Ok(_) => Frame::ok(),
        Err(e) => e,
    }
}

pub(crate) fn renamenx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    match rename_generic(ctx, args, true) {
        Ok(renamed) => Frame::Integer(i64::from(renamed)),
        Err(e) => e,
    }
}

/// Moves the value and its TTL from `args[1]` to `args[2]`. Returns whether
/// the rename happened (only ever `false` when `only_if_absent` is set).
fn rename_generic(ctx: &mut Ctx<'_>, args: &[Bytes], only_if_absent: bool) -> Result<bool, Frame> {
    let (src, dst) = (&args[1], &args[2]);
    let (db, now) = ctx.db();
    if !db.exists(src, now) {
        return Err(Frame::error(ERR_NO_SUCH_KEY));
    }
    if src == dst {
        return Ok(!only_if_absent);
    }
    if only_if_absent && db.exists(dst, now) {
        return Ok(false);
    }
    let Some((value, expire_at)) = db.take(src, now) else {
        return Err(Frame::error(ERR_NO_SUCH_KEY));
    };
    db.set(dst, value, expire_at.map_or(SetTtl::Clear, SetTtl::At), now);
    Ok(true)
}

/// `MOVE key db`: moves the key, with its TTL, to another database, unless
/// it is not there or the target already has that key.
pub(crate) fn move_key(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(move_impl(ctx, args))
}

fn move_impl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let key = &args[1];
    let index = int(&args[2])?;
    if index < 0 || index as u64 >= ctx.server.databases as u64 {
        return Err(Frame::error(ERR_DB_RANGE));
    }
    let (src_db, dst_db) = (ctx.client.db, index as usize);
    if src_db == dst_db {
        return Err(Frame::error(
            "ERR source and destination objects are the same",
        ));
    }
    let now = ctx.now;
    if !ctx.keyspace.db(src_db).exists(key, now) {
        return Ok(Frame::Integer(0));
    }
    if ctx.keyspace.db(dst_db).exists(key, now) {
        return Ok(Frame::Integer(0));
    }
    let Some((value, expire_at)) = ctx.keyspace.db(src_db).take(key, now) else {
        return Ok(Frame::Integer(0));
    };
    ctx.keyspace
        .db(dst_db)
        .set(key, value, expire_at.map_or(SetTtl::Clear, SetTtl::At), now);
    Ok(Frame::Integer(1))
}

/// `SWAPDB index1 index2`: exchanges the contents of two databases. Clients
/// keep their database number and see the other contents.
pub(crate) fn swapdb(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let index = |arg: &Bytes, which: &str| match rexyra_resp::parse_i64(arg) {
        Some(index) if index < 0 || index as u64 >= ctx.server.databases as u64 => {
            Err(Frame::error(ERR_DB_RANGE))
        }
        Some(index) => Ok(index as usize),
        None => Err(Frame::error(format!("ERR invalid {which} DB index"))),
    };
    let a = match index(&args[1], "first") {
        Ok(a) => a,
        Err(e) => return e,
    };
    let b = match index(&args[2], "second") {
        Ok(b) => b,
        Err(e) => return e,
    };
    // Outside a transaction or a script the command waited for this.
    if crate::persistence::capturing(ctx.server) {
        return Frame::error(
            "ERR SWAPDB cannot run in a transaction or a script while a background save or rewrite is reading the data; try again when it has finished",
        );
    }
    ctx.keyspace.swap(a, b);
    Frame::ok()
}

/// `COPY source destination [DB destination-db] [REPLACE]`
pub(crate) fn copy(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(copy_impl(ctx, args))
}

fn copy_impl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let (src, dst) = (&args[1], &args[2]);
    let src_db = ctx.client.db;
    let mut dst_db = src_db;
    let mut replace = false;

    let mut options = &args[3..];
    loop {
        match options {
            [] => break,
            [opt, index, rest @ ..] if is(opt, "DB") => {
                let index = int(index)?;
                if index < 0 || index as u64 >= ctx.server.databases as u64 {
                    return Err(Frame::error(ERR_DB_RANGE));
                }
                dst_db = index as usize;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "REPLACE") => {
                replace = true;
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
    if src_db == dst_db && src == dst {
        return Err(Frame::error(
            "ERR source and destination objects are the same",
        ));
    }

    let now = ctx.now;
    let source = ctx.keyspace.db(src_db);
    let Some(value) = source.get(src, now).cloned() else {
        return Ok(Frame::Integer(0));
    };
    let expire_at = source.expire_at(src, now).flatten();

    let target = ctx.keyspace.db(dst_db);
    if !replace && target.exists(dst, now) {
        return Ok(Frame::Integer(0));
    }
    target.set(dst, value, expire_at.map_or(SetTtl::Clear, SetTtl::At), now);
    Ok(Frame::Integer(1))
}

// --- iteration --------------------------------------------------------------

/// `KEYS pattern`
pub(crate) fn keys(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let pattern = &args[1];
    let (db, now) = ctx.db();
    Frame::Array(
        db.iter()
            .filter(|(key, entry)| !entry.is_expired(now) && glob_match(pattern, key))
            .map(|(key, _)| Frame::Bulk(key.clone()))
            .collect(),
    )
}

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`
pub(crate) fn scan(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(scan_impl(ctx, args))
}

fn scan_impl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let mut cursor: u64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| Frame::error("ERR invalid cursor"))?;

    let mut pattern: Option<&[u8]> = None;
    let mut count = 10usize;
    let mut type_filter: Option<String> = None;

    let mut options = &args[2..];
    loop {
        match options {
            [] => break,
            [opt, value, rest @ ..] if is(opt, "MATCH") => {
                // `*` matches everything; skip the matcher entirely.
                pattern = (&value[..] != b"*").then_some(&value[..]);
                options = rest;
            }
            [opt, value, rest @ ..] if is(opt, "COUNT") => {
                count = match int(value)? {
                    n if n >= 1 => n as usize,
                    _ => return Err(Frame::error(ERR_SYNTAX)),
                };
                options = rest;
            }
            [opt, value, rest @ ..] if is(opt, "TYPE") => {
                type_filter = Some(lossy(value).to_ascii_lowercase());
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }

    let (db, now) = ctx.db();
    let mut found = Vec::new();
    // A sparse table could otherwise make one call walk a huge number of
    // empty buckets; cap the work like Redis does.
    let mut budget = count.saturating_mul(10);
    loop {
        cursor = db.scan(cursor, |key, entry| {
            let wanted = !entry.is_expired(now)
                && pattern.is_none_or(|pattern| glob_match(pattern, key))
                && type_filter
                    .as_deref()
                    .is_none_or(|wanted| entry.value.type_name() == wanted);
            if wanted {
                found.push(Frame::Bulk(key.clone()));
            }
        });
        budget -= 1;
        if cursor == 0 || budget == 0 || found.len() >= count {
            break;
        }
    }

    Ok(Frame::Array(vec![
        Frame::bulk(cursor.to_string()),
        Frame::Array(found),
    ]))
}

pub(crate) fn randomkey(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    db.random_key(now).map_or(Frame::Null, Frame::Bulk)
}

// --- expiry -----------------------------------------------------------------

#[derive(Clone, Copy)]
enum Unit {
    Seconds,
    Millis,
}

pub(crate) fn expire(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(expire_generic(ctx, args, Unit::Seconds, false, "expire"))
}

pub(crate) fn pexpire(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(expire_generic(ctx, args, Unit::Millis, false, "pexpire"))
}

pub(crate) fn expireat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(expire_generic(ctx, args, Unit::Seconds, true, "expireat"))
}

pub(crate) fn pexpireat(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(expire_generic(ctx, args, Unit::Millis, true, "pexpireat"))
}

/// `EXPIRE key time [NX | XX | GT | LT]` and its three siblings.
fn expire_generic(
    ctx: &mut Ctx<'_>,
    args: &[Bytes],
    unit: Unit,
    absolute: bool,
    command: &str,
) -> Result<Frame, Frame> {
    let key = &args[1];
    let amount = int(&args[2])?;

    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for opt in &args[3..] {
        if is(opt, "NX") {
            nx = true;
        } else if is(opt, "XX") {
            xx = true;
        } else if is(opt, "GT") {
            gt = true;
        } else if is(opt, "LT") {
            lt = true;
        } else {
            return Err(Frame::error(format!(
                "ERR Unsupported option {}",
                lossy(opt)
            )));
        }
    }
    if nx && (xx || gt || lt) {
        return Err(Frame::error(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(Frame::error(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }

    // Unless the TTL really changes, there is nothing to log: replaying a
    // conditional `EXPIRE` at another time could decide differently.
    ctx.log_instead(Vec::new);
    let (db, now) = ctx.db();
    // Signed arithmetic: the requested time may legitimately be negative.
    let now_ms = i64::try_from(now).unwrap_or(i64::MAX);
    let millis = match unit {
        Unit::Seconds => amount.checked_mul(1000),
        Unit::Millis => Some(amount),
    };
    let at = match (millis, absolute) {
        (Some(ms), true) => Some(ms),
        (Some(ms), false) => ms.checked_add(now_ms),
        (None, _) => None,
    }
    .ok_or_else(|| Frame::error(format!("ERR invalid expire time in '{command}' command")))?;

    let Some(current) = db.expire_at(key, now) else {
        return Ok(Frame::Integer(0));
    };
    // For GT and LT a key without a TTL counts as expiring infinitely far in
    // the future.
    let blocked = match current {
        Some(current) => {
            let current = i64::try_from(current).unwrap_or(i64::MAX);
            nx || (gt && at <= current) || (lt && at >= current)
        }
        None => xx || gt,
    };
    if blocked {
        return Ok(Frame::Integer(0));
    }

    let deleted = at <= now_ms;
    if deleted {
        // A time in the past deletes the key immediately.
        db.remove(key, now);
    } else {
        db.set_expire(key, Some(at as u64), now);
    }
    ctx.log_instead(|| {
        vec![if deleted {
            crate::aof::command(&[b"DEL", key])
        } else {
            crate::aof::command(&[b"PEXPIREAT", key, at.to_string().as_bytes()])
        }]
    });
    Ok(Frame::Integer(1))
}

/// Shared by the four TTL queries: `-2` for a missing key, `-1` for a key
/// without a TTL, otherwise `convert(expire_at, now)`.
fn ttl_generic(ctx: &mut Ctx<'_>, key: &[u8], convert: fn(u64, u64) -> u64) -> Frame {
    let (db, now) = ctx.db();
    Frame::Integer(match db.expire_at(key, now) {
        None => -2,
        Some(None) => -1,
        Some(Some(at)) => convert(at, now) as i64,
    })
}

pub(crate) fn ttl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    // Rounded to the nearest second, as Redis does.
    ttl_generic(ctx, &args[1], |at, now| (at - now + 500) / 1000)
}

pub(crate) fn pttl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, &args[1], |at, now| at - now)
}

pub(crate) fn expiretime(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    // Rounded to the nearest second, as Redis does.
    ttl_generic(ctx, &args[1], |at, _| (at + 500) / 1000)
}

pub(crate) fn pexpiretime(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    ttl_generic(ctx, &args[1], |at, _| at)
}

pub(crate) fn persist(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    let had_ttl = matches!(db.expire_at(&args[1], now), Some(Some(_)));
    if had_ttl {
        db.set_expire(&args[1], None, now);
    }
    Frame::Integer(i64::from(had_ttl))
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, err, Harness};
    use rexyra_resp::Frame;
    use std::collections::HashSet;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn strings(frame: Frame) -> Vec<String> {
        let Frame::Array(items) = frame else {
            panic!("expected array, got {frame:?}");
        };
        items
            .into_iter()
            .map(|item| match item {
                Frame::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                other => panic!("expected bulk, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn del_exists_type() {
        let mut h = Harness::new();
        h.run("MSET a 1 b 2 c 3");
        assert_eq!(h.run("EXISTS a b missing"), int(2));
        assert_eq!(h.run("EXISTS a a"), int(2));
        assert_eq!(h.run("TOUCH a missing"), int(1));
        assert_eq!(h.run("TYPE a"), Frame::simple("string"));
        assert_eq!(h.run("TYPE missing"), Frame::simple("none"));
        assert_eq!(h.run("DEL a b missing"), int(2));
        assert_eq!(h.run("UNLINK c c"), int(1));
        assert_eq!(h.run("DBSIZE"), int(0));
    }

    #[test]
    fn dbsize_and_flush() {
        let mut h = Harness::new();
        h.run("MSET a 1 b 2");
        h.run("SELECT 1");
        h.run("SET c 3");
        assert_eq!(h.run("DBSIZE"), int(1));
        assert_eq!(h.run("FLUSHDB"), Frame::ok());
        assert_eq!(h.run("DBSIZE"), int(0));
        h.run("SELECT 0");
        assert_eq!(h.run("DBSIZE"), int(2), "FLUSHDB only touches one db");

        h.run("SELECT 1");
        h.run("SET c 3");
        assert_eq!(h.run("FLUSHALL ASYNC"), Frame::ok());
        assert_eq!(h.run("DBSIZE"), int(0));
        h.run("SELECT 0");
        assert_eq!(h.run("DBSIZE"), int(0));

        assert_eq!(h.run("FLUSHALL NOW"), err("ERR syntax error"));
        assert_eq!(h.run("FLUSHDB SYNC extra"), err("ERR syntax error"));
    }

    #[test]
    fn databases_are_isolated() {
        let mut h = Harness::new();
        h.run("SET k zero");
        h.run("SELECT 5");
        assert_eq!(h.run("GET k"), Frame::Null);
        h.run("SET k five");
        h.run("SELECT 0");
        assert_eq!(h.run("GET k"), bulk("zero"));
    }

    #[test]
    fn rename() {
        let mut h = Harness::new();
        assert_eq!(h.run("RENAME missing b"), err("ERR no such key"));
        h.run("SET a 1");
        h.run("SET b old");
        assert_eq!(h.run("RENAME a b"), Frame::ok());
        assert_eq!(h.run("GET b"), bulk("1"));
        assert_eq!(h.run("EXISTS a"), int(0));
        assert_eq!(h.run("RENAME b b"), Frame::ok());
        assert_eq!(h.run("GET b"), bulk("1"));
    }

    #[test]
    fn rename_carries_the_ttl_and_replaces_the_destination_ttl() {
        let mut h = Harness::new();
        h.run("SET a 1 EX 100");
        h.run("SET b old EX 5000");
        h.run("RENAME a b");
        assert_eq!(h.run("TTL b"), int(100));

        h.run("SET c 1");
        h.run("RENAME c b");
        assert_eq!(h.run("TTL b"), int(-1));
    }

    #[test]
    fn renamenx() {
        let mut h = Harness::new();
        h.run("MSET a 1 b 2");
        assert_eq!(h.run("RENAMENX a b"), int(0));
        assert_eq!(h.run("GET a"), bulk("1"));
        assert_eq!(h.run("RENAMENX a c"), int(1));
        assert_eq!(h.run("GET c"), bulk("1"));
        assert_eq!(h.run("RENAMENX c c"), int(0));
        assert_eq!(h.run("RENAMENX missing x"), err("ERR no such key"));
    }

    #[test]
    fn move_key() {
        let mut h = Harness::new();
        h.run("SET a 1 EX 100");
        assert_eq!(h.run("MOVE a 1"), int(1));
        assert_eq!(h.run("EXISTS a"), int(0));
        assert_eq!(h.run("MOVE a 1"), int(0), "no longer here");
        h.run("SELECT 1");
        assert_eq!(h.run("GET a"), bulk("1"));
        assert_eq!(h.run("TTL a"), int(100), "the TTL moves with it");

        h.run("SET b there");
        h.run("SELECT 0");
        h.run("SET b here");
        assert_eq!(h.run("MOVE b 1"), int(0), "the target has the key");
        assert_eq!(h.run("GET b"), bulk("here"));
        assert_eq!(
            h.run("MOVE b 0"),
            err("ERR source and destination objects are the same")
        );
        assert_eq!(h.run("MOVE b 16"), err("ERR DB index is out of range"));
        assert_eq!(h.run("MOVE b -1"), err("ERR DB index is out of range"));
        assert_eq!(
            h.run("MOVE b x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(h.run("MOVE missing 1"), int(0));
    }

    #[test]
    fn swapdb() {
        let mut h = Harness::new();
        h.run("SET a zero EX 100");
        h.run("SELECT 3");
        h.run("SET b three");
        assert_eq!(h.run("SWAPDB 0 3"), Frame::ok());
        assert_eq!(
            h.run("GET b"),
            Frame::Null,
            "the client stays in database 3"
        );
        assert_eq!(h.run("GET a"), bulk("zero"));
        assert_eq!(h.run("TTL a"), int(100));
        h.run("SELECT 0");
        assert_eq!(h.run("GET b"), bulk("three"));
        assert_eq!(h.run("SWAPDB 0 0"), Frame::ok());
        assert_eq!(h.run("GET b"), bulk("three"));
        // Compared with Redis 7.4.
        assert_eq!(h.run("SWAPDB x 1"), err("ERR invalid first DB index"));
        assert_eq!(h.run("SWAPDB 1 x"), err("ERR invalid second DB index"));
        assert_eq!(h.run("SWAPDB 0 16"), err("ERR DB index is out of range"));
        assert_eq!(h.run("SWAPDB -1 0"), err("ERR DB index is out of range"));
    }

    #[test]
    fn copy() {
        let mut h = Harness::new();
        h.run("SET a 1 EX 100");
        assert_eq!(h.run("COPY a b"), int(1));
        assert_eq!(h.run("GET b"), bulk("1"));
        assert_eq!(h.run("TTL b"), int(100), "TTL is copied");
        assert_eq!(h.run("GET a"), bulk("1"), "source is kept");

        h.run("SET c other");
        assert_eq!(h.run("COPY a c"), int(0));
        assert_eq!(h.run("GET c"), bulk("other"));
        assert_eq!(h.run("COPY a c REPLACE"), int(1));
        assert_eq!(h.run("GET c"), bulk("1"));

        assert_eq!(h.run("COPY missing z"), int(0));
        assert_eq!(
            h.run("COPY a a"),
            err("ERR source and destination objects are the same")
        );
    }

    #[test]
    fn copy_across_databases() {
        let mut h = Harness::new();
        h.run("SET a 1");
        assert_eq!(h.run("COPY a a DB 2"), int(1));
        h.run("SELECT 2");
        assert_eq!(h.run("GET a"), bulk("1"));
        h.run("SELECT 0");
        assert_eq!(h.run("COPY a a DB 99"), err("ERR DB index is out of range"));
        assert_eq!(
            h.run("COPY a a DB x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(h.run("COPY a b BOGUS"), err("ERR syntax error"));
    }

    #[test]
    fn keys_matches_patterns_and_hides_expired() {
        let mut h = Harness::new();
        h.run("MSET user:1 a user:2 b order:1 c");
        h.run("SET user:gone x PX 10");
        h.advance(10);
        let mut found = strings(h.run("KEYS user:*"));
        found.sort();
        assert_eq!(found, ["user:1", "user:2"]);
        assert_eq!(strings(h.run("KEYS *")).len(), 3);
        assert!(strings(h.run("KEYS nothing*")).is_empty());
    }

    /// Runs a complete SCAN iteration and returns every key it reported.
    fn scan_all(h: &mut Harness, options: &str) -> Vec<String> {
        let mut cursor = "0".to_owned();
        let mut all = Vec::new();
        for _ in 0..100_000 {
            let Frame::Array(mut reply) = h.run(&format!("SCAN {cursor} {options}")) else {
                panic!("expected array");
            };
            assert_eq!(reply.len(), 2);
            all.extend(strings(reply.pop().unwrap()));
            let Some(Frame::Bulk(next)) = reply.pop() else {
                panic!("cursor must be a bulk string");
            };
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                return all;
            }
        }
        panic!("SCAN never terminated");
    }

    #[test]
    fn scan_returns_every_key() {
        let mut h = Harness::new();
        for i in 0..1000 {
            h.cmd(&["SET", &format!("key:{i}"), "v"]);
        }
        let found: HashSet<String> = scan_all(&mut h, "").into_iter().collect();
        assert_eq!(found.len(), 1000);

        let found: HashSet<String> = scan_all(&mut h, "COUNT 1000").into_iter().collect();
        assert_eq!(found.len(), 1000);
    }

    #[test]
    fn scan_match_type_and_expiry() {
        let mut h = Harness::new();
        for i in 0..100 {
            h.cmd(&["SET", &format!("a:{i}"), "v"]);
            h.cmd(&["SET", &format!("b:{i}"), "v"]);
        }
        h.run("SET a:dead v PX 5");
        h.advance(5);

        let found: HashSet<String> = scan_all(&mut h, "MATCH a:*").into_iter().collect();
        assert_eq!(found.len(), 100);
        assert!(found.iter().all(|k| k.starts_with("a:")));

        assert!(scan_all(&mut h, "TYPE string").len() >= 200);
        assert!(scan_all(&mut h, "TYPE list").is_empty());
        assert!(scan_all(&mut h, "MATCH nothing*").is_empty());
    }

    #[test]
    fn scan_on_empty_database_and_errors() {
        let mut h = Harness::new();
        assert_eq!(
            h.run("SCAN 0"),
            Frame::Array(vec![bulk("0"), Frame::Array(vec![])])
        );
        assert_eq!(h.run("SCAN abc"), err("ERR invalid cursor"));
        assert_eq!(h.run("SCAN -1"), err("ERR invalid cursor"));
        assert_eq!(h.run("SCAN 0 COUNT 0"), err("ERR syntax error"));
        assert_eq!(
            h.run("SCAN 0 COUNT x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(h.run("SCAN 0 MATCH"), err("ERR syntax error"));
        assert_eq!(h.run("SCAN 0 BOGUS x"), err("ERR syntax error"));
    }

    #[test]
    fn randomkey() {
        let mut h = Harness::new();
        assert_eq!(h.run("RANDOMKEY"), Frame::Null);
        h.run("MSET a 1 b 2");
        for _ in 0..20 {
            let key = h.run("RANDOMKEY");
            assert!(key == bulk("a") || key == bulk("b"));
        }
    }

    #[test]
    fn expire_and_ttl() {
        let mut h = Harness::new();
        assert_eq!(h.run("EXPIRE missing 10"), int(0));
        assert_eq!(h.run("TTL missing"), int(-2));
        assert_eq!(h.run("PTTL missing"), int(-2));

        h.run("SET a 1");
        assert_eq!(h.run("TTL a"), int(-1));
        assert_eq!(h.run("EXPIRE a 10"), int(1));
        assert_eq!(h.run("TTL a"), int(10));
        assert_eq!(h.run("PTTL a"), int(10_000));

        h.advance(9_400);
        assert_eq!(h.run("TTL a"), int(1), "600 ms rounds to 1 s");
        assert_eq!(h.run("PTTL a"), int(600));

        h.advance(599);
        assert_eq!(h.run("GET a"), bulk("1"));
        h.advance(1);
        assert_eq!(h.run("GET a"), Frame::Null);
        assert_eq!(h.run("TTL a"), int(-2));
    }

    #[test]
    fn pexpire_expireat_pexpireat_and_expiretime() {
        let mut h = Harness::new();
        let now = h.now as i64;
        h.run("SET a 1");
        assert_eq!(h.run("EXPIRETIME a"), int(-1));
        assert_eq!(h.run("PEXPIRETIME missing"), int(-2));

        assert_eq!(h.run("PEXPIRE a 1500"), int(1));
        assert_eq!(h.run("PTTL a"), int(1500));
        assert_eq!(h.run("PEXPIRETIME a"), int(now + 1500));

        let at_seconds = now / 1000 + 60;
        assert_eq!(h.cmd(&["EXPIREAT", "a", &at_seconds.to_string()]), int(1));
        assert_eq!(h.run("EXPIRETIME a"), int(at_seconds));

        let at_millis = now + 777;
        assert_eq!(h.cmd(&["PEXPIREAT", "a", &at_millis.to_string()]), int(1));
        assert_eq!(h.run("PTTL a"), int(777));
    }

    #[test]
    fn expiring_in_the_past_deletes_the_key() {
        let mut h = Harness::new();
        h.run("SET a 1");
        assert_eq!(h.run("EXPIRE a -1"), int(1));
        assert_eq!(h.run("EXISTS a"), int(0));

        h.run("SET b 1");
        assert_eq!(h.run("EXPIREAT b 1"), int(1));
        assert_eq!(h.run("EXISTS b"), int(0));

        h.run("SET c 1");
        assert_eq!(h.run("PEXPIRE c 0"), int(1));
        assert_eq!(h.run("EXISTS c"), int(0));
    }

    #[test]
    fn expire_conditions() {
        let mut h = Harness::new();
        h.run("SET a 1");

        assert_eq!(h.run("EXPIRE a 100 XX"), int(0), "XX needs an existing TTL");
        assert_eq!(
            h.run("EXPIRE a 100 GT"),
            int(0),
            "no TTL counts as infinite"
        );
        assert_eq!(h.run("EXPIRE a 100 LT"), int(1));
        assert_eq!(h.run("EXPIRE a 200 NX"), int(0), "NX needs no TTL");
        assert_eq!(h.run("EXPIRE a 50 GT"), int(0));
        assert_eq!(h.run("EXPIRE a 200 GT"), int(1));
        assert_eq!(h.run("EXPIRE a 300 LT"), int(0));
        assert_eq!(h.run("EXPIRE a 150 lt"), int(1));
        assert_eq!(h.run("EXPIRE a 400 XX"), int(1));
        assert_eq!(h.run("TTL a"), int(400));

        h.run("SET b 1");
        assert_eq!(h.run("EXPIRE b 100 NX"), int(1));
    }

    #[test]
    fn expire_errors() {
        let mut h = Harness::new();
        h.run("SET a 1");
        assert_eq!(
            h.run("EXPIRE a abc"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            h.run("EXPIRE a 10 NX XX"),
            err("ERR NX and XX, GT or LT options at the same time are not compatible")
        );
        assert_eq!(
            h.run("EXPIRE a 10 GT LT"),
            err("ERR GT and LT options at the same time are not compatible")
        );
        assert_eq!(h.run("EXPIRE a 10 FOO"), err("ERR Unsupported option FOO"));
        assert_eq!(
            h.run("EXPIRE a 9223372036854775807"),
            err("ERR invalid expire time in 'expire' command")
        );
        assert_eq!(
            h.run("PEXPIRE a 9223372036854775807"),
            err("ERR invalid expire time in 'pexpire' command")
        );
        assert_eq!(h.run("TTL a"), int(-1), "failed commands change nothing");
    }

    #[test]
    fn persist() {
        let mut h = Harness::new();
        assert_eq!(h.run("PERSIST missing"), int(0));
        h.run("SET a 1");
        assert_eq!(h.run("PERSIST a"), int(0));
        h.run("EXPIRE a 10");
        assert_eq!(h.run("PERSIST a"), int(1));
        assert_eq!(h.run("TTL a"), int(-1));
        h.advance(60_000);
        assert_eq!(h.run("GET a"), bulk("1"));
    }

    #[test]
    fn writes_keep_or_clear_the_ttl_as_redis_does() {
        let mut h = Harness::new();
        h.run("SET n 1 EX 100");
        h.run("INCR n");
        h.run("APPEND n 0");
        assert_eq!(h.run("TTL n"), int(100), "INCR and APPEND keep the TTL");
        h.run("SET n 5");
        assert_eq!(h.run("TTL n"), int(-1), "plain SET clears it");
    }

    #[test]
    fn expiretime_rounds_to_the_nearest_second() {
        let mut h = Harness::new();
        h.run("SET a v");
        h.run("PEXPIREAT a 1900000000499");
        assert_eq!(h.run("EXPIRETIME a"), Frame::Integer(1_900_000_000));
        h.run("PEXPIREAT a 1900000000500");
        assert_eq!(h.run("EXPIRETIME a"), Frame::Integer(1_900_000_001));
        assert_eq!(h.run("PEXPIRETIME a"), Frame::Integer(1_900_000_000_500));
    }
}
