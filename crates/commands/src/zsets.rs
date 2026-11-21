//! Sorted-set commands (the non-blocking ones).

use std::ops::Range;

use bytes::Bytes;
use rexyra_engine::{glob_match, Db, LexBound, ScoreBound, Set, SetTtl, Value, ZSet};
use rexyra_resp::{format_double, Frame, Protocol};

use crate::notify::{self, notify, GENERIC, ZSET};
use crate::strings::parse_float;
use crate::{
    int, is, parse_cursor, parse_scan_options, parse_timeout, reply, sample_distinct, wrong_type,
    BlockKind, Ctx, ERR_SYNTAX,
};

/// Deletes `key` if its sorted set has become empty, and says so.
fn remove_if_empty(ctx: &mut Ctx<'_>, key: &[u8]) {
    let (db, now) = ctx.db();
    if db.remove_if_empty(key, now) {
        notify(ctx, GENERIC, "del", key);
    }
}

const ERR_NOT_FLOAT: &str = "ERR value is not a valid float";
const ERR_NAN: &str = "ERR resulting score is not a number (NaN)";
const ERR_POSITIVE: &str = "ERR value is out of range, must be positive";
const ERR_LIMIT_WITHOUT_BY: &str =
    "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX";
const ERR_SCORES_WITH_LEX: &str =
    "ERR syntax error, WITHSCORES not supported in combination with BYLEX";

type Scored = Vec<(Bytes, f64)>;

fn zset_ref<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a ZSet>, Frame> {
    match db.get(key, now) {
        None => Ok(None),
        Some(Value::ZSet(zset)) => Ok(Some(zset)),
        Some(_) => Err(wrong_type()),
    }
}

fn zset_mut<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a mut ZSet>, Frame> {
    match db.get_mut(key, now) {
        None => Ok(None),
        Some(Value::ZSet(zset)) => Ok(Some(zset)),
        Some(_) => Err(wrong_type()),
    }
}

fn zset_or_create<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<&'a mut ZSet, Frame> {
    match db.get_or_insert_with(key, now, || Value::ZSet(ZSet::new())) {
        Value::ZSet(zset) => Ok(zset),
        _ => Err(wrong_type()),
    }
}

fn is_resp3(ctx: &Ctx<'_>) -> bool {
    ctx.client.proto == Protocol::Resp3
}

/// Members, optionally with scores. RESP3 pairs each member with its score;
/// RESP2 flattens them into one array.
fn scored_reply(items: Scored, with_scores: bool, resp3: bool) -> Frame {
    Frame::Array(if !with_scores {
        items.into_iter().map(|(m, _)| Frame::Bulk(m)).collect()
    } else if resp3 {
        items
            .into_iter()
            .map(|(m, s)| Frame::Array(vec![Frame::Bulk(m), Frame::Double(s)]))
            .collect()
    } else {
        items
            .into_iter()
            .flat_map(|(m, s)| [Frame::Bulk(m), Frame::Double(s)])
            .collect()
    })
}

fn collect(zset: &ZSet, ranks: Range<usize>, reverse: bool) -> Scored {
    zset.range(ranks, reverse)
        .map(|(member, score)| (member.clone(), score))
        .collect()
}

// --- range selection --------------------------------------------------------

/// What a range command selects by, with its two ends already in ascending
/// order (`REV` variants take them the other way round on the wire).
enum Bounds {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

fn score_bound(arg: &[u8]) -> Result<ScoreBound, Frame> {
    let (text, exclusive) = match arg.strip_prefix(b"(") {
        Some(rest) => (rest, true),
        None => (arg, false),
    };
    parse_float(text)
        .map(|value| ScoreBound { value, exclusive })
        .ok_or_else(|| Frame::error("ERR min or max is not a float"))
}

fn lex_bound(arg: &[u8]) -> Result<LexBound, Frame> {
    match arg {
        b"-" => Ok(LexBound::Min),
        b"+" => Ok(LexBound::Max),
        [b'[', rest @ ..] => Ok(LexBound::Inclusive(Bytes::copy_from_slice(rest))),
        [b'(', rest @ ..] => Ok(LexBound::Exclusive(Bytes::copy_from_slice(rest))),
        _ => Err(Frame::error("ERR min or max not valid string range item")),
    }
}

fn score_bounds(min: &[u8], max: &[u8]) -> Result<Bounds, Frame> {
    Ok(Bounds::Score(score_bound(min)?, score_bound(max)?))
}

fn lex_bounds(min: &[u8], max: &[u8]) -> Result<Bounds, Frame> {
    Ok(Bounds::Lex(lex_bound(min)?, lex_bound(max)?))
}

/// Resolves `start..=stop` indexes (negative = from the end) to a rank range.
fn index_range(start: i64, stop: i64, len: usize) -> Range<usize> {
    let len = len as i64;
    let resolve = |index: i64| {
        if index < 0 {
            index.saturating_add(len)
        } else {
            index
        }
    };
    let (start, stop) = (resolve(start).max(0), resolve(stop).min(len - 1));
    if start > stop {
        0..0
    } else {
        start as usize..stop as usize + 1
    }
}

/// The ranks (ascending) that `bounds` selects, after applying `LIMIT`.
fn select(zset: &ZSet, bounds: &Bounds, reverse: bool, limit: Option<(i64, i64)>) -> Range<usize> {
    let ranks = match bounds {
        Bounds::Rank(start, stop) => {
            let picked = index_range(*start, *stop, zset.len());
            if reverse {
                // Indexes count from the highest score down.
                zset.len() - picked.end..zset.len() - picked.start
            } else {
                picked
            }
        }
        Bounds::Score(min, max) => zset.score_range(*min, *max),
        Bounds::Lex(min, max) => zset.lex_range(min, max),
    };
    let Some((offset, count)) = limit else {
        return ranks;
    };
    let available = match usize::try_from(offset) {
        Ok(offset) if offset < ranks.len() => ranks.len() - offset,
        _ => return 0..0, // negative offset, or past the end
    };
    let offset = offset as usize;
    // A negative count means "everything from the offset".
    let take = usize::try_from(count).map_or(available, |count| count.min(available));
    if reverse {
        let end = ranks.end - offset;
        end - take..end
    } else {
        let start = ranks.start + offset;
        start..start + take
    }
}

struct RangeQuery {
    bounds: Bounds,
    reverse: bool,
    limit: Option<(i64, i64)>,
    with_scores: bool,
}

impl RangeQuery {
    fn run(&self, ctx: &mut Ctx<'_>, key: &[u8]) -> Result<Scored, Frame> {
        let (db, now) = ctx.db();
        Ok(match zset_ref(db, key, now)? {
            Some(zset) => {
                let ranks = select(zset, &self.bounds, self.reverse, self.limit);
                collect(zset, ranks, self.reverse)
            }
            None => Vec::new(),
        })
    }

    fn reply(&self, ctx: &mut Ctx<'_>, key: &[u8]) -> Result<Frame, Frame> {
        let resp3 = is_resp3(ctx);
        Ok(scored_reply(self.run(ctx, key)?, self.with_scores, resp3))
    }
}

/// Parses the unified `ZRANGE` syntax:
/// `start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
fn parse_zrange(
    start: &[u8],
    stop: &[u8],
    mut options: &[Bytes],
    allow_scores: bool,
) -> Result<RangeQuery, Frame> {
    let (mut by_score, mut by_lex, mut reverse, mut with_scores) = (false, false, false, false);
    let mut limit = None;
    loop {
        match options {
            [] => break,
            [opt, rest @ ..] if is(opt, "BYSCORE") => {
                by_score = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "BYLEX") => {
                by_lex = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "REV") => {
                reverse = true;
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "WITHSCORES") && allow_scores => {
                with_scores = true;
                options = rest;
            }
            [opt, offset, count, rest @ ..] if is(opt, "LIMIT") => {
                limit = Some((int(offset)?, int(count)?));
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
    if by_score && by_lex {
        return Err(Frame::error(ERR_SYNTAX));
    }
    if limit.is_some() && !by_score && !by_lex {
        return Err(Frame::error(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if with_scores && by_lex {
        return Err(Frame::error(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    // With REV, score and lex ranges are written `max min`.
    let (min, max) = if reverse {
        (stop, start)
    } else {
        (start, stop)
    };
    let bounds = if by_score {
        score_bounds(min, max)?
    } else if by_lex {
        lex_bounds(min, max)?
    } else {
        Bounds::Rank(int(start)?, int(stop)?)
    };
    Ok(RangeQuery {
        bounds,
        reverse,
        limit,
        with_scores,
    })
}

/// Options of the older range commands: `[WITHSCORES] [LIMIT offset count]`.
///
/// Both are parsed for every command; a combination the command does not
/// support is rejected afterwards with the same specific message `ZRANGE`
/// gives, as in Redis.
fn legacy_options(
    mut options: &[Bytes],
    by_lex: bool,
) -> Result<(bool, Option<(i64, i64)>), Frame> {
    let (mut with_scores, mut limit) = (false, None);
    loop {
        match options {
            [] => break,
            [opt, rest @ ..] if is(opt, "WITHSCORES") => {
                with_scores = true;
                options = rest;
            }
            [opt, offset, count, rest @ ..] if is(opt, "LIMIT") => {
                limit = Some((int(offset)?, int(count)?));
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
    if with_scores && by_lex {
        return Err(Frame::error(ERR_SCORES_WITH_LEX));
    }
    Ok((with_scores, limit))
}

/// `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
pub(crate) fn zrange(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(parse_zrange(&args[2], &args[3], &args[4..], true).and_then(|q| q.reply(ctx, &args[1])))
}

/// `ZREVRANGE key start stop [WITHSCORES]`
pub(crate) fn zrevrange(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let bounds = Bounds::Rank(int(&args[2])?, int(&args[3])?);
        let (with_scores, limit) = legacy_options(&args[4..], false)?;
        if limit.is_some() {
            return Err(Frame::error(ERR_LIMIT_WITHOUT_BY));
        }
        RangeQuery {
            bounds,
            reverse: true,
            limit,
            with_scores,
        }
        .reply(ctx, &args[1])
    })())
}

fn legacy_range(ctx: &mut Ctx<'_>, args: &[Bytes], by_lex: bool, reverse: bool) -> Frame {
    reply((|| {
        let (min, max) = if reverse {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let bounds = if by_lex {
            lex_bounds(min, max)?
        } else {
            score_bounds(min, max)?
        };
        let (with_scores, limit) = legacy_options(&args[4..], by_lex)?;
        RangeQuery {
            bounds,
            reverse,
            limit,
            with_scores,
        }
        .reply(ctx, &args[1])
    })())
}

/// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
pub(crate) fn zrangebyscore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    legacy_range(ctx, args, false, false)
}

/// `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]`
pub(crate) fn zrevrangebyscore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    legacy_range(ctx, args, false, true)
}

/// `ZRANGEBYLEX key min max [LIMIT offset count]`
pub(crate) fn zrangebylex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    legacy_range(ctx, args, true, false)
}

/// `ZREVRANGEBYLEX key max min [LIMIT offset count]`
pub(crate) fn zrevrangebylex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    legacy_range(ctx, args, true, true)
}

/// Replaces `key` with a sorted set of `items`, or deletes it if there are
/// none. Replies with the number stored. `event` names the change.
fn store(ctx: &mut Ctx<'_>, key: &[u8], items: &Scored, event: &'static str) -> Frame {
    let (db, now) = ctx.db();
    if items.is_empty() {
        if db.remove(key, now) {
            notify(ctx, GENERIC, "del", key);
        }
    } else {
        let mut zset = ZSet::new();
        for (member, score) in items {
            zset.insert(member, *score);
        }
        db.set(key, Value::ZSet(zset), SetTtl::Clear, now);
        notify(ctx, ZSET, event, key);
    }
    Frame::Integer(items.len() as i64)
}

/// `ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]`
pub(crate) fn zrangestore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let query = parse_zrange(&args[3], &args[4], &args[5..], false)?;
        // The source is read as `ZRANGE` reads it.
        notify::reading(ctx, true);
        let items = query.run(ctx, &args[2]);
        notify::reading(ctx, false);
        Ok(store(ctx, &args[1], &items?, "zrangestore"))
    })())
}

// --- counting ---------------------------------------------------------------

fn count_in(ctx: &mut Ctx<'_>, key: &[u8], bounds: Result<Bounds, Frame>) -> Frame {
    reply((|| {
        let bounds = bounds?;
        let (db, now) = ctx.db();
        let count =
            zset_ref(db, key, now)?.map_or(0, |zset| select(zset, &bounds, false, None).len());
        Ok(Frame::Integer(count as i64))
    })())
}

pub(crate) fn zcount(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    count_in(ctx, &args[1], score_bounds(&args[2], &args[3]))
}

pub(crate) fn zlexcount(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    count_in(ctx, &args[1], lex_bounds(&args[2], &args[3]))
}

pub(crate) fn zcard(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(zset_ref(db, &args[1], now).map(|z| Frame::Integer(z.map_or(0, |z| z.len() as i64))))
}

// --- lookups ----------------------------------------------------------------

pub(crate) fn zscore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(zset_ref(db, &args[1], now).map(|zset| {
        zset.and_then(|z| z.score(&args[2]))
            .map_or(Frame::Null, Frame::Double)
    }))
}

pub(crate) fn zmscore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(zset_ref(db, &args[1], now).map(|zset| {
        Frame::Array(
            args[2..]
                .iter()
                .map(|member| {
                    zset.and_then(|z| z.score(member))
                        .map_or(Frame::Null, Frame::Double)
                })
                .collect(),
        )
    }))
}

/// `ZRANK key member [WITHSCORE]` / `ZREVRANK key member [WITHSCORE]`
fn rank(ctx: &mut Ctx<'_>, args: &[Bytes], reverse: bool) -> Frame {
    reply((|| {
        let with_score = match &args[3..] {
            [] => false,
            [opt] if is(opt, "WITHSCORE") => true,
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let (db, now) = ctx.db();
        let found = zset_ref(db, &args[1], now)?.and_then(|zset| {
            let rank = zset.rank(&args[2])?;
            let rank = if reverse { zset.len() - 1 - rank } else { rank };
            Some((rank as i64, zset.score(&args[2])?))
        });
        Ok(match (found, with_score) {
            (Some((rank, _)), false) => Frame::Integer(rank),
            (Some((rank, score)), true) => {
                Frame::Array(vec![Frame::Integer(rank), Frame::Double(score)])
            }
            (None, false) => Frame::Null,
            (None, true) => Frame::NullArray,
        })
    })())
}

pub(crate) fn zrank(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    rank(ctx, args, false)
}

pub(crate) fn zrevrank(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    rank(ctx, args, true)
}

// --- writes -----------------------------------------------------------------

/// `ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]`
pub(crate) fn zadd(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(zadd_impl(ctx, args))
}

fn zadd_impl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, false);
    let mut rest = &args[2..];
    while let [opt, tail @ ..] = rest {
        let flag = if is(opt, "NX") {
            &mut nx
        } else if is(opt, "XX") {
            &mut xx
        } else if is(opt, "GT") {
            &mut gt
        } else if is(opt, "LT") {
            &mut lt
        } else if is(opt, "CH") {
            &mut ch
        } else if is(opt, "INCR") {
            &mut incr
        } else {
            break;
        };
        *flag = true;
        rest = tail;
    }

    let (pairs, leftover) = rest.as_chunks::<2>();
    if pairs.is_empty() || !leftover.is_empty() {
        return Err(Frame::error(ERR_SYNTAX));
    }
    if nx && xx {
        return Err(Frame::error(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if (gt && lt) || (nx && (gt || lt)) {
        return Err(Frame::error(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if incr && pairs.len() > 1 {
        return Err(Frame::error(
            "ERR INCR option supports a single increment-element pair",
        ));
    }
    // Every score is validated before anything is written.
    let scores = pairs
        .iter()
        .map(|[score, _]| parse_float(score).ok_or_else(|| Frame::error(ERR_NOT_FLOAT)))
        .collect::<Result<Vec<f64>, Frame>>()?;

    let key = &args[1];
    let (db, now) = ctx.db();
    if xx && zset_ref(db, key, now)?.is_none() {
        // Nothing can be updated in a set that does not exist; don't create it.
        return Ok(if incr { Frame::Null } else { Frame::Integer(0) });
    }
    let zset = zset_or_create(db, key, now)?;

    let (mut added, mut updated) = (0, 0);
    let mut incremented = None;
    let mut failure = None;
    for ([_, member], &score) in pairs.iter().zip(&scores) {
        let old = zset.score(member);
        if (old.is_some() && nx) || (old.is_none() && xx) {
            continue;
        }
        let new = if incr {
            old.unwrap_or(0.0) + score
        } else {
            score
        };
        if new.is_nan() {
            failure = Some(Frame::error(ERR_NAN));
            break;
        }
        if old.is_some_and(|old| (gt && new <= old) || (lt && new >= old)) {
            continue;
        }
        zset.insert(member, new);
        match old {
            None => added += 1,
            Some(old) if old != new => updated += 1,
            Some(_) => {}
        }
        incremented = Some(new);
    }
    if added + updated == 0 {
        db.unchanged(key);
    }
    let emptied = db.remove_if_empty(key, now);
    if added + updated > 0 {
        notify(ctx, ZSET, if incr { "zincr" } else { "zadd" }, key);
    }
    if emptied {
        notify(ctx, GENERIC, "del", key);
    }

    if let Some(error) = failure {
        return Err(error);
    }
    Ok(if incr {
        incremented.map_or(Frame::Null, Frame::Double)
    } else {
        Frame::Integer(if ch { added + updated } else { added })
    })
}

/// `ZINCRBY key increment member`
pub(crate) fn zincrby(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let delta = parse_float(&args[2]).ok_or_else(|| Frame::error(ERR_NOT_FLOAT))?;
        let (db, now) = ctx.db();
        let current = zset_ref(db, &args[1], now)?.and_then(|zset| zset.score(&args[3]));
        let updated = current.unwrap_or(0.0) + delta;
        if updated.is_nan() {
            return Err(Frame::error(ERR_NAN));
        }
        zset_or_create(db, &args[1], now)?.insert(&args[3], updated);
        if current == Some(updated) {
            db.unchanged(&args[1]);
        } else {
            notify(ctx, ZSET, "zincr", &args[1]);
        }
        Ok(Frame::Double(updated))
    })())
}

pub(crate) fn zrem(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        let Some(zset) = zset_mut(db, &args[1], now)? else {
            return Ok(Frame::Integer(0));
        };
        let removed = args[2..]
            .iter()
            .filter(|member| zset.remove(member).is_some())
            .count();
        if removed == 0 {
            db.unchanged(&args[1]);
            return Ok(Frame::Integer(0));
        }
        notify(ctx, ZSET, "zrem", &args[1]);
        remove_if_empty(ctx, &args[1]);
        Ok(Frame::Integer(removed as i64))
    })())
}

/// Removes the members `bounds` selects and returns them in ascending order
/// (descending when `reverse`). `event` names the change, if there is one.
fn remove_selected(
    ctx: &mut Ctx<'_>,
    key: &[u8],
    event: &'static str,
    pick: impl FnOnce(&ZSet) -> (Range<usize>, bool),
) -> Result<Option<Scored>, Frame> {
    let (db, now) = ctx.db();
    let Some(zset) = zset_mut(db, key, now)? else {
        return Ok(None);
    };
    let (ranks, reverse) = pick(zset);
    let removed = collect(zset, ranks, reverse);
    for (member, _) in &removed {
        zset.remove(member);
    }
    if removed.is_empty() {
        db.unchanged(key);
    } else {
        notify(ctx, ZSET, event, key);
        remove_if_empty(ctx, key);
    }
    Ok(Some(removed))
}

fn remove_range(
    ctx: &mut Ctx<'_>,
    key: &[u8],
    bounds: Result<Bounds, Frame>,
    event: &'static str,
) -> Frame {
    reply((|| {
        let bounds = bounds?;
        let removed = remove_selected(ctx, key, event, |zset| {
            (select(zset, &bounds, false, None), false)
        })?;
        Ok(Frame::Integer(removed.map_or(0, |r| r.len() as i64)))
    })())
}

pub(crate) fn zremrangebyrank(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let bounds = int(&args[2]).and_then(|start| Ok(Bounds::Rank(start, int(&args[3])?)));
    remove_range(ctx, &args[1], bounds, "zremrangebyrank")
}

pub(crate) fn zremrangebyscore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    remove_range(
        ctx,
        &args[1],
        score_bounds(&args[2], &args[3]),
        "zremrangebyscore",
    )
}

pub(crate) fn zremrangebylex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    remove_range(
        ctx,
        &args[1],
        lex_bounds(&args[2], &args[3]),
        "zremrangebylex",
    )
}

// --- pop --------------------------------------------------------------------

/// Removes up to `count` members from the low end, or the high end when
/// `max`. `None` if the key does not exist.
fn pop_ends(
    ctx: &mut Ctx<'_>,
    key: &[u8],
    max: bool,
    count: usize,
) -> Result<Option<Scored>, Frame> {
    let event = if max { "zpopmax" } else { "zpopmin" };
    remove_selected(ctx, key, event, |zset| {
        let take = count.min(zset.len());
        if max {
            (zset.len() - take..zset.len(), true)
        } else {
            (0..take, false)
        }
    })
}

/// `ZPOPMIN key [count]` / `ZPOPMAX key [count]`
fn pop(ctx: &mut Ctx<'_>, args: &[Bytes], max: bool) -> Frame {
    reply((|| {
        let count = match &args[2..] {
            [] => None,
            [count] => match int(count)? {
                count if count >= 0 => Some(count as usize),
                _ => return Err(Frame::error(ERR_POSITIVE)),
            },
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let resp3 = is_resp3(ctx);
        let popped = pop_ends(ctx, &args[1], max, count.unwrap_or(1))?.unwrap_or_default();
        // RESP3 nests the pairs only when a count was given explicitly.
        Ok(scored_reply(popped, true, resp3 && count.is_some()))
    })())
}

pub(crate) fn zpopmin(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    pop(ctx, args, false)
}

pub(crate) fn zpopmax(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    pop(ctx, args, true)
}

/// The shared body of `ZMPOP` and `BZMPOP`; `args` starts at `numkeys`.
/// `timeout` is how long to block when every key is missing, or `None` for
/// the non-blocking command.
fn multi_pop(ctx: &mut Ctx<'_>, args: &[Bytes], timeout: Option<&[u8]>) -> Result<Frame, Frame> {
    let numkeys = match int(&args[0]) {
        Ok(n) if n > 0 => n as usize,
        _ => return Err(Frame::error("ERR numkeys should be greater than 0")),
    };
    let rest = &args[1..];
    if rest.len() <= numkeys {
        return Err(Frame::error(ERR_SYNTAX));
    }
    let (keys, rest) = rest.split_at(numkeys);
    let max = if is(&rest[0], "MAX") {
        true
    } else if is(&rest[0], "MIN") {
        false
    } else {
        return Err(Frame::error(ERR_SYNTAX));
    };
    let count = match &rest[1..] {
        [] => 1,
        [opt, count] if is(opt, "COUNT") => match int(count) {
            Ok(count) if count > 0 => count as usize,
            _ => return Err(Frame::error("ERR count should be greater than 0")),
        },
        _ => return Err(Frame::error(ERR_SYNTAX)),
    };
    // Redis checks the timeout last, although it is the first argument.
    let wait = match timeout {
        Some(timeout) => Some(parse_timeout(timeout, ctx.now)?),
        None => None,
    };

    for key in keys {
        if let Some(popped) = pop_ends(ctx, key, max, count)? {
            // Pairs are nested in both protocol versions here.
            return Ok(Frame::Array(vec![
                Frame::Bulk(key.clone()),
                scored_reply(popped, true, true),
            ]));
        }
    }
    Ok(match wait {
        Some(deadline) => ctx.block_on(keys, BlockKind::ZSet, deadline),
        None => Frame::NullArray,
    })
}

/// `ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]`
pub(crate) fn zmpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(multi_pop(ctx, &args[1..], None))
}

/// `BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]`
pub(crate) fn bzmpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(multi_pop(ctx, &args[2..], Some(&args[1])))
}

/// `BZPOPMIN key [key ...] timeout` / `BZPOPMAX key [key ...] timeout` — pops
/// from the first key that holds a sorted set, or waits for one to appear.
/// Replies with the key, the member and its score.
fn blocking_pop(ctx: &mut Ctx<'_>, args: &[Bytes], max: bool) -> Frame {
    reply((|| {
        let (timeout, keys) = args[1..]
            .split_last()
            .expect("arity guarantees a key and a timeout");
        let deadline = parse_timeout(timeout, ctx.now)?;
        for key in keys {
            if let Some(popped) = pop_ends(ctx, key, max, 1)? {
                let mut triple = vec![Frame::Bulk(key.clone())];
                for (member, score) in popped {
                    triple.push(Frame::Bulk(member));
                    triple.push(Frame::Double(score));
                }
                return Ok(Frame::Array(triple));
            }
        }
        Ok(ctx.block_on(keys, BlockKind::ZSet, deadline))
    })())
}

pub(crate) fn bzpopmin(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    blocking_pop(ctx, args, false)
}

pub(crate) fn bzpopmax(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    blocking_pop(ctx, args, true)
}

// --- algebra ----------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Algebra {
    Union,
    Inter,
    Diff,
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Aggregate {
    fn apply(self, a: f64, b: f64) -> f64 {
        match self {
            // +inf + -inf is NaN, which Redis defines as 0.
            Aggregate::Sum => Some(a + b).filter(|sum| !sum.is_nan()).unwrap_or(0.0),
            Aggregate::Min => a.min(b),
            Aggregate::Max => a.max(b),
        }
    }
}

/// One input of a set operation. Plain sets are accepted too; their members
/// count as having score 1.
#[derive(Clone, Copy)]
enum Input<'a> {
    Sorted(&'a ZSet),
    Plain(&'a Set),
    Missing,
}

impl Input<'_> {
    fn len(&self) -> usize {
        match self {
            Input::Sorted(zset) => zset.len(),
            Input::Plain(set) => set.len(),
            Input::Missing => 0,
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            Input::Sorted(zset) => zset.score(member),
            Input::Plain(set) => set.contains(member).then_some(1.0),
            Input::Missing => None,
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, f64)> + '_> {
        match self {
            Input::Sorted(zset) => {
                Box::new(zset.iter().map(|(member, score)| (member.clone(), score)))
            }
            Input::Plain(set) => Box::new(set.iter().map(|member| (member, 1.0))),
            Input::Missing => Box::new(std::iter::empty()),
        }
    }
}

struct AlgebraArgs<'a> {
    keys: &'a [Bytes],
    weights: Vec<f64>,
    aggregate: Aggregate,
    with_scores: bool,
}

/// Parses `numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM | MIN | MAX]
/// [WITHSCORES]`. `command` is the lower-case name used in error messages.
fn parse_algebra<'a>(
    args: &'a [Bytes],
    command: &str,
    op: Algebra,
    allow_scores: bool,
) -> Result<AlgebraArgs<'a>, Frame> {
    let numkeys = int(&args[0])?;
    if numkeys < 1 {
        return Err(Frame::error(format!(
            "ERR at least 1 input key is needed for '{command}' command"
        )));
    }
    let numkeys = numkeys as usize;
    if numkeys > args.len() - 1 {
        return Err(Frame::error(ERR_SYNTAX));
    }
    let (keys, mut options) = args[1..].split_at(numkeys);
    let mut parsed = AlgebraArgs {
        keys,
        weights: vec![1.0; numkeys],
        aggregate: Aggregate::Sum,
        with_scores: false,
    };
    loop {
        match options {
            [] => return Ok(parsed),
            [opt, rest @ ..]
                if is(opt, "WEIGHTS") && op != Algebra::Diff && rest.len() >= numkeys =>
            {
                let (weights, rest) = rest.split_at(numkeys);
                parsed.weights = weights
                    .iter()
                    .map(|w| {
                        parse_float(w)
                            .ok_or_else(|| Frame::error("ERR weight value is not a float"))
                    })
                    .collect::<Result<_, _>>()?;
                options = rest;
            }
            [opt, kind, rest @ ..] if is(opt, "AGGREGATE") && op != Algebra::Diff => {
                parsed.aggregate = if is(kind, "SUM") {
                    Aggregate::Sum
                } else if is(kind, "MIN") {
                    Aggregate::Min
                } else if is(kind, "MAX") {
                    Aggregate::Max
                } else {
                    return Err(Frame::error(ERR_SYNTAX));
                };
                options = rest;
            }
            [opt, rest @ ..] if is(opt, "WITHSCORES") && allow_scores => {
                parsed.with_scores = true;
                options = rest;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
}

/// Computes the result of a set operation, ordered by `(score, member)`.
/// `limit` stops an intersection early (0 = no limit), for `ZINTERCARD`.
fn combine(
    ctx: &mut Ctx<'_>,
    parsed: &AlgebraArgs<'_>,
    op: Algebra,
    limit: usize,
) -> Result<Scored, Frame> {
    let (db, now) = ctx.db();
    // First pass: expire and type-check each key, so that the second pass
    // can borrow all of them together.
    for key in parsed.keys {
        match db.get(key, now) {
            None | Some(Value::ZSet(_) | Value::Set(_)) => {}
            Some(_) => return Err(wrong_type()),
        }
    }
    let inputs: Vec<Input<'_>> = parsed
        .keys
        .iter()
        .map(|key| match db.peek(key, now) {
            Some(Value::ZSet(zset)) => Input::Sorted(zset),
            Some(Value::Set(set)) => Input::Plain(set),
            _ => Input::Missing,
        })
        .collect();
    // Multiplying an infinite score by a zero weight is NaN; Redis uses 0.
    let weigh = |score: f64, index: usize| {
        Some(score * parsed.weights[index])
            .filter(|weighted| !weighted.is_nan())
            .unwrap_or(0.0)
    };

    let mut result = ZSet::new();
    match op {
        Algebra::Union => {
            for (index, input) in inputs.iter().enumerate() {
                for (member, score) in input.iter() {
                    let score = weigh(score, index);
                    let score = result
                        .score(&member)
                        .map_or(score, |seen| parsed.aggregate.apply(seen, score));
                    result.insert(&member, score);
                }
            }
        }
        Algebra::Inter => {
            // Walk the smallest input; everything else is a lookup.
            let smallest = (0..inputs.len())
                .min_by_key(|&index| inputs[index].len())
                .expect("numkeys is at least 1");
            'members: for (member, score) in inputs[smallest].iter() {
                let mut total = weigh(score, smallest);
                for (index, input) in inputs.iter().enumerate() {
                    if index == smallest {
                        continue;
                    }
                    match input.score(&member) {
                        Some(score) => total = parsed.aggregate.apply(total, weigh(score, index)),
                        None => continue 'members,
                    }
                }
                result.insert(&member, total);
                if limit != 0 && result.len() >= limit {
                    break;
                }
            }
        }
        Algebra::Diff => {
            let (first, others) = inputs.split_first().expect("numkeys is at least 1");
            for (member, score) in first.iter() {
                if !others.iter().any(|input| input.score(&member).is_some()) {
                    result.insert(&member, score);
                }
            }
        }
    }
    let len = result.len();
    Ok(collect(&result, 0..len, false))
}

fn algebra(ctx: &mut Ctx<'_>, args: &[Bytes], command: &str, op: Algebra) -> Frame {
    reply((|| {
        let parsed = parse_algebra(&args[1..], command, op, true)?;
        let resp3 = is_resp3(ctx);
        Ok(scored_reply(
            combine(ctx, &parsed, op, 0)?,
            parsed.with_scores,
            resp3,
        ))
    })())
}

fn algebra_store(ctx: &mut Ctx<'_>, args: &[Bytes], command: &str, op: Algebra) -> Frame {
    reply((|| {
        let parsed = parse_algebra(&args[2..], command, op, false)?;
        // The sources are read as `ZUNION` reads them.
        notify::reading(ctx, true);
        let items = combine(ctx, &parsed, op, 0);
        notify::reading(ctx, false);
        let event = match op {
            Algebra::Union => "zunionstore",
            Algebra::Inter => "zinterstore",
            Algebra::Diff => "zdiffstore",
        };
        Ok(store(ctx, &args[1], &items?, event))
    })())
}

pub(crate) fn zunion(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, "zunion", Algebra::Union)
}

pub(crate) fn zinter(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, "zinter", Algebra::Inter)
}

pub(crate) fn zdiff(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, "zdiff", Algebra::Diff)
}

pub(crate) fn zunionstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, "zunionstore", Algebra::Union)
}

pub(crate) fn zinterstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, "zinterstore", Algebra::Inter)
}

pub(crate) fn zdiffstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, "zdiffstore", Algebra::Diff)
}

/// `ZINTERCARD numkeys key [key ...] [LIMIT limit]`
pub(crate) fn zintercard(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let numkeys = match int(&args[1]) {
            Ok(n) if n > 0 => n as usize,
            Ok(_) => {
                return Err(Frame::error(
                    "ERR at least 1 input key is needed for 'zintercard' command",
                ))
            }
            Err(e) => return Err(e),
        };
        let rest = &args[2..];
        if numkeys > rest.len() {
            return Err(Frame::error(ERR_SYNTAX));
        }
        let (keys, options) = rest.split_at(numkeys);
        let limit = match options {
            [] => 0,
            [opt, limit] if is(opt, "LIMIT") => match int(limit) {
                Ok(limit) if limit >= 0 => limit as usize,
                _ => return Err(Frame::error("ERR LIMIT can't be negative")),
            },
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let parsed = AlgebraArgs {
            keys,
            weights: vec![1.0; numkeys],
            aggregate: Aggregate::Sum,
            with_scores: false,
        };
        let members = combine(ctx, &parsed, Algebra::Inter, limit)?;
        Ok(Frame::Integer(members.len() as i64))
    })())
}

// --- scan and random --------------------------------------------------------

/// `ZSCAN key cursor [MATCH pattern] [COUNT count]`
pub(crate) fn zscan(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let mut cursor = parse_cursor(&args[2])?;
        let options = parse_scan_options(&args[3..])?;
        let (db, now) = ctx.db();
        let mut found = Vec::new();
        if let Some(zset) = zset_ref(db, &args[1], now)? {
            let mut budget = options.count.saturating_mul(10);
            loop {
                cursor = zset.scan(cursor, |member, score| {
                    if options.pattern.is_none_or(|p| glob_match(p, member)) {
                        found.push(Frame::Bulk(member.clone()));
                        found.push(Frame::bulk(format_double(score)));
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

/// `ZRANDMEMBER key [count [WITHSCORES]]`
pub(crate) fn zrandmember(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (count, with_scores) = match &args[2..] {
            [] => (None, false),
            [count] => (Some(int(count)?), false),
            [count, opt] if is(opt, "WITHSCORES") => (Some(int(count)?), true),
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        if count.is_some_and(|count| count < -(i64::MAX / 2)) {
            return Err(Frame::error("ERR value is out of range"));
        }
        let resp3 = is_resp3(ctx);
        let (db, now) = ctx.db();
        let zset = zset_ref(db, &args[1], now)?;
        let Some(count) = count else {
            return Ok(zset
                .and_then(ZSet::random)
                .map_or(Frame::Null, |(member, _)| Frame::Bulk(member.clone())));
        };
        let Some(zset) = zset else {
            return Ok(Frame::Array(Vec::new()));
        };
        let picked: Scored = if count as u64 >= zset.len() as u64 && count >= 0 {
            // Asking for everything returns it highest score first, as Redis does.
            collect(zset, 0..zset.len(), true)
        } else if count >= 0 {
            sample_distinct(collect(zset, 0..zset.len(), false), count as usize)
        } else {
            (0..count.unsigned_abs())
                .filter_map(|_| zset.random())
                .map(|(member, score)| (member.clone(), score))
                .collect()
        };
        Ok(scored_reply(picked, with_scores, resp3))
    })())
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness, WRONGTYPE};
    use rexyra_resp::Frame;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn list(items: &[&str]) -> Frame {
        Frame::Array(bulks(items))
    }

    /// A flat RESP2 `member, score, ...` reply.
    fn scored(items: &[(&str, f64)]) -> Frame {
        Frame::Array(
            items
                .iter()
                .flat_map(|(m, s)| [bulk(m), Frame::Double(*s)])
                .collect(),
        )
    }

    fn abc() -> Harness {
        let mut h = Harness::new();
        h.run("ZADD z 1 a 2 b 3 c 4 d 5 e");
        h
    }

    #[test]
    fn zadd_basics() {
        let mut h = Harness::new();
        assert_eq!(h.run("ZADD z 1 a 2 b"), int(2));
        assert_eq!(h.run("ZADD z 5 a 3 c"), int(1), "only new members count");
        assert_eq!(
            h.run("ZADD z CH 9 a 3 c 4 d"),
            int(2),
            "CH counts changes too"
        );
        assert_eq!(h.run("ZCARD z"), int(4));
        assert_eq!(h.run("ZSCORE z a"), Frame::Double(9.0));
        assert_eq!(h.run("ZSCORE z nope"), Frame::Null);
        assert_eq!(h.run("ZSCORE missing a"), Frame::Null);
        assert_eq!(
            h.run("ZMSCORE z a nope b"),
            Frame::Array(vec![Frame::Double(9.0), Frame::Null, Frame::Double(2.0)])
        );
        assert_eq!(h.run("TYPE z"), Frame::simple("zset"));
        assert_eq!(h.run("ZRANGE z 0 -1"), list(&["b", "c", "d", "a"]));
        assert_eq!(h.run("ZADD z inf top -inf bottom"), int(2));
        assert_eq!(
            h.run("ZRANGE z 0 -1"),
            list(&["bottom", "b", "c", "d", "a", "top"])
        );
    }

    #[test]
    fn zadd_conditions() {
        let mut h = Harness::new();
        h.run("ZADD z 5 a");
        assert_eq!(h.run("ZADD z NX 9 a 1 b"), int(1));
        assert_eq!(h.run("ZSCORE z a"), Frame::Double(5.0));
        assert_eq!(h.run("ZADD z XX 7 a 1 new"), int(0));
        assert_eq!(h.run("ZSCORE z a"), Frame::Double(7.0));
        assert_eq!(h.run("ZSCORE z new"), Frame::Null);

        assert_eq!(h.run("ZADD z GT CH 3 a"), int(0));
        assert_eq!(h.run("ZADD z GT CH 8 a"), int(1));
        assert_eq!(h.run("ZADD z LT CH 9 a"), int(0));
        assert_eq!(h.run("ZADD z LT CH 2 a"), int(1));
        assert_eq!(
            h.run("ZADD z GT 1 fresh"),
            int(1),
            "GT still adds new members"
        );

        assert_eq!(h.run("ZADD missing XX 1 a"), int(0));
        assert_eq!(h.run("EXISTS missing"), int(0));
    }

    #[test]
    fn zadd_incr_and_zincrby() {
        let mut h = Harness::new();
        assert_eq!(h.run("ZADD z INCR 5 a"), Frame::Double(5.0));
        assert_eq!(h.run("ZADD z INCR 2.5 a"), Frame::Double(7.5));
        assert_eq!(h.run("ZADD z NX INCR 1 a"), Frame::Null);
        assert_eq!(h.run("ZADD z XX INCR 1 nope"), Frame::Null);
        assert_eq!(h.run("ZADD z GT INCR -1 a"), Frame::Null);
        assert_eq!(h.run("ZINCRBY z 2.5 a"), Frame::Double(10.0));
        assert_eq!(h.run("ZINCRBY z -3 fresh"), Frame::Double(-3.0));
        assert_eq!(
            h.run("ZINCRBY z x a"),
            err("ERR value is not a valid float")
        );

        h.run("ZADD z inf top");
        assert_eq!(
            h.run("ZINCRBY z -inf top"),
            err("ERR resulting score is not a number (NaN)")
        );
        assert_eq!(
            h.run("ZADD z INCR -inf top"),
            err("ERR resulting score is not a number (NaN)")
        );
        assert_eq!(h.run("ZSCORE z top"), Frame::Double(f64::INFINITY));
    }

    #[test]
    fn zadd_errors_change_nothing() {
        let mut h = Harness::new();
        for (command, message) in [
            (
                "ZADD z 1",
                "ERR wrong number of arguments for 'zadd' command",
            ),
            ("ZADD z 1 a 2", "ERR syntax error"),
            ("ZADD z NX CH", "ERR syntax error"),
            ("ZADD z NX 1", "ERR syntax error"),
            ("ZADD z x a", "ERR value is not a valid float"),
            ("ZADD z 1 a nan b", "ERR value is not a valid float"),
            (
                "ZADD z NX XX 1 a",
                "ERR XX and NX options at the same time are not compatible",
            ),
            (
                "ZADD z GT LT 1 a",
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            ),
            (
                "ZADD z NX GT 1 a",
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            ),
            (
                "ZADD z INCR 1 a 2 b",
                "ERR INCR option supports a single increment-element pair",
            ),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        assert_eq!(h.run("EXISTS z"), int(0));
    }

    #[test]
    fn zrem_and_ranks() {
        let mut h = abc();
        assert_eq!(h.run("ZRANK z a"), int(0));
        assert_eq!(h.run("ZRANK z e"), int(4));
        assert_eq!(h.run("ZREVRANK z a"), int(4));
        assert_eq!(h.run("ZRANK z nope"), Frame::Null);
        assert_eq!(h.run("ZRANK missing a"), Frame::Null);
        assert_eq!(
            h.run("ZRANK z c WITHSCORE"),
            Frame::Array(vec![int(2), Frame::Double(3.0)])
        );
        assert_eq!(h.run("ZRANK z nope WITHSCORE"), Frame::NullArray);
        assert_eq!(h.run("ZRANK z a BOGUS"), err("ERR syntax error"));

        assert_eq!(h.run("ZREM z a nope c"), int(2));
        assert_eq!(h.run("ZRANK z b"), int(0));
        assert_eq!(h.run("ZREM missing a"), int(0));
        assert_eq!(h.run("ZREM z b d e"), int(3));
        assert_eq!(
            h.run("EXISTS z"),
            int(0),
            "an emptied sorted set is deleted"
        );
    }

    #[test]
    fn zrange_by_rank() {
        let mut h = abc();
        assert_eq!(h.run("ZRANGE z 0 -1"), list(&["a", "b", "c", "d", "e"]));
        assert_eq!(h.run("ZRANGE z 1 2"), list(&["b", "c"]));
        assert_eq!(h.run("ZRANGE z -2 -1"), list(&["d", "e"]));
        assert_eq!(h.run("ZRANGE z 3 100"), list(&["d", "e"]));
        assert_eq!(h.run("ZRANGE z 3 1"), list(&[]));
        assert_eq!(h.run("ZRANGE z 0 1 REV"), list(&["e", "d"]));
        assert_eq!(h.run("ZREVRANGE z 0 1"), list(&["e", "d"]));
        assert_eq!(h.run("ZREVRANGE z -2 -1"), list(&["b", "a"]));
        assert_eq!(
            h.run("ZRANGE z 0 1 WITHSCORES"),
            scored(&[("a", 1.0), ("b", 2.0)])
        );
        assert_eq!(h.run("ZREVRANGE z 0 0 WITHSCORES"), scored(&[("e", 5.0)]));
        assert_eq!(h.run("ZRANGE missing 0 -1"), list(&[]));
        assert_eq!(
            h.run("ZRANGE z a 1"),
            err("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn zrange_by_score() {
        let mut h = abc();
        assert_eq!(h.run("ZRANGE z 2 4 BYSCORE"), list(&["b", "c", "d"]));
        assert_eq!(h.run("ZRANGE z (2 4 BYSCORE"), list(&["c", "d"]));
        assert_eq!(h.run("ZRANGE z 2 (4 BYSCORE"), list(&["b", "c"]));
        assert_eq!(
            h.run("ZRANGE z -inf +inf BYSCORE"),
            list(&["a", "b", "c", "d", "e"])
        );
        assert_eq!(h.run("ZRANGE z 4 2 BYSCORE REV"), list(&["d", "c", "b"]));
        assert_eq!(h.run("ZRANGE z 2 4 BYSCORE REV"), list(&[]));
        assert_eq!(h.run("ZRANGE z 1 5 BYSCORE LIMIT 1 2"), list(&["b", "c"]));
        assert_eq!(
            h.run("ZRANGE z 5 1 BYSCORE REV LIMIT 1 2"),
            list(&["d", "c"])
        );
        assert_eq!(h.run("ZRANGE z 1 5 BYSCORE LIMIT 3 -1"), list(&["d", "e"]));
        assert_eq!(h.run("ZRANGE z 1 5 BYSCORE LIMIT 9 2"), list(&[]));
        assert_eq!(h.run("ZRANGE z 1 5 BYSCORE LIMIT -1 2"), list(&[]));
        assert_eq!(h.run("ZRANGE z 1 5 BYSCORE LIMIT 0 0"), list(&[]));

        assert_eq!(h.run("ZRANGEBYSCORE z 2 4"), list(&["b", "c", "d"]));
        assert_eq!(
            h.run("ZRANGEBYSCORE z (2 4 WITHSCORES LIMIT 0 1"),
            scored(&[("c", 3.0)])
        );
        assert_eq!(h.run("ZREVRANGEBYSCORE z 4 2"), list(&["d", "c", "b"]));
        assert_eq!(
            h.run("ZREVRANGEBYSCORE z +inf -inf LIMIT 1 2"),
            list(&["d", "c"])
        );
        assert_eq!(h.run("ZCOUNT z 2 4"), int(3));
        assert_eq!(h.run("ZCOUNT z (2 (4"), int(1));
        assert_eq!(h.run("ZCOUNT z -inf +inf"), int(5));
        assert_eq!(h.run("ZCOUNT missing 0 1"), int(0));

        assert_eq!(
            h.run("ZRANGE z a b BYSCORE"),
            err("ERR min or max is not a float")
        );
        assert_eq!(h.run("ZCOUNT z a 1"), err("ERR min or max is not a float"));
        assert_eq!(
            h.run("ZRANGEBYSCORE z 1 (x"),
            err("ERR min or max is not a float")
        );
    }

    #[test]
    fn zrange_by_lex() {
        let mut h = Harness::new();
        h.run("ZADD z 0 a 0 b 0 c 0 d 0 e");
        assert_eq!(
            h.run("ZRANGE z - + BYLEX"),
            list(&["a", "b", "c", "d", "e"])
        );
        assert_eq!(h.run("ZRANGE z [b [d BYLEX"), list(&["b", "c", "d"]));
        assert_eq!(h.run("ZRANGE z (b (d BYLEX"), list(&["c"]));
        assert_eq!(h.run("ZRANGE z [d [b BYLEX REV"), list(&["d", "c", "b"]));
        assert_eq!(h.run("ZRANGE z - + BYLEX LIMIT 1 2"), list(&["b", "c"]));
        assert_eq!(h.run("ZRANGEBYLEX z [b +"), list(&["b", "c", "d", "e"]));
        assert_eq!(h.run("ZRANGEBYLEX z - (c LIMIT 1 5"), list(&["b"]));
        assert_eq!(h.run("ZREVRANGEBYLEX z + [c"), list(&["e", "d", "c"]));
        assert_eq!(h.run("ZLEXCOUNT z - +"), int(5));
        assert_eq!(h.run("ZLEXCOUNT z [b (e"), int(3));
        assert_eq!(h.run("ZLEXCOUNT z + -"), int(0));

        let bad = "ERR min or max not valid string range item";
        assert_eq!(h.run("ZRANGE z b d BYLEX"), err(bad));
        assert_eq!(h.run("ZLEXCOUNT z [b d"), err(bad));
    }

    #[test]
    fn zrange_option_errors() {
        let mut h = abc();
        assert_eq!(
            h.run("ZRANGE z 0 -1 LIMIT 0 1"),
            err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
        );
        assert_eq!(
            h.run("ZRANGE z - + BYLEX WITHSCORES"),
            err("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
        );
        assert_eq!(h.run("ZRANGE z 0 -1 BOGUS"), err("ERR syntax error"));
        assert_eq!(h.run("ZRANGE z 0 1 BYSCORE BYLEX"), err("ERR syntax error"));
        // The older commands reject these with the same specific messages.
        assert_eq!(
            h.run("ZREVRANGE z 0 -1 LIMIT 0 1"),
            err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
        );
        assert_eq!(
            h.run("ZRANGEBYLEX z - + WITHSCORES"),
            err("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
        );
        assert_eq!(h.run("ZRANGEBYSCORE z 0 1 BOGUS"), err("ERR syntax error"));
    }

    #[test]
    fn resp3_pairs_members_with_scores() {
        let mut h = abc();
        h.run("HELLO 3");
        assert_eq!(
            h.run("ZRANGE z 0 1 WITHSCORES"),
            Frame::Array(vec![
                Frame::Array(vec![bulk("a"), Frame::Double(1.0)]),
                Frame::Array(vec![bulk("b"), Frame::Double(2.0)]),
            ])
        );
        // Without a count, ZPOPMIN stays flat; with one, it nests.
        assert_eq!(
            h.run("ZPOPMIN z"),
            Frame::Array(vec![bulk("a"), Frame::Double(1.0)])
        );
        assert_eq!(
            h.run("ZPOPMIN z 1"),
            Frame::Array(vec![Frame::Array(vec![bulk("b"), Frame::Double(2.0)])])
        );
    }

    #[test]
    fn zrangestore() {
        let mut h = abc();
        assert_eq!(h.run("ZRANGESTORE dst z 1 3"), int(3));
        assert_eq!(
            h.run("ZRANGE dst 0 -1 WITHSCORES"),
            scored(&[("b", 2.0), ("c", 3.0), ("d", 4.0)])
        );
        assert_eq!(h.run("ZRANGESTORE dst z 4 5 BYSCORE"), int(2));
        assert_eq!(h.run("ZRANGE dst 0 -1"), list(&["d", "e"]));
        assert_eq!(h.run("ZRANGESTORE dst z 5 1 BYSCORE REV LIMIT 0 1"), int(1));
        assert_eq!(h.run("ZRANGE dst 0 -1"), list(&["e"]));
        assert_eq!(h.run("ZRANGESTORE dst z 50 60 BYSCORE"), int(0));
        assert_eq!(h.run("EXISTS dst"), int(0));
        assert_eq!(
            h.run("ZRANGESTORE dst z 0 -1 WITHSCORES"),
            err("ERR syntax error")
        );
    }

    #[test]
    fn zremrange() {
        let mut h = abc();
        assert_eq!(h.run("ZREMRANGEBYRANK z 0 1"), int(2));
        assert_eq!(h.run("ZRANGE z 0 -1"), list(&["c", "d", "e"]));
        assert_eq!(h.run("ZREMRANGEBYRANK z -1 -1"), int(1));
        assert_eq!(h.run("ZREMRANGEBYSCORE z (3 4"), int(1));
        assert_eq!(h.run("ZRANGE z 0 -1"), list(&["c"]));
        assert_eq!(h.run("ZREMRANGEBYSCORE z -inf +inf"), int(1));
        assert_eq!(h.run("EXISTS z"), int(0));
        assert_eq!(h.run("ZREMRANGEBYRANK missing 0 -1"), int(0));

        h.run("ZADD l 0 a 0 b 0 c 0 d");
        assert_eq!(h.run("ZREMRANGEBYLEX l [b (d"), int(2));
        assert_eq!(h.run("ZRANGE l 0 -1"), list(&["a", "d"]));
        assert_eq!(
            h.run("ZREMRANGEBYLEX l b d"),
            err("ERR min or max not valid string range item")
        );
    }

    #[test]
    fn zpop() {
        let mut h = abc();
        assert_eq!(h.run("ZPOPMIN z"), scored(&[("a", 1.0)]));
        assert_eq!(h.run("ZPOPMAX z"), scored(&[("e", 5.0)]));
        assert_eq!(h.run("ZPOPMAX z 2"), scored(&[("d", 4.0), ("c", 3.0)]));
        assert_eq!(h.run("ZPOPMIN z 0"), scored(&[]));
        assert_eq!(h.run("ZPOPMIN z 10"), scored(&[("b", 2.0)]));
        assert_eq!(h.run("EXISTS z"), int(0));
        assert_eq!(h.run("ZPOPMIN z"), scored(&[]));
        assert_eq!(
            h.run("ZPOPMIN z -1"),
            err("ERR value is out of range, must be positive")
        );
    }

    #[test]
    fn zmpop() {
        let mut h = Harness::new();
        h.run("ZADD b 1 x 2 y 3 z");
        let pair = |m: &str, s: f64| Frame::Array(vec![bulk(m), Frame::Double(s)]);
        assert_eq!(
            h.run("ZMPOP 2 a b MIN"),
            Frame::Array(vec![bulk("b"), Frame::Array(vec![pair("x", 1.0)])])
        );
        assert_eq!(
            h.run("ZMPOP 2 a b MAX COUNT 10"),
            Frame::Array(vec![
                bulk("b"),
                Frame::Array(vec![pair("z", 3.0), pair("y", 2.0)])
            ])
        );
        assert_eq!(h.run("ZMPOP 2 a b MIN"), Frame::NullArray);
        assert_eq!(
            h.run("ZMPOP 0 a MIN"),
            err("ERR numkeys should be greater than 0")
        );
        assert_eq!(h.run("ZMPOP 1 a MID"), err("ERR syntax error"));
        assert_eq!(
            h.run("ZMPOP 1 a MIN COUNT 0"),
            err("ERR count should be greater than 0")
        );
    }

    fn two_sets() -> Harness {
        let mut h = Harness::new();
        h.run("ZADD a 1 x 2 y 3 z");
        h.run("ZADD b 10 y 20 z 30 w");
        h
    }

    #[test]
    fn zunion_zinter_zdiff() {
        let mut h = two_sets();
        assert_eq!(
            h.run("ZUNION 2 a b WITHSCORES"),
            scored(&[("x", 1.0), ("y", 12.0), ("z", 23.0), ("w", 30.0)])
        );
        assert_eq!(h.run("ZUNION 2 a b"), list(&["x", "y", "z", "w"]));
        assert_eq!(
            h.run("ZINTER 2 a b WITHSCORES"),
            scored(&[("y", 12.0), ("z", 23.0)])
        );
        assert_eq!(h.run("ZDIFF 2 a b WITHSCORES"), scored(&[("x", 1.0)]));
        assert_eq!(h.run("ZDIFF 2 b a"), list(&["w"]));
        assert_eq!(h.run("ZINTER 2 a missing"), list(&[]));
        assert_eq!(h.run("ZUNION 2 missing a"), list(&["x", "y", "z"]));
    }

    #[test]
    fn weights_and_aggregates() {
        let mut h = two_sets();
        assert_eq!(
            h.run("ZUNION 2 a b WEIGHTS 10 1 WITHSCORES"),
            // Equal scores order by member: w before y.
            scored(&[("x", 10.0), ("w", 30.0), ("y", 30.0), ("z", 50.0)])
        );
        assert_eq!(
            h.run("ZINTER 2 a b AGGREGATE MIN WITHSCORES"),
            scored(&[("y", 2.0), ("z", 3.0)])
        );
        assert_eq!(
            h.run("ZINTER 2 a b AGGREGATE MAX WITHSCORES"),
            scored(&[("y", 10.0), ("z", 20.0)])
        );
        assert_eq!(
            h.run("ZINTER 2 a b WEIGHTS 2 0.5 AGGREGATE SUM WITHSCORES"),
            scored(&[("y", 9.0), ("z", 16.0)])
        );
        assert_eq!(
            h.run("ZUNION 2 a b WEIGHTS 1 x"),
            err("ERR weight value is not a float")
        );
        assert_eq!(h.run("ZUNION 2 a b WEIGHTS 1"), err("ERR syntax error"));
        assert_eq!(h.run("ZUNION 2 a b AGGREGATE AVG"), err("ERR syntax error"));
        assert_eq!(h.run("ZDIFF 2 a b WEIGHTS 1 1"), err("ERR syntax error"));
    }

    #[test]
    fn store_variants_and_plain_set_inputs() {
        let mut h = two_sets();
        assert_eq!(h.run("ZUNIONSTORE d 2 a b"), int(4));
        assert_eq!(h.run("ZSCORE d z"), Frame::Double(23.0));
        assert_eq!(h.run("ZINTERSTORE d 2 a b"), int(2));
        assert_eq!(h.run("ZRANGE d 0 -1"), list(&["y", "z"]));
        assert_eq!(h.run("ZDIFFSTORE d 2 a b"), int(1));
        assert_eq!(h.run("ZRANGE d 0 -1"), list(&["x"]));
        assert_eq!(h.run("ZINTERSTORE d 2 a missing"), int(0));
        assert_eq!(h.run("EXISTS d"), int(0));
        assert_eq!(
            h.run("ZUNIONSTORE a 2 a b"),
            int(4),
            "destination may be a source"
        );
        assert_eq!(
            h.run("ZUNIONSTORE d 2 a b WITHSCORES"),
            err("ERR syntax error")
        );

        // Members of a plain set count with score 1.
        h.run("SADD s y z q");
        assert_eq!(
            h.run("ZINTER 2 b s WITHSCORES"),
            scored(&[("y", 11.0), ("z", 21.0)])
        );
        assert_eq!(
            h.run("ZUNION 1 s WITHSCORES"),
            scored(&[("q", 1.0), ("y", 1.0), ("z", 1.0)])
        );
    }

    #[test]
    fn algebra_errors() {
        let mut h = two_sets();
        assert_eq!(
            h.run("ZUNION 0 a"),
            err("ERR at least 1 input key is needed for 'zunion' command")
        );
        assert_eq!(
            h.run("ZINTERSTORE d 0 a"),
            err("ERR at least 1 input key is needed for 'zinterstore' command")
        );
        assert_eq!(h.run("ZUNION 3 a b"), err("ERR syntax error"));
        assert_eq!(
            h.run("ZUNION x a"),
            err("ERR value is not an integer or out of range")
        );
        h.run("SET str v");
        assert_eq!(h.run("ZUNION 2 a str"), err(WRONGTYPE));
        assert_eq!(h.run("ZINTERSTORE d 2 a str"), err(WRONGTYPE));
    }

    #[test]
    fn zintercard() {
        let mut h = two_sets();
        assert_eq!(h.run("ZINTERCARD 2 a b"), int(2));
        assert_eq!(h.run("ZINTERCARD 2 a b LIMIT 1"), int(1));
        assert_eq!(h.run("ZINTERCARD 2 a b LIMIT 0"), int(2));
        assert_eq!(
            h.run("ZINTERCARD 0 a"),
            err("ERR at least 1 input key is needed for 'zintercard' command")
        );
        assert_eq!(h.run("ZINTERCARD 3 a b"), err("ERR syntax error"));
        assert_eq!(
            h.run("ZINTERCARD x a"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            h.run("ZINTERCARD 2 a b LIMIT -1"),
            err("ERR LIMIT can't be negative")
        );
    }

    #[test]
    fn zscan_and_zrandmember() {
        let mut h = abc();
        assert_eq!(
            h.run("ZSCAN z 0"),
            Frame::Array(vec![
                bulk("0"),
                list(&["a", "1", "b", "2", "c", "3", "d", "4", "e", "5"])
            ])
        );
        assert_eq!(
            h.run("ZSCAN z 0 MATCH [ab]"),
            Frame::Array(vec![bulk("0"), list(&["a", "1", "b", "2"])])
        );
        assert_eq!(
            h.run("ZSCAN missing 0"),
            Frame::Array(vec![bulk("0"), list(&[])])
        );

        assert_eq!(h.run("ZRANDMEMBER missing"), Frame::Null);
        assert_eq!(h.run("ZRANDMEMBER missing 2"), list(&[]));
        assert!(bulks(&["a", "b", "c", "d", "e"]).contains(&h.run("ZRANDMEMBER z")));
        // Asking for at least the whole set returns it highest score first.
        assert_eq!(h.run("ZRANDMEMBER z 10"), list(&["e", "d", "c", "b", "a"]));
        assert_eq!(
            h.run("ZRANDMEMBER z 5 WITHSCORES"),
            scored(&[("e", 5.0), ("d", 4.0), ("c", 3.0), ("b", 2.0), ("a", 1.0)])
        );
        assert_eq!(h.run("ZRANDMEMBER z 0"), list(&[]));
        let Frame::Array(two) = h.run("ZRANDMEMBER z 2") else {
            panic!()
        };
        assert_eq!(two.len(), 2);
        assert_ne!(two[0], two[1]);
        let Frame::Array(many) = h.run("ZRANDMEMBER z -12") else {
            panic!()
        };
        assert_eq!(many.len(), 12);
        assert_eq!(h.run("ZCARD z"), int(5));
    }

    #[test]
    fn large_sorted_set() {
        let mut h = Harness::new();
        for i in 0..1000 {
            h.cmd(&["ZADD", "big", &i.to_string(), &format!("m{i:04}")]);
        }
        assert_eq!(h.run("ZCARD big"), int(1000));
        assert_eq!(h.run("ZRANK big m0500"), int(500));
        assert_eq!(h.run("ZREVRANK big m0500"), int(499));
        assert_eq!(h.run("ZCOUNT big 100 199"), int(100));
        assert_eq!(h.run("ZRANGE big 998 -1"), list(&["m0998", "m0999"]));
        assert_eq!(h.run("OBJECT ENCODING big"), bulk("skiplist"));
        assert_eq!(h.run("ZREMRANGEBYSCORE big 0 899"), int(900));
        assert_eq!(h.run("ZRANK big m0900"), int(0));
        h.run("ZADD small 1 a");
        assert_eq!(h.run("OBJECT ENCODING small"), bulk("listpack"));
    }

    #[test]
    fn wrong_type() {
        let mut h = Harness::new();
        h.run("SET str v");
        for command in [
            "ZADD str 1 a",
            "ZREM str a",
            "ZCARD str",
            "ZSCORE str a",
            "ZMSCORE str a",
            "ZRANK str a",
            "ZINCRBY str 1 a",
            "ZRANGE str 0 -1",
            "ZRANGEBYSCORE str 0 1",
            "ZRANGEBYLEX str - +",
            "ZCOUNT str 0 1",
            "ZLEXCOUNT str - +",
            "ZREMRANGEBYRANK str 0 1",
            "ZPOPMIN str",
            "ZMPOP 1 str MIN",
            "ZSCAN str 0",
            "ZRANDMEMBER str",
            "ZRANGESTORE d str 0 -1",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
    }

    #[test]
    fn blocking_pops_return_at_once_when_there_is_data() {
        let mut h = Harness::new();
        h.run("ZADD z 1 a 2 b 3 c");
        assert_eq!(
            h.run("BZPOPMIN nokey z 0"),
            Frame::Array(vec![bulk("z"), bulk("a"), Frame::Double(1.0)])
        );
        assert_eq!(
            h.run("BZPOPMAX z 1"),
            Frame::Array(vec![bulk("z"), bulk("c"), Frame::Double(3.0)])
        );
        assert_eq!(
            h.run("BZMPOP 0 2 nokey z MAX COUNT 5"),
            Frame::Array(vec![
                bulk("z"),
                Frame::Array(vec![Frame::Array(vec![bulk("b"), Frame::Double(2.0)])]),
            ])
        );
        assert_eq!(h.block, None);
        assert_eq!(h.run("EXISTS z"), int(0));
    }

    #[test]
    fn blocking_pops_ask_to_wait_when_there_is_nothing() {
        let mut h = Harness::new();
        let now = h.now;
        for (command, deadline) in [
            ("BZPOPMIN a b 0", None),
            ("BZPOPMAX a b 0.25", Some(now + 250)),
            ("BZMPOP 3 2 a b MIN", Some(now + 3000)),
        ] {
            assert_eq!(h.run(command), Frame::NullArray, "{command}");
            let request = h.block.clone().expect(command);
            assert_eq!(request.keys, vec![&b"a"[..], &b"b"[..]], "{command}");
            assert_eq!(request.kind, crate::BlockKind::ZSet, "{command}");
            assert_eq!(request.deadline, deadline, "{command}");
        }
    }

    #[test]
    fn blocking_pop_errors_do_not_wait() {
        let mut h = Harness::new();
        h.run("SET s v");
        for (command, message) in [
            ("BZPOPMIN a x", "ERR timeout is not a float or out of range"),
            ("BZPOPMAX a -0.5", "ERR timeout is negative"),
            ("BZPOPMIN a s 0", WRONGTYPE),
            // The timeout is the last thing checked.
            ("BZMPOP x 0 a MIN", "ERR numkeys should be greater than 0"),
            ("BZMPOP x 1 a MID", "ERR syntax error"),
            ("BZMPOP 0 0 a MIN", "ERR numkeys should be greater than 0"),
            ("BZMPOP 0 1 a MID", "ERR syntax error"),
            (
                "BZMPOP 0 1 a MIN COUNT 0",
                "ERR count should be greater than 0",
            ),
            ("BZMPOP 0 1 s MIN", WRONGTYPE),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
            assert_eq!(h.block, None, "{command}");
        }
    }
}
