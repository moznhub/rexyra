//! List commands.
//!
//! The blocking variants (`BLPOP` and friends) behave like their plain
//! counterparts when there is something to return. Otherwise they ask the
//! server to park the client through [`Ctx::block_on`]; the server runs the
//! command again once one of the keys holds a list.

use std::collections::VecDeque;

use bytes::Bytes;
use rexyra_engine::{Db, Value};
use rexyra_resp::Frame;

use crate::{copied, int, is, parse_timeout, reply, wrong_type, BlockKind, Ctx, ERR_SYNTAX};

type List = VecDeque<Bytes>;

const ERR_NO_SUCH_KEY: &str = "ERR no such key";
const ERR_INDEX: &str = "ERR index out of range";
const ERR_POSITIVE: &str = "ERR value is out of range, must be positive";

fn list_ref<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a List>, Frame> {
    match db.get(key, now) {
        None => Ok(None),
        Some(Value::List(list)) => Ok(Some(list)),
        Some(_) => Err(wrong_type()),
    }
}

fn list_mut<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a mut List>, Frame> {
    match db.get_mut(key, now) {
        None => Ok(None),
        Some(Value::List(list)) => Ok(Some(list)),
        Some(_) => Err(wrong_type()),
    }
}

fn list_or_create<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<&'a mut List, Frame> {
    match db.get_or_insert_with(key, now, || Value::List(List::new())) {
        Value::List(list) => Ok(list),
        _ => Err(wrong_type()),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum End {
    Left,
    Right,
}

impl End {
    fn parse(arg: &[u8]) -> Result<End, Frame> {
        if is(arg, "LEFT") {
            Ok(End::Left)
        } else if is(arg, "RIGHT") {
            Ok(End::Right)
        } else {
            Err(Frame::error(ERR_SYNTAX))
        }
    }

    fn push(self, list: &mut List, element: Bytes) {
        match self {
            End::Left => list.push_front(element),
            End::Right => list.push_back(element),
        }
    }

    fn pop(self, list: &mut List) -> Option<Bytes> {
        match self {
            End::Left => list.pop_front(),
            End::Right => list.pop_back(),
        }
    }
}

/// Resolves an inclusive `start..=stop` range where negative indexes count
/// from the end. `None` means the range selects nothing.
fn clamp_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let resolve = |index: i64| {
        if index < 0 {
            index.saturating_add(len)
        } else {
            index
        }
    };
    let (start, stop) = (resolve(start).max(0), resolve(stop).min(len - 1));
    (start <= stop).then_some((start as usize, stop as usize))
}

/// Resolves a single index; `None` if it is outside the list.
fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let resolved = if index < 0 {
        index.checked_add(len as i64)?
    } else {
        index
    };
    (0..len as i64)
        .contains(&resolved)
        .then_some(resolved as usize)
}

// --- push -------------------------------------------------------------------

fn push(ctx: &mut Ctx<'_>, args: &[Bytes], end: End, only_if_exists: bool) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        let list = if only_if_exists {
            match list_mut(db, &args[1], now)? {
                Some(list) => list,
                None => return Ok(Frame::Integer(0)),
            }
        } else {
            list_or_create(db, &args[1], now)?
        };
        // Elements are pushed one at a time, so `LPUSH k a b c` leaves `c`
        // at the head.
        for element in &args[2..] {
            end.push(list, copied(element));
        }
        Ok(Frame::Integer(list.len() as i64))
    })())
}

pub(crate) fn lpush(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    push(ctx, args, End::Left, false)
}

pub(crate) fn rpush(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    push(ctx, args, End::Right, false)
}

pub(crate) fn lpushx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    push(ctx, args, End::Left, true)
}

pub(crate) fn rpushx(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    push(ctx, args, End::Right, true)
}

// --- pop --------------------------------------------------------------------

/// Pops up to `count` elements, deleting the key if the list empties.
/// `None` if the key does not exist.
fn pop_many(
    db: &mut Db,
    key: &[u8],
    now: u64,
    end: End,
    count: usize,
) -> Result<Option<Vec<Bytes>>, Frame> {
    let Some(list) = list_mut(db, key, now)? else {
        return Ok(None);
    };
    let popped: Vec<Bytes> = (0..count.min(list.len()))
        .filter_map(|_| end.pop(list))
        .collect();
    if popped.is_empty() {
        db.unchanged(key);
    }
    db.remove_if_empty(key, now);
    Ok(Some(popped))
}

/// `LPOP key [count]` / `RPOP key [count]`
fn pop(ctx: &mut Ctx<'_>, args: &[Bytes], end: End) -> Frame {
    reply((|| {
        let count = match &args[2..] {
            [] => None,
            [count] => match int(count) {
                Ok(count) if count >= 0 => Some(count as usize),
                _ => return Err(Frame::error(ERR_POSITIVE)),
            },
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let (db, now) = ctx.db();
        let popped = pop_many(db, &args[1], now, end, count.unwrap_or(1))?;
        Ok(match (popped, count) {
            (None, None) => Frame::Null,
            (None, Some(_)) => Frame::NullArray,
            (Some(mut one), None) => one.pop().map_or(Frame::Null, Frame::Bulk),
            (Some(many), Some(_)) => Frame::Array(many.into_iter().map(Frame::Bulk).collect()),
        })
    })())
}

pub(crate) fn lpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    pop(ctx, args, End::Left)
}

pub(crate) fn rpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    pop(ctx, args, End::Right)
}

/// `BLPOP key [key ...] timeout` / `BRPOP key [key ...] timeout` — pops from
/// the first key that holds a list, or waits for one to appear.
fn blocking_pop(ctx: &mut Ctx<'_>, args: &[Bytes], end: End) -> Frame {
    reply((|| {
        let (timeout, keys) = args[1..]
            .split_last()
            .expect("arity guarantees a key and a timeout");
        let deadline = parse_timeout(timeout, ctx.now)?;
        let (db, now) = ctx.db();
        for key in keys {
            if let Some(popped) = pop_many(db, key, now, end, 1)? {
                let mut pair = vec![Frame::Bulk(key.clone())];
                pair.extend(popped.into_iter().map(Frame::Bulk));
                return Ok(Frame::Array(pair));
            }
        }
        Ok(ctx.block_on(keys, BlockKind::List, deadline))
    })())
}

pub(crate) fn blpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    blocking_pop(ctx, args, End::Left)
}

pub(crate) fn brpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    blocking_pop(ctx, args, End::Right)
}

/// The shared body of `LMPOP` and `BLMPOP`; `args` starts at `numkeys`.
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
    let end = End::parse(&rest[0])?;
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

    let (db, now) = ctx.db();
    for key in keys {
        if let Some(popped) = pop_many(db, key, now, end, count)? {
            return Ok(Frame::Array(vec![
                Frame::Bulk(key.clone()),
                Frame::Array(popped.into_iter().map(Frame::Bulk).collect()),
            ]));
        }
    }
    Ok(match wait {
        Some(deadline) => ctx.block_on(keys, BlockKind::List, deadline),
        None => Frame::NullArray,
    })
}

/// `LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]` — pops from the
/// first of the keys that holds a non-empty list.
pub(crate) fn lmpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(multi_pop(ctx, &args[1..], None))
}

/// `BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]`
pub(crate) fn blmpop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(multi_pop(ctx, &args[2..], Some(&args[1])))
}

/// Moves one element between lists (or rotates a list when both keys are
/// the same). `None` if the source does not exist.
fn move_element(
    ctx: &mut Ctx<'_>,
    source: &[u8],
    destination: &[u8],
    from: End,
    to: End,
) -> Result<Option<Bytes>, Frame> {
    let (db, now) = ctx.db();
    if list_ref(db, source, now)?.is_none() {
        return Ok(None);
    }
    // Check the destination's type before removing anything from the source.
    list_ref(db, destination, now)?;
    let Some(element) = list_mut(db, source, now)?.and_then(|list| from.pop(list)) else {
        return Ok(None);
    };
    to.push(list_or_create(db, destination, now)?, element.clone());
    // Only now: when both keys are the same list, it is momentarily empty.
    db.remove_if_empty(source, now);
    Ok(Some(element))
}

fn moved_reply(moved: Option<Bytes>) -> Frame {
    moved.map_or(Frame::Null, Frame::Bulk)
}

/// `LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>`
pub(crate) fn lmove(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (from, to) = (End::parse(&args[3])?, End::parse(&args[4])?);
        move_element(ctx, &args[1], &args[2], from, to).map(moved_reply)
    })())
}

/// `RPOPLPUSH source destination`
pub(crate) fn rpoplpush(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(move_element(ctx, &args[1], &args[2], End::Right, End::Left).map(moved_reply))
}

/// Moves an element as [`move_element`] does, or waits for the source list
/// to appear.
fn blocking_move(
    ctx: &mut Ctx<'_>,
    source: &Bytes,
    destination: &[u8],
    ends: (End, End),
    timeout: &[u8],
) -> Result<Frame, Frame> {
    let deadline = parse_timeout(timeout, ctx.now)?;
    Ok(
        match move_element(ctx, source, destination, ends.0, ends.1)? {
            Some(element) => Frame::Bulk(element),
            None => ctx.block_on(std::slice::from_ref(source), BlockKind::List, deadline),
        },
    )
}

/// `BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout`
pub(crate) fn blmove(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let ends = (End::parse(&args[3])?, End::parse(&args[4])?);
        blocking_move(ctx, &args[1], &args[2], ends, &args[5])
    })())
}

/// `BRPOPLPUSH source destination timeout`
pub(crate) fn brpoplpush(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply(blocking_move(
        ctx,
        &args[1],
        &args[2],
        (End::Right, End::Left),
        &args[3],
    ))
}

// --- reads ------------------------------------------------------------------

pub(crate) fn llen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(
        list_ref(db, &args[1], now).map(|list| Frame::Integer(list.map_or(0, |l| l.len() as i64))),
    )
}

pub(crate) fn lrange(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (start, stop) = (int(&args[2])?, int(&args[3])?);
        let (db, now) = ctx.db();
        let Some(list) = list_ref(db, &args[1], now)? else {
            return Ok(Frame::Array(Vec::new()));
        };
        Ok(Frame::Array(match clamp_range(start, stop, list.len()) {
            Some((start, stop)) => list
                .range(start..=stop)
                .map(|element| Frame::Bulk(element.clone()))
                .collect(),
            None => Vec::new(),
        }))
    })())
}

pub(crate) fn lindex(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let index = int(&args[2])?;
        let (db, now) = ctx.db();
        Ok(list_ref(db, &args[1], now)?
            .and_then(|list| list.get(resolve_index(index, list.len())?))
            .map_or(Frame::Null, |element| Frame::Bulk(element.clone())))
    })())
}

/// `LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]`
pub(crate) fn lpos(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let element = &args[2];
        let mut rank = 1i64;
        let mut count: Option<usize> = None;
        let mut max_len = 0usize;

        let (pairs, leftover) = args[3..].as_chunks::<2>();
        if !leftover.is_empty() {
            return Err(Frame::error(ERR_SYNTAX));
        }
        for [opt, value] in pairs {
            if is(opt, "RANK") {
                rank = int(value)?;
                if rank == 0 {
                    return Err(Frame::error(
                        "ERR RANK can't be zero: use 1 to start from the first match, \
                         2 from the second ... or use negative to start from the end of the list",
                    ));
                }
            } else if is(opt, "COUNT") {
                count = Some(match int(value)? {
                    n if n >= 0 => n as usize,
                    _ => return Err(Frame::error("ERR COUNT can't be negative")),
                });
            } else if is(opt, "MAXLEN") {
                max_len = match int(value)? {
                    n if n >= 0 => n as usize,
                    _ => return Err(Frame::error("ERR MAXLEN can't be negative")),
                };
            } else {
                return Err(Frame::error(ERR_SYNTAX));
            }
        }

        let (db, now) = ctx.db();
        let Some(list) = list_ref(db, &args[1], now)? else {
            return Ok(match count {
                Some(_) => Frame::Array(Vec::new()),
                None => Frame::Null,
            });
        };

        // Walk from whichever end RANK asks for, comparing at most MAXLEN
        // elements (0 = no limit), skipping the first |RANK| - 1 matches.
        let limit = if max_len == 0 { list.len() } else { max_len };
        let positions: Box<dyn Iterator<Item = usize>> = if rank > 0 {
            Box::new(0..list.len())
        } else {
            Box::new((0..list.len()).rev())
        };
        let wanted = match count {
            Some(0) => usize::MAX,
            Some(n) => n,
            None => 1,
        };
        let matches: Vec<Frame> = positions
            .take(limit)
            .filter(|&index| list[index] == *element)
            .skip((rank.unsigned_abs() - 1) as usize)
            .take(wanted)
            .map(|index| Frame::Integer(index as i64))
            .collect();

        Ok(match count {
            Some(_) => Frame::Array(matches),
            None => matches.into_iter().next().unwrap_or(Frame::Null),
        })
    })())
}

// --- edits ------------------------------------------------------------------

pub(crate) fn lset(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let index = int(&args[2])?;
        let (db, now) = ctx.db();
        let list = list_mut(db, &args[1], now)?.ok_or_else(|| Frame::error(ERR_NO_SUCH_KEY))?;
        let index = resolve_index(index, list.len()).ok_or_else(|| Frame::error(ERR_INDEX))?;
        list[index] = copied(&args[3]);
        Ok(Frame::ok())
    })())
}

/// `LINSERT key <BEFORE | AFTER> pivot element`
pub(crate) fn linsert(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let after = if is(&args[2], "AFTER") {
            true
        } else if is(&args[2], "BEFORE") {
            false
        } else {
            return Err(Frame::error(ERR_SYNTAX));
        };
        let (db, now) = ctx.db();
        let Some(list) = list_mut(db, &args[1], now)? else {
            return Ok(Frame::Integer(0));
        };
        let Some(pivot) = list.iter().position(|element| element == &args[3]) else {
            db.unchanged(&args[1]);
            return Ok(Frame::Integer(-1));
        };
        list.insert(pivot + usize::from(after), copied(&args[4]));
        Ok(Frame::Integer(list.len() as i64))
    })())
}

/// `LREM key count element` — removes the first `count` matches from the
/// head (`count > 0`), from the tail (`count < 0`), or all of them (`0`).
pub(crate) fn lrem(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let count = int(&args[2])?;
        let element = &args[3];
        let (db, now) = ctx.db();
        let Some(list) = list_mut(db, &args[1], now)? else {
            return Ok(Frame::Integer(0));
        };
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };

        // Choose the victims by index first, then rebuild once: O(n) however
        // many are removed.
        let mut doomed = vec![false; list.len()];
        let mut removed = 0;
        let positions: Box<dyn Iterator<Item = usize>> = if count >= 0 {
            Box::new(0..list.len())
        } else {
            Box::new((0..list.len()).rev())
        };
        for index in positions {
            if removed == limit {
                break;
            }
            if list[index] == *element {
                doomed[index] = true;
                removed += 1;
            }
        }
        if removed > 0 {
            let mut index = 0;
            list.retain(|_| {
                index += 1;
                !doomed[index - 1]
            });
            db.remove_if_empty(&args[1], now);
        } else {
            db.unchanged(&args[1]);
        }
        Ok(Frame::Integer(removed as i64))
    })())
}

/// `LTRIM key start stop` — keeps only the given range.
pub(crate) fn ltrim(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (start, stop) = (int(&args[2])?, int(&args[3])?);
        let (db, now) = ctx.db();
        let Some(list) = list_mut(db, &args[1], now)? else {
            return Ok(Frame::ok());
        };
        match clamp_range(start, stop, list.len()) {
            Some((start, stop)) => {
                list.truncate(stop + 1);
                list.drain(..start);
            }
            None => list.clear(),
        }
        db.remove_if_empty(&args[1], now);
        Ok(Frame::ok())
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

    fn ints(values: &[i64]) -> Frame {
        Frame::Array(values.iter().map(|&n| int(n)).collect())
    }

    fn abcde() -> Harness {
        let mut h = Harness::new();
        h.run("RPUSH l a b c d e");
        h
    }

    #[test]
    fn push_and_range() {
        let mut h = Harness::new();
        assert_eq!(h.run("RPUSH l a b"), int(2));
        assert_eq!(h.run("LPUSH l x y"), int(4));
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["y", "x", "a", "b"]));
        assert_eq!(h.run("LLEN l"), int(4));
        assert_eq!(h.run("LLEN missing"), int(0));
        assert_eq!(h.run("TYPE l"), Frame::simple("list"));

        assert_eq!(h.run("LPUSHX missing a"), int(0));
        assert_eq!(h.run("RPUSHX missing a"), int(0));
        assert_eq!(h.run("EXISTS missing"), int(0));
        assert_eq!(h.run("RPUSHX l z"), int(5));
        assert_eq!(h.run("LPUSHX l w"), int(6));
        assert_eq!(
            h.run("LRANGE l 0 -1"),
            list(&["w", "y", "x", "a", "b", "z"])
        );
    }

    #[test]
    fn lrange_indexes() {
        let mut h = abcde();
        assert_eq!(h.run("LRANGE l 1 3"), list(&["b", "c", "d"]));
        assert_eq!(h.run("LRANGE l -2 -1"), list(&["d", "e"]));
        assert_eq!(h.run("LRANGE l -100 1"), list(&["a", "b"]));
        assert_eq!(h.run("LRANGE l 3 100"), list(&["d", "e"]));
        assert_eq!(h.run("LRANGE l 3 1"), list(&[]));
        assert_eq!(h.run("LRANGE l 10 20"), list(&[]));
        assert_eq!(h.run("LRANGE missing 0 -1"), list(&[]));
        assert_eq!(
            h.run("LRANGE l a 1"),
            err("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn pop() {
        let mut h = abcde();
        assert_eq!(h.run("LPOP l"), bulk("a"));
        assert_eq!(h.run("RPOP l"), bulk("e"));
        assert_eq!(h.run("LPOP l 2"), list(&["b", "c"]));
        assert_eq!(h.run("RPOP l 0"), list(&[]));
        assert_eq!(h.run("RPOP l 10"), list(&["d"]));
        assert_eq!(h.run("EXISTS l"), int(0), "an emptied list is deleted");

        assert_eq!(h.run("LPOP l"), Frame::Null);
        assert_eq!(h.run("LPOP l 2"), Frame::NullArray);
        assert_eq!(
            h.run("LPOP l -1"),
            err("ERR value is out of range, must be positive")
        );

        h.run("RPUSH r 1 2 3");
        assert_eq!(
            h.run("RPOP r 2"),
            list(&["3", "2"]),
            "RPOP returns tail first"
        );
    }

    #[test]
    fn lindex_and_lset() {
        let mut h = abcde();
        assert_eq!(h.run("LINDEX l 0"), bulk("a"));
        assert_eq!(h.run("LINDEX l -1"), bulk("e"));
        assert_eq!(h.run("LINDEX l 5"), Frame::Null);
        assert_eq!(h.run("LINDEX l -6"), Frame::Null);
        assert_eq!(h.run("LINDEX missing 0"), Frame::Null);

        assert_eq!(h.run("LSET l 1 B"), Frame::ok());
        assert_eq!(h.run("LSET l -1 E"), Frame::ok());
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["a", "B", "c", "d", "E"]));
        assert_eq!(h.run("LSET l 5 x"), err("ERR index out of range"));
        assert_eq!(h.run("LSET missing 0 x"), err("ERR no such key"));
    }

    #[test]
    fn linsert() {
        let mut h = abcde();
        assert_eq!(h.run("LINSERT l BEFORE c X"), int(6));
        assert_eq!(h.run("LINSERT l after c Y"), int(7));
        assert_eq!(
            h.run("LRANGE l 0 -1"),
            list(&["a", "b", "X", "c", "Y", "d", "e"])
        );
        assert_eq!(h.run("LINSERT l BEFORE nope Z"), int(-1));
        assert_eq!(h.run("LINSERT missing BEFORE a Z"), int(0));
        assert_eq!(h.run("LINSERT l BESIDE a Z"), err("ERR syntax error"));
    }

    #[test]
    fn lrem() {
        let mut h = Harness::new();
        h.run("RPUSH l a x b x c x d");
        assert_eq!(h.run("LREM l 1 x"), int(1));
        assert_eq!(
            h.run("LRANGE l 0 -1"),
            list(&["a", "b", "x", "c", "x", "d"])
        );
        assert_eq!(h.run("LREM l -1 x"), int(1));
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["a", "b", "x", "c", "d"]));
        h.run("RPUSH l x x");
        assert_eq!(h.run("LREM l 0 x"), int(3));
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["a", "b", "c", "d"]));
        assert_eq!(h.run("LREM l 5 nope"), int(0));
        assert_eq!(h.run("LREM missing 0 x"), int(0));

        h.run("RPUSH same x x");
        assert_eq!(h.run("LREM same 0 x"), int(2));
        assert_eq!(h.run("EXISTS same"), int(0));
    }

    #[test]
    fn ltrim() {
        let mut h = abcde();
        assert_eq!(h.run("LTRIM l 1 3"), Frame::ok());
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["b", "c", "d"]));
        assert_eq!(h.run("LTRIM l -2 -1"), Frame::ok());
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["c", "d"]));
        assert_eq!(h.run("LTRIM l 5 10"), Frame::ok());
        assert_eq!(
            h.run("EXISTS l"),
            int(0),
            "trimming to nothing deletes the key"
        );
        assert_eq!(h.run("LTRIM missing 0 1"), Frame::ok());
    }

    #[test]
    fn lpos() {
        let mut h = Harness::new();
        h.run("RPUSH l a b c 1 2 3 c c");
        assert_eq!(h.run("LPOS l c"), int(2));
        assert_eq!(h.run("LPOS l c RANK 2"), int(6));
        assert_eq!(h.run("LPOS l c RANK -1"), int(7));
        assert_eq!(h.run("LPOS l c RANK -2"), int(6));
        assert_eq!(h.run("LPOS l c RANK 4"), Frame::Null);
        assert_eq!(h.run("LPOS l nope"), Frame::Null);
        assert_eq!(h.run("LPOS l c COUNT 2"), ints(&[2, 6]));
        assert_eq!(h.run("LPOS l c COUNT 0"), ints(&[2, 6, 7]));
        assert_eq!(h.run("LPOS l c RANK -1 COUNT 2"), ints(&[7, 6]));
        assert_eq!(h.run("LPOS l c COUNT 0 MAXLEN 7"), ints(&[2, 6]));
        assert_eq!(h.run("LPOS l c RANK -1 MAXLEN 1"), int(7));
        assert_eq!(h.run("LPOS l nope COUNT 0"), ints(&[]));
        assert_eq!(h.run("LPOS missing c"), Frame::Null);
        assert_eq!(h.run("LPOS missing c COUNT 1"), ints(&[]));

        assert!(
            matches!(h.run("LPOS l c RANK 0"), Frame::Error(e) if e.starts_with("ERR RANK can't be zero"))
        );
        assert_eq!(
            h.run("LPOS l c COUNT -1"),
            err("ERR COUNT can't be negative")
        );
        assert_eq!(
            h.run("LPOS l c MAXLEN -1"),
            err("ERR MAXLEN can't be negative")
        );
        assert_eq!(h.run("LPOS l c RANK"), err("ERR syntax error"));
        assert_eq!(h.run("LPOS l c BOGUS 1"), err("ERR syntax error"));
    }

    #[test]
    fn lmove_and_rpoplpush() {
        let mut h = Harness::new();
        h.run("RPUSH src a b c");
        assert_eq!(h.run("LMOVE src dst LEFT RIGHT"), bulk("a"));
        assert_eq!(h.run("LMOVE src dst RIGHT LEFT"), bulk("c"));
        assert_eq!(h.run("LRANGE src 0 -1"), list(&["b"]));
        assert_eq!(h.run("LRANGE dst 0 -1"), list(&["c", "a"]));
        assert_eq!(h.run("RPOPLPUSH src dst"), bulk("b"));
        assert_eq!(h.run("EXISTS src"), int(0));
        assert_eq!(h.run("LMOVE src dst LEFT LEFT"), Frame::Null);
        assert_eq!(h.run("LMOVE dst x UP LEFT"), err("ERR syntax error"));

        // Same key: rotation, and a one-element list survives it.
        assert_eq!(h.run("LMOVE dst dst LEFT RIGHT"), bulk("b"));
        assert_eq!(h.run("LRANGE dst 0 -1"), list(&["c", "a", "b"]));
        h.run("RPUSH one x");
        assert_eq!(h.run("RPOPLPUSH one one"), bulk("x"));
        assert_eq!(h.run("LRANGE one 0 -1"), list(&["x"]));
    }

    #[test]
    fn lmove_checks_the_destination_before_popping() {
        let mut h = Harness::new();
        h.run("RPUSH src a");
        h.run("SET str v");
        assert_eq!(h.run("LMOVE src str LEFT LEFT"), err(WRONGTYPE));
        assert_eq!(h.run("LRANGE src 0 -1"), list(&["a"]));
    }

    #[test]
    fn lmpop() {
        let mut h = Harness::new();
        h.run("RPUSH b 1 2 3");
        assert_eq!(
            h.run("LMPOP 2 a b LEFT"),
            Frame::Array(vec![bulk("b"), list(&["1"])])
        );
        assert_eq!(
            h.run("LMPOP 2 a b RIGHT COUNT 10"),
            Frame::Array(vec![bulk("b"), list(&["3", "2"])])
        );
        assert_eq!(h.run("LMPOP 2 a b LEFT"), Frame::NullArray);

        assert_eq!(
            h.run("LMPOP 0 a LEFT"),
            err("ERR numkeys should be greater than 0")
        );
        assert_eq!(h.run("LMPOP 2 a LEFT"), err("ERR syntax error"));
        assert_eq!(h.run("LMPOP 1 a UP"), err("ERR syntax error"));
        assert_eq!(
            h.run("LMPOP 1 a LEFT COUNT 0"),
            err("ERR count should be greater than 0")
        );
        assert_eq!(h.run("LMPOP 1 a LEFT BOGUS 1"), err("ERR syntax error"));
    }

    #[test]
    fn wrong_type() {
        let mut h = Harness::new();
        h.run("SET s v");
        for command in [
            "LPUSH s a",
            "RPUSHX s a",
            "LPOP s",
            "LLEN s",
            "LRANGE s 0 -1",
            "LINDEX s 0",
            "LSET s 0 a",
            "LINSERT s BEFORE a b",
            "LREM s 0 a",
            "LTRIM s 0 1",
            "LPOS s a",
            "LMOVE s other LEFT LEFT",
            "LMPOP 1 s LEFT",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
    }

    #[test]
    fn collections_keep_their_ttl_when_modified() {
        let mut h = Harness::new();
        h.run("RPUSH l a");
        h.run("EXPIRE l 100");
        h.run("RPUSH l b");
        h.run("LPOP l");
        assert_eq!(h.run("TTL l"), int(100));
    }

    /// Keys, awaited type and deadline of a wait.
    type Wait<'a> = (Vec<&'a [u8]>, crate::BlockKind, Option<u64>);

    fn waiting_for(h: &Harness) -> Option<Wait<'_>> {
        h.block.as_ref().map(|request| {
            let keys = request.keys.iter().map(|key| &key[..]).collect();
            (keys, request.kind, request.deadline)
        })
    }

    #[test]
    fn blocking_pops_return_at_once_when_there_is_data() {
        let mut h = Harness::new();
        h.run("RPUSH b x y z");
        assert_eq!(h.run("BLPOP a b 0"), list(&["b", "x"]));
        assert_eq!(h.block, None);
        assert_eq!(h.run("BRPOP a b 1.5"), list(&["b", "z"]));
        assert_eq!(h.run("BLPOP b a 0"), list(&["b", "y"]));
        assert_eq!(h.run("EXISTS b"), int(0));

        h.run("RPUSH b 1 2 3");
        assert_eq!(
            h.run("BLMPOP 0 2 a b RIGHT COUNT 2"),
            Frame::Array(vec![bulk("b"), list(&["3", "2"])])
        );
        assert_eq!(h.run("BLMOVE b c LEFT RIGHT 0"), bulk("1"));
        assert_eq!(h.run("BRPOPLPUSH c d 0"), bulk("1"));
        assert_eq!(h.run("LRANGE d 0 -1"), list(&["1"]));
        assert_eq!(h.block, None);
    }

    #[test]
    fn blocking_pops_ask_to_wait_when_there_is_nothing() {
        use crate::BlockKind::List;
        let mut h = Harness::new();
        let now = h.now;

        assert_eq!(h.run("BLPOP a b 0"), Frame::NullArray);
        assert_eq!(
            waiting_for(&h),
            Some((vec![&b"a"[..], &b"b"[..]], List, None))
        );
        assert_eq!(h.run("BRPOP a 2"), Frame::NullArray);
        assert_eq!(
            waiting_for(&h),
            Some((vec![&b"a"[..]], List, Some(now + 2000)))
        );
        // Fractions of a second count, rounded up to a whole millisecond.
        h.run("BLPOP a 0.0015");
        assert_eq!(
            waiting_for(&h),
            Some((vec![&b"a"[..]], List, Some(now + 2)))
        );

        h.run("BLMPOP 0.5 2 a b LEFT");
        assert_eq!(
            waiting_for(&h),
            Some((vec![&b"a"[..], &b"b"[..]], List, Some(now + 500)))
        );
        // Only the source is waited for.
        h.run("BLMOVE a b LEFT LEFT 1");
        assert_eq!(
            waiting_for(&h),
            Some((vec![&b"a"[..]], List, Some(now + 1000)))
        );
        h.run("BRPOPLPUSH a b 0");
        assert_eq!(waiting_for(&h), Some((vec![&b"a"[..]], List, None)));

        // An ordinary command afterwards clears the request.
        h.run("LLEN a");
        assert_eq!(h.block, None);
    }

    #[test]
    fn blocking_pop_errors_do_not_wait() {
        let mut h = Harness::new();
        h.run("SET s v");
        for (command, message) in [
            ("BLPOP a x", "ERR timeout is not a float or out of range"),
            ("BLPOP a -1", "ERR timeout is negative"),
            ("BLPOP a 1e400", "ERR timeout is out of range"),
            ("BLPOP a 9223372036854775807", "ERR timeout is out of range"),
            ("BLPOP s 0", WRONGTYPE),
            ("BRPOP a s 0", WRONGTYPE),
            ("BLMOVE s a LEFT LEFT 0", WRONGTYPE),
            ("BLMOVE a b UP LEFT 0", "ERR syntax error"),
            (
                "BLMOVE a b LEFT LEFT x",
                "ERR timeout is not a float or out of range",
            ),
            ("BRPOPLPUSH s a 0", WRONGTYPE),
            (
                "BLMPOP x 1 a LEFT",
                "ERR timeout is not a float or out of range",
            ),
            // The timeout is the last thing checked.
            ("BLMPOP x 0 a LEFT", "ERR numkeys should be greater than 0"),
            ("BLMPOP x 1 a UP", "ERR syntax error"),
            ("BLMPOP 0 0 a LEFT", "ERR numkeys should be greater than 0"),
            ("BLMPOP 0 1 a UP", "ERR syntax error"),
            (
                "BLMPOP 0 1 a LEFT COUNT 0",
                "ERR count should be greater than 0",
            ),
            ("BLMPOP 0 1 s LEFT", WRONGTYPE),
            (
                "BLPOP a",
                "ERR wrong number of arguments for 'blpop' command",
            ),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
            assert_eq!(h.block, None, "{command}");
        }
    }

    #[test]
    fn a_missing_source_is_reported_before_a_bad_destination() {
        let mut h = Harness::new();
        h.run("SET s v");
        assert_eq!(h.run("LMOVE nokey s LEFT LEFT"), Frame::Null);
        assert_eq!(h.run("RPOPLPUSH nokey s"), Frame::Null);
        h.run("RPUSH l a");
        assert_eq!(h.run("LMOVE l s LEFT LEFT"), err(WRONGTYPE));
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["a"]));
    }
}
