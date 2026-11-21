//! Set commands.

use bytes::Bytes;
use rexyra_engine::{glob_match, Db, Set, SetTtl, Value};
use rexyra_resp::Frame;

use crate::notify::{self, notify, GENERIC, SET};
use crate::{
    int, is, parse_cursor, parse_scan_options, reply, sample_distinct, wrong_type, Ctx, ERR_SYNTAX,
};

/// Deletes `key` if its set has become empty, and says so.
fn remove_if_empty(ctx: &mut Ctx<'_>, key: &[u8]) {
    let (db, now) = ctx.db();
    if db.remove_if_empty(key, now) {
        notify(ctx, GENERIC, "del", key);
    }
}

fn set_ref<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a Set>, Frame> {
    match db.get(key, now) {
        None => Ok(None),
        Some(Value::Set(set)) => Ok(Some(set)),
        Some(_) => Err(wrong_type()),
    }
}

fn set_mut<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<Option<&'a mut Set>, Frame> {
    match db.get_mut(key, now) {
        None => Ok(None),
        Some(Value::Set(set)) => Ok(Some(set)),
        Some(_) => Err(wrong_type()),
    }
}

fn set_or_create<'a>(db: &'a mut Db, key: &[u8], now: u64) -> Result<&'a mut Set, Frame> {
    match db.get_or_insert_with(key, now, || Value::Set(Set::new())) {
        Value::Set(set) => Ok(set),
        _ => Err(wrong_type()),
    }
}

fn members_reply(members: impl IntoIterator<Item = Bytes>) -> Frame {
    Frame::Set(members.into_iter().map(Frame::Bulk).collect())
}

// --- membership -------------------------------------------------------------

pub(crate) fn sadd(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        let set = set_or_create(db, &args[1], now)?;
        let added = args[2..].iter().filter(|member| set.insert(member)).count();
        if added == 0 {
            db.unchanged(&args[1]);
        } else {
            notify(ctx, SET, "sadd", &args[1]);
        }
        Ok(Frame::Integer(added as i64))
    })())
}

pub(crate) fn srem(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (db, now) = ctx.db();
        let Some(set) = set_mut(db, &args[1], now)? else {
            return Ok(Frame::Integer(0));
        };
        let removed = args[2..].iter().filter(|member| set.remove(member)).count();
        if removed == 0 {
            db.unchanged(&args[1]);
            return Ok(Frame::Integer(0));
        }
        notify(ctx, SET, "srem", &args[1]);
        remove_if_empty(ctx, &args[1]);
        Ok(Frame::Integer(removed as i64))
    })())
}

pub(crate) fn scard(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(set_ref(db, &args[1], now).map(|set| Frame::Integer(set.map_or(0, |s| s.len() as i64))))
}

pub(crate) fn sismember(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(
        set_ref(db, &args[1], now)
            .map(|set| Frame::Integer(i64::from(set.is_some_and(|s| s.contains(&args[2]))))),
    )
}

pub(crate) fn smismember(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(set_ref(db, &args[1], now).map(|set| {
        Frame::Array(
            args[2..]
                .iter()
                .map(|member| Frame::Integer(i64::from(set.is_some_and(|s| s.contains(member)))))
                .collect(),
        )
    }))
}

pub(crate) fn smembers(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    reply(set_ref(db, &args[1], now).map(|set| members_reply(set.into_iter().flat_map(Set::iter))))
}

/// `SMOVE source destination member`
pub(crate) fn smove(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (source, destination, member) = (&args[1], &args[2], &args[3]);
        let (db, now) = ctx.db();
        // Both keys must hold sets (or not exist) before anything changes.
        set_ref(db, destination, now)?;
        let Some(from) = set_mut(db, source, now)? else {
            return Ok(Frame::Integer(0));
        };
        if source == destination {
            let found = from.contains(member);
            db.unchanged(source);
            return Ok(Frame::Integer(i64::from(found)));
        }
        if !from.remove(member) {
            db.unchanged(source);
            return Ok(Frame::Integer(0));
        }
        // As in Redis: the source is reported (and deleted if emptied)
        // before the destination is created.
        notify(ctx, SET, "srem", source);
        remove_if_empty(ctx, source);
        let (db, now) = ctx.db();
        if set_or_create(db, destination, now)?.insert(member) {
            notify(ctx, SET, "sadd", destination);
        }
        Ok(Frame::Integer(1))
    })())
}

// --- random -----------------------------------------------------------------

/// `SPOP key [count]`
pub(crate) fn spop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let count = match &args[2..] {
            [] => None,
            [count] => match int(count)? {
                count if count >= 0 => Some(count as usize),
                _ => return Err(Frame::error("ERR value is out of range, must be positive")),
            },
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        // Nothing popped, nothing logged.
        ctx.log_instead(Vec::new);
        let (db, now) = ctx.db();
        let Some(set) = set_mut(db, &args[1], now)? else {
            return Ok(match count {
                Some(_) => Frame::Set(Vec::new()),
                None => Frame::Null,
            });
        };
        let popped: Vec<Bytes> = (0..count.unwrap_or(1).min(set.len()))
            .filter_map(|_| {
                let member = set.random()?;
                set.remove(&member);
                Some(member)
            })
            .collect();
        if popped.is_empty() {
            db.unchanged(&args[1]);
        } else {
            notify(ctx, SET, "spop", &args[1]);
            remove_if_empty(ctx, &args[1]);
        }
        // Which members go is random, so the log records the ones that went.
        ctx.log_instead(|| {
            if popped.is_empty() {
                return Vec::new();
            }
            let mut removal = vec![Bytes::from_static(b"SREM"), args[1].clone()];
            removal.extend(popped.iter().cloned());
            vec![removal]
        });
        Ok(match count {
            Some(_) => members_reply(popped),
            None => popped.into_iter().next().map_or(Frame::Null, Frame::Bulk),
        })
    })())
}

/// `SRANDMEMBER key [count]` — a positive count returns distinct members; a
/// negative count returns exactly that many, possibly repeating.
pub(crate) fn srandmember(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let count = match &args[2..] {
            [] => None,
            [count] => Some(int(count)?),
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        if count.is_some_and(|count| count < -(i64::MAX / 2)) {
            return Err(Frame::error("ERR value is out of range"));
        }
        let (db, now) = ctx.db();
        let set = set_ref(db, &args[1], now)?;
        let Some(count) = count else {
            return Ok(set.and_then(Set::random).map_or(Frame::Null, Frame::Bulk));
        };
        let Some(set) = set else {
            return Ok(Frame::Array(Vec::new()));
        };
        let picked: Vec<Bytes> = if count >= 0 {
            sample_distinct(set.iter().collect(), count as usize)
        } else {
            (0..count.unsigned_abs())
                .filter_map(|_| set.random())
                .collect()
        };
        Ok(Frame::Array(picked.into_iter().map(Frame::Bulk).collect()))
    })())
}

/// `SSCAN key cursor [MATCH pattern] [COUNT count]`
pub(crate) fn sscan(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let mut cursor = parse_cursor(&args[2])?;
        let options = parse_scan_options(&args[3..])?;
        let (db, now) = ctx.db();
        let mut found = Vec::new();
        if let Some(set) = set_ref(db, &args[1], now)? {
            let mut budget = options.count.saturating_mul(10);
            loop {
                cursor = set.scan(cursor, |member| {
                    if options.pattern.is_none_or(|p| glob_match(p, &member)) {
                        found.push(Frame::Bulk(member));
                    }
                });
                budget -= 1;
                if cursor == 0 || budget == 0 || found.len() >= options.count {
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

// --- algebra ----------------------------------------------------------------

#[derive(Clone, Copy)]
enum Algebra {
    Inter,
    Union,
    Diff,
}

/// Computes the intersection, union or difference of the sets at `keys`.
/// Missing keys are empty sets. `limit` stops an intersection early (0 = no
/// limit), for `SINTERCARD`.
///
/// The result order follows Redis: an intersection walks the smallest input;
/// a union or difference is the order in which members were first seen.
fn combine(
    ctx: &mut Ctx<'_>,
    keys: &[Bytes],
    op: Algebra,
    limit: usize,
) -> Result<Vec<Bytes>, Frame> {
    let (db, now) = ctx.db();
    // First pass: expire and type-check every key. After it, the sets can be
    // borrowed together because `peek` does not need exclusive access.
    for key in keys {
        set_ref(db, key, now)?;
    }
    let empty = Set::new();
    let sets: Vec<&Set> = keys
        .iter()
        .map(|key| match db.peek(key, now) {
            Some(Value::Set(set)) => set,
            _ => &empty,
        })
        .collect();

    Ok(match op {
        Algebra::Inter => {
            let mut by_size = sets.clone();
            by_size.sort_by_key(|set| set.len());
            let (smallest, others) = by_size.split_first().expect("arity guarantees one key");
            let matching = smallest
                .iter()
                .filter(|member| others.iter().all(|set| set.contains(member)));
            if limit == 0 {
                matching.collect()
            } else {
                matching.take(limit).collect()
            }
        }
        Algebra::Union => {
            let mut seen = Set::new();
            sets.iter()
                .flat_map(|set| set.iter())
                .filter(|member| seen.insert(member))
                .collect()
        }
        Algebra::Diff => {
            let (first, others) = sets.split_first().expect("arity guarantees one key");
            first
                .iter()
                .filter(|member| !others.iter().any(|set| set.contains(member)))
                .collect()
        }
    })
}

fn algebra(ctx: &mut Ctx<'_>, args: &[Bytes], op: Algebra) -> Frame {
    reply(combine(ctx, &args[1..], op, 0).map(members_reply))
}

/// Stores the result at `args[1]`, replacing whatever was there, and replies
/// with its size. An empty result deletes the destination.
fn algebra_store(ctx: &mut Ctx<'_>, args: &[Bytes], op: Algebra) -> Frame {
    reply((|| {
        // The sources are read as `SINTER` reads them: a missing one is a
        // `keymiss`.
        notify::reading(ctx, true);
        let members = combine(ctx, &args[2..], op, 0);
        notify::reading(ctx, false);
        let members = members?;
        let (db, now) = ctx.db();
        if members.is_empty() {
            if db.remove(&args[1], now) {
                notify(ctx, GENERIC, "del", &args[1]);
            }
        } else {
            let set: Set = members.iter().map(|member| &member[..]).collect();
            db.set(&args[1], Value::Set(set), SetTtl::Clear, now);
            let event = match op {
                Algebra::Inter => "sinterstore",
                Algebra::Union => "sunionstore",
                Algebra::Diff => "sdiffstore",
            };
            notify(ctx, SET, event, &args[1]);
        }
        Ok(Frame::Integer(members.len() as i64))
    })())
}

pub(crate) fn sinter(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, Algebra::Inter)
}

pub(crate) fn sunion(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, Algebra::Union)
}

pub(crate) fn sdiff(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra(ctx, args, Algebra::Diff)
}

pub(crate) fn sinterstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, Algebra::Inter)
}

pub(crate) fn sunionstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, Algebra::Union)
}

pub(crate) fn sdiffstore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    algebra_store(ctx, args, Algebra::Diff)
}

/// `SINTERCARD numkeys key [key ...] [LIMIT limit]`
pub(crate) fn sintercard(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let numkeys = match int(&args[1]) {
            Ok(n) if n > 0 => n as usize,
            _ => return Err(Frame::error("ERR numkeys should be greater than 0")),
        };
        let rest = &args[2..];
        if numkeys > rest.len() {
            return Err(Frame::error(
                "ERR Number of keys can't be greater than number of args",
            ));
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
        let members = combine(ctx, keys, Algebra::Inter, limit)?;
        Ok(Frame::Integer(members.len() as i64))
    })())
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness, WRONGTYPE};
    use rexyra_resp::Frame;
    use std::collections::HashSet;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn set(items: &[&str]) -> Frame {
        Frame::Set(bulks(items))
    }

    fn sorted(frame: Frame) -> Vec<String> {
        let (Frame::Set(items) | Frame::Array(items)) = frame else {
            panic!("expected a collection, got {frame:?}");
        };
        let mut out: Vec<String> = items
            .into_iter()
            .map(|item| match item {
                Frame::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                other => panic!("expected bulk, got {other:?}"),
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn sadd_srem_and_membership() {
        let mut h = Harness::new();
        assert_eq!(h.run("SADD s a b c"), int(3));
        assert_eq!(h.run("SADD s a d"), int(1), "only new members count");
        assert_eq!(h.run("SCARD s"), int(4));
        assert_eq!(h.run("SISMEMBER s a"), int(1));
        assert_eq!(h.run("SISMEMBER s z"), int(0));
        assert_eq!(h.run("SISMEMBER missing a"), int(0));
        assert_eq!(
            h.run("SMISMEMBER s a z d"),
            Frame::Array(vec![int(1), int(0), int(1)])
        );
        assert_eq!(h.run("TYPE s"), Frame::simple("set"));

        assert_eq!(h.run("SREM s a z"), int(1));
        assert_eq!(h.run("SREM missing a"), int(0));
        assert_eq!(h.run("SREM s b c d"), int(3));
        assert_eq!(h.run("EXISTS s"), int(0), "an emptied set is deleted");
    }

    #[test]
    fn smembers_order_follows_the_encoding() {
        let mut h = Harness::new();
        h.run("SADD ints 10 2 33 -5");
        assert_eq!(h.run("SMEMBERS ints"), set(&["-5", "2", "10", "33"]));
        assert_eq!(h.run("OBJECT ENCODING ints"), bulk("intset"));

        h.run("SADD strs c a b");
        assert_eq!(h.run("SMEMBERS strs"), set(&["c", "a", "b"]));
        assert_eq!(h.run("OBJECT ENCODING strs"), bulk("listpack"));

        h.run("SADD ints x");
        assert_eq!(h.run("SMEMBERS ints"), set(&["-5", "2", "10", "33", "x"]));
        assert_eq!(h.run("SMEMBERS missing"), set(&[]));

        for i in 0..200 {
            h.cmd(&["SADD", "big", &format!("m{i}")]);
        }
        assert_eq!(h.run("OBJECT ENCODING big"), bulk("hashtable"));
        assert_eq!(h.run("SCARD big"), int(200));
    }

    #[test]
    fn smove() {
        let mut h = Harness::new();
        h.run("SADD a x y");
        h.run("SADD b z");
        assert_eq!(h.run("SMOVE a b x"), int(1));
        assert_eq!(h.run("SMOVE a b nope"), int(0));
        assert_eq!(h.run("SMOVE missing b x"), int(0));
        assert_eq!(h.run("SMEMBERS a"), set(&["y"]));
        assert_eq!(h.run("SMEMBERS b"), set(&["z", "x"]));
        assert_eq!(h.run("SMOVE a b y"), int(1));
        assert_eq!(h.run("EXISTS a"), int(0));
        assert_eq!(h.run("SMOVE b b z"), int(1));
        assert_eq!(h.run("SMOVE b b nope"), int(0));
        assert_eq!(h.run("SCARD b"), int(3));

        h.run("SET str v");
        assert_eq!(h.run("SMOVE b str z"), err(WRONGTYPE));
        assert_eq!(h.run("SISMEMBER b z"), int(1), "nothing moved on error");
    }

    #[test]
    fn spop() {
        let mut h = Harness::new();
        assert_eq!(h.run("SPOP missing"), Frame::Null);
        assert_eq!(h.run("SPOP missing 2"), set(&[]));
        h.run("SADD s a b c d");

        let Frame::Bulk(one) = h.run("SPOP s") else {
            panic!()
        };
        assert_eq!(h.run("SCARD s"), int(3));
        assert_eq!(
            h.cmd(&["SISMEMBER", "s", std::str::from_utf8(&one).unwrap()]),
            int(0)
        );

        assert_eq!(h.run("SPOP s 0"), set(&[]));
        let two = sorted(h.run("SPOP s 2"));
        assert_eq!(two.iter().collect::<HashSet<_>>().len(), 2);
        assert_eq!(sorted(h.run("SPOP s 10")).len(), 1);
        assert_eq!(h.run("EXISTS s"), int(0));
        assert_eq!(
            h.run("SPOP s -1"),
            err("ERR value is out of range, must be positive")
        );
    }

    #[test]
    fn srandmember() {
        let mut h = Harness::new();
        assert_eq!(h.run("SRANDMEMBER missing"), Frame::Null);
        assert_eq!(h.run("SRANDMEMBER missing 3"), Frame::Array(vec![]));
        h.run("SADD s a b c");

        assert!(bulks(&["a", "b", "c"]).contains(&h.run("SRANDMEMBER s")));
        let two = sorted(h.run("SRANDMEMBER s 2"));
        assert_eq!(two.iter().collect::<HashSet<_>>().len(), 2);
        assert_eq!(sorted(h.run("SRANDMEMBER s 10")), ["a", "b", "c"]);
        assert_eq!(sorted(h.run("SRANDMEMBER s -10")).len(), 10);
        assert_eq!(h.run("SRANDMEMBER s 0"), Frame::Array(vec![]));
        assert_eq!(h.run("SCARD s"), int(3), "nothing is removed");
    }

    #[test]
    fn sscan() {
        let mut h = Harness::new();
        h.run("SADD s a1 b1 a2");
        assert_eq!(
            h.run("SSCAN s 0"),
            Frame::Array(vec![bulk("0"), Frame::Array(bulks(&["a1", "b1", "a2"]))])
        );
        assert_eq!(
            h.run("SSCAN s 0 MATCH a*"),
            Frame::Array(vec![bulk("0"), Frame::Array(bulks(&["a1", "a2"]))])
        );
        assert_eq!(
            h.run("SSCAN missing 0"),
            Frame::Array(vec![bulk("0"), Frame::Array(vec![])])
        );

        for i in 0..300 {
            h.cmd(&["SADD", "big", &format!("m{i}")]);
        }
        let mut cursor = "0".to_owned();
        let mut seen = HashSet::new();
        loop {
            let Frame::Array(reply) = h.run(&format!("SSCAN big {cursor}")) else {
                panic!()
            };
            let Frame::Bulk(next) = &reply[0] else {
                panic!()
            };
            let Frame::Array(items) = &reply[1] else {
                panic!()
            };
            seen.extend(items.iter().map(|item| format!("{item:?}")));
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), 300);
    }

    fn abc() -> Harness {
        let mut h = Harness::new();
        h.run("SADD a 1 2 3 4");
        h.run("SADD b 3 4 5");
        h.run("SADD c 4 6");
        h
    }

    #[test]
    fn inter_union_diff() {
        let mut h = abc();
        assert_eq!(h.run("SINTER a b"), set(&["3", "4"]));
        assert_eq!(h.run("SINTER a b c"), set(&["4"]));
        assert_eq!(h.run("SINTER a missing"), set(&[]));
        assert_eq!(h.run("SINTER a"), set(&["1", "2", "3", "4"]));

        assert_eq!(h.run("SUNION a b c"), set(&["1", "2", "3", "4", "5", "6"]));
        assert_eq!(h.run("SUNION missing a"), set(&["1", "2", "3", "4"]));
        assert_eq!(h.run("SUNION missing"), set(&[]));

        assert_eq!(h.run("SDIFF a b"), set(&["1", "2"]));
        assert_eq!(h.run("SDIFF a b c"), set(&["1", "2"]));
        assert_eq!(h.run("SDIFF a missing"), set(&["1", "2", "3", "4"]));
        assert_eq!(h.run("SDIFF missing a"), set(&[]));
        assert_eq!(h.run("SDIFF a a"), set(&[]));
    }

    #[test]
    fn store_variants() {
        let mut h = abc();
        assert_eq!(h.run("SINTERSTORE d a b"), int(2));
        assert_eq!(h.run("SMEMBERS d"), set(&["3", "4"]));
        assert_eq!(h.run("SUNIONSTORE d a c"), int(5));
        assert_eq!(h.run("SMEMBERS d"), set(&["1", "2", "3", "4", "6"]));
        assert_eq!(h.run("SDIFFSTORE d a b"), int(2));
        assert_eq!(h.run("SMEMBERS d"), set(&["1", "2"]));

        // The destination may be one of the sources, and is replaced
        // whatever its type.
        assert_eq!(h.run("SINTERSTORE a a b"), int(2));
        assert_eq!(h.run("SMEMBERS a"), set(&["3", "4"]));
        h.run("SET str v");
        assert_eq!(h.run("SUNIONSTORE str b c"), int(4));
        assert_eq!(h.run("TYPE str"), Frame::simple("set"));

        h.run("EXPIRE d 100");
        assert_eq!(h.run("SINTERSTORE d a missing"), int(0));
        assert_eq!(
            h.run("EXISTS d"),
            int(0),
            "an empty result deletes the destination"
        );
    }

    #[test]
    fn sintercard() {
        let mut h = abc();
        assert_eq!(h.run("SINTERCARD 2 a b"), int(2));
        assert_eq!(h.run("SINTERCARD 2 a b LIMIT 1"), int(1));
        assert_eq!(h.run("SINTERCARD 2 a b LIMIT 0"), int(2));
        assert_eq!(h.run("SINTERCARD 2 a missing"), int(0));
        assert_eq!(
            h.run("SINTERCARD 0 a"),
            err("ERR numkeys should be greater than 0")
        );
        assert_eq!(
            h.run("SINTERCARD 3 a b"),
            err("ERR Number of keys can't be greater than number of args")
        );
        assert_eq!(
            h.run("SINTERCARD 2 a b LIMIT -1"),
            err("ERR LIMIT can't be negative")
        );
        assert_eq!(h.run("SINTERCARD 1 a b"), err("ERR syntax error"));
    }

    #[test]
    fn wrong_type() {
        let mut h = Harness::new();
        h.run("SET str v");
        h.run("SADD s a");
        for command in [
            "SADD str a",
            "SREM str a",
            "SCARD str",
            "SISMEMBER str a",
            "SMISMEMBER str a",
            "SMEMBERS str",
            "SPOP str",
            "SRANDMEMBER str",
            "SSCAN str 0",
            "SINTER s str",
            "SUNION str s",
            "SDIFF s str",
            "SINTERSTORE d s str",
            "SINTERCARD 2 s str",
            "SMOVE str s a",
        ] {
            assert_eq!(h.run(command), err(WRONGTYPE), "{command}");
        }
        assert_eq!(h.run("EXISTS d"), int(0));
    }
}
