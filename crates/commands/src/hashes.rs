//! Hash commands.

use bytes::Bytes;
use rexyra_engine::{glob_match, Db, Hash, Value};
use rexyra_resp::{parse_i64, Frame, Protocol};

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
    f: impl FnOnce(&Hash) -> Frame,
) -> Frame {
    let (db, now) = ctx.db();
    match hash_ref(db, &args[1], now) {
        Ok(Some(hash)) => f(hash),
        Ok(None) => missing,
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
    Ok(pairs
        .iter()
        .filter(|[field, value]| hash.insert(field, copied(value)))
        .count())
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
        let hash = hash_or_create(db, &args[1], now)?;
        if hash.contains(&args[2]) {
            db.unchanged(&args[1]);
            return Ok(Frame::Integer(0));
        }
        hash.insert(&args[2], copied(&args[3]));
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
        }
        db.remove_if_empty(&args[1], now);
        Ok(Frame::Integer(removed as i64))
    })())
}

/// The current value of `field`, read without creating the hash, so that a
/// failed increment leaves nothing behind.
fn current_field(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Option<Bytes>, Frame> {
    let (db, now) = ctx.db();
    Ok(hash_ref(db, &args[1], now)?.and_then(|hash| hash.get(&args[2]).cloned()))
}

fn store_field(ctx: &mut Ctx<'_>, args: &[Bytes], value: Bytes) -> Result<(), Frame> {
    let (db, now) = ctx.db();
    hash_or_create(db, &args[1], now)?.insert(&args[2], value);
    Ok(())
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
        store_field(ctx, args, text.clone())?;
        // Logged as its result: another server might round differently.
        ctx.log_instead(|| vec![crate::aof::command(&[b"HSET", &args[1], &args[2], &text])]);
        Ok(Frame::Bulk(text))
    })())
}

// --- reads ------------------------------------------------------------------

pub(crate) fn hget(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Null, |hash| {
        hash.get(&args[2]).cloned().map_or(Frame::Null, Frame::Bulk)
    })
}

pub(crate) fn hmget(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let fields = &args[2..];
    let all_missing = Frame::Array(vec![Frame::Null; fields.len()]);
    with_hash(ctx, args, all_missing, |hash| {
        Frame::Array(
            fields
                .iter()
                .map(|field| hash.get(field).cloned().map_or(Frame::Null, Frame::Bulk))
                .collect(),
        )
    })
}

pub(crate) fn hgetall(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Map(Vec::new()), |hash| {
        Frame::Map(
            hash.iter()
                .map(|(field, value)| (Frame::Bulk(field.clone()), Frame::Bulk(value.clone())))
                .collect(),
        )
    })
}

pub(crate) fn hkeys(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Array(Vec::new()), |hash| {
        Frame::Array(
            hash.iter()
                .map(|(field, _)| Frame::Bulk(field.clone()))
                .collect(),
        )
    })
}

pub(crate) fn hvals(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Array(Vec::new()), |hash| {
        Frame::Array(
            hash.iter()
                .map(|(_, value)| Frame::Bulk(value.clone()))
                .collect(),
        )
    })
}

pub(crate) fn hlen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Integer(0), |hash| {
        Frame::Integer(hash.len() as i64)
    })
}

pub(crate) fn hexists(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Integer(0), |hash| {
        Frame::Integer(i64::from(hash.contains(&args[2])))
    })
}

pub(crate) fn hstrlen(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    with_hash(ctx, args, Frame::Integer(0), |hash| {
        Frame::Integer(hash.get(&args[2]).map_or(0, |value| value.len() as i64))
    })
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
                cursor = hash.scan(cursor, |field, value| {
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
/// negative count returns exactly that many, possibly repeating.
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
        let hash = hash_ref(db, &args[1], now)?;

        let Some(count) = count else {
            return Ok(hash
                .and_then(Hash::random)
                .map_or(Frame::Null, |(field, _)| Frame::Bulk(field.clone())));
        };
        let Some(hash) = hash else {
            return Ok(Frame::Array(Vec::new()));
        };
        let picked: Vec<(&Bytes, &Bytes)> = if count >= 0 {
            sample_distinct(hash.iter().collect(), count as usize)
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

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness, WRONGTYPE};
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
}
