//! `SORT` and `SORT_RO`.
//!
//! ```text
//! SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]]
//!          [ASC | DESC] [ALPHA] [STORE destination]
//! ```
//!
//! Sorts the elements of a list, set or sorted set. By default elements are
//! compared as numbers; `ALPHA` compares them as strings. `BY` sorts by the
//! value of another key derived from each element, and `GET` returns such
//! values instead of the elements themselves.

use std::cmp::Ordering;
use std::collections::VecDeque;

use bytes::Bytes;
use rexyra_engine::{Db, SetTtl, Value};
use rexyra_resp::Frame;

use crate::notify::{self, notify, GENERIC, LIST};
use crate::{int, is, reply, wrong_type, Ctx, ERR_SYNTAX};

const ERR_NOT_DOUBLE: &str = "ERR One or more scores can't be converted into double";

struct Options<'a> {
    by: Option<&'a [u8]>,
    /// Some `BY` pattern named no key (conventionally `BY nosort`), which
    /// turns sorting off, even if another `BY` follows.
    skip_sort: bool,
    /// Offset and count; a negative count means "to the end".
    limit: (i64, i64),
    gets: Vec<&'a [u8]>,
    descending: bool,
    alpha: bool,
    store: Option<&'a Bytes>,
}

fn parse(mut rest: &[Bytes], read_only: bool) -> Result<Options<'_>, Frame> {
    let mut options = Options {
        by: None,
        skip_sort: false,
        limit: (0, -1),
        gets: Vec::new(),
        descending: false,
        alpha: false,
        store: None,
    };
    loop {
        rest = match rest {
            [] => return Ok(options),
            [opt, tail @ ..] if is(opt, "ASC") => {
                options.descending = false;
                tail
            }
            [opt, tail @ ..] if is(opt, "DESC") => {
                options.descending = true;
                tail
            }
            [opt, tail @ ..] if is(opt, "ALPHA") => {
                options.alpha = true;
                tail
            }
            [opt, offset, count, tail @ ..] if is(opt, "LIMIT") => {
                options.limit = (int(offset)?, int(count)?);
                tail
            }
            [opt, destination, tail @ ..] if is(opt, "STORE") && !read_only => {
                options.store = Some(destination);
                tail
            }
            [opt, pattern, tail @ ..] if is(opt, "BY") => {
                options.by = Some(pattern);
                options.skip_sort |= !pattern.contains(&b'*');
                tail
            }
            [opt, pattern, tail @ ..] if is(opt, "GET") => {
                options.gets.push(pattern);
                tail
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
    }
}

/// Resolves a `BY` or `GET` pattern for one element.
///
/// The first `*` in the pattern is replaced by the element to form a key
/// name. A pattern of the form `key*->field` reads a hash field; otherwise
/// the key must hold a string. `#` stands for the element itself. `None` if
/// the pattern has no `*`, or the key or field is missing or of another type.
fn lookup(db: &mut Db, pattern: &[u8], element: &Bytes, now: u64) -> Option<Bytes> {
    if pattern == b"#" {
        return Some(element.clone());
    }
    let star = pattern.iter().position(|&byte| byte == b'*')?;
    let (prefix, after) = (&pattern[..star], &pattern[star + 1..]);
    // `->` introduces a field name, unless nothing follows it.
    let (postfix, field) = match after.windows(2).position(|pair| pair == b"->") {
        Some(arrow) if arrow + 2 < after.len() => (&after[..arrow], Some(&after[arrow + 2..])),
        _ => (after, None),
    };
    let key = [prefix, &element[..], postfix].concat();
    // A real lookup, as in Redis: an expired key goes, a missing one is a
    // `keymiss`.
    match (db.get(&key, now)?, field) {
        (Value::Hash(hash), Some(field)) => hash.get(field, now).cloned(),
        (Value::String(value), None) => Some(value.clone()),
        _ => None,
    }
}

/// Parses a sort weight the way C's `strtod` does for the cases that matter:
/// leading whitespace is skipped and an empty string counts as zero.
fn weight(text: &[u8]) -> Option<f64> {
    let start = text
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(text.len());
    let text = &text[start..];
    if text.is_empty() {
        return Some(0.0);
    }
    std::str::from_utf8(text)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|value| !value.is_nan())
}

/// What an element is ordered by.
enum SortKey {
    Number(f64),
    /// `None` when the `BY` key does not exist; such elements sort first.
    Text(Option<Bytes>),
}

fn compare(a: &(SortKey, Bytes), b: &(SortKey, Bytes)) -> Ordering {
    match (&a.0, &b.0) {
        // Equal weights fall back to the elements, so the order is defined.
        (SortKey::Number(x), SortKey::Number(y)) => x
            .partial_cmp(y)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1)),
        (SortKey::Text(x), SortKey::Text(y)) => x.cmp(y),
        // Every key of one sort has the same variant.
        _ => Ordering::Equal,
    }
}

/// The elements to return, in order and with `LIMIT` applied.
fn sorted_elements(
    db: &mut Db,
    key: &[u8],
    now: u64,
    options: &Options<'_>,
) -> Result<Vec<Bytes>, Frame> {
    let mut skip_sort = options.skip_sort;
    // Unsorted lists and sorted sets keep their own order, which `DESC`
    // reverses.
    let reverse = skip_sort && options.descending;
    let mut by = options.by;
    let mut alpha = options.alpha;

    let mut elements: Vec<Bytes> = match db.get(key, now) {
        None => Vec::new(),
        Some(Value::List(list)) if reverse => list.iter().rev().cloned().collect(),
        Some(Value::List(list)) => list.iter().cloned().collect(),
        Some(Value::Set(set)) => {
            // A set has no order of its own, so a stored result is sorted
            // anyway to make it the same every time.
            if skip_sort && options.store.is_some() {
                (skip_sort, by, alpha) = (false, None, true);
            }
            set.iter().collect()
        }
        Some(Value::ZSet(zset)) => zset
            .range(0..zset.len(), reverse)
            .map(|(member, _)| member.clone())
            .collect(),
        Some(_) => return Err(wrong_type()),
    };

    if !skip_sort {
        let mut keyed = Vec::with_capacity(elements.len());
        let mut bad_number = false;
        for element in elements {
            let sort_key = if alpha {
                SortKey::Text(match by {
                    Some(pattern) => lookup(db, pattern, &element, now),
                    None => Some(element.clone()),
                })
            } else {
                let parsed = match by {
                    // A missing `BY` key weighs zero.
                    Some(pattern) => {
                        lookup(db, pattern, &element, now).map_or(Some(0.0), |value| weight(&value))
                    }
                    None => weight(&element),
                };
                bad_number |= parsed.is_none();
                SortKey::Number(parsed.unwrap_or(0.0))
            };
            keyed.push((sort_key, element));
        }
        if bad_number {
            return Err(Frame::error(ERR_NOT_DOUBLE));
        }
        if options.descending {
            keyed.sort_by(|a, b| compare(b, a));
        } else {
            keyed.sort_by(compare);
        }
        elements = keyed.into_iter().map(|(_, element)| element).collect();
    }

    let (offset, count) = options.limit;
    let start = (offset.max(0) as usize).min(elements.len());
    let end = if count < 0 {
        elements.len()
    } else {
        start.saturating_add(count as usize).min(elements.len())
    };
    elements.truncate(end);
    elements.drain(..start);
    Ok(elements)
}

fn run(ctx: &mut Ctx<'_>, args: &[Bytes], read_only: bool) -> Frame {
    reply((|| {
        let options = parse(&args[2..], read_only)?;
        let logging = ctx.server.aof.is_some();
        // Everything but the destination is read as `GET` reads: missing
        // keys are `keymiss` events.
        notify::reading(ctx, true);
        let (db, now) = ctx.db();
        let output = sorted_elements(db, &args[1], now, &options).map(|elements| {
            // One value per element, or one per element per `GET`.
            if options.gets.is_empty() {
                elements.into_iter().map(Some).collect::<Vec<_>>()
            } else {
                elements
                    .iter()
                    .flat_map(|element| {
                        options
                            .gets
                            .iter()
                            .map(|pattern| lookup(db, pattern, element, now))
                            .collect::<Vec<_>>()
                    })
                    .collect()
            }
        });
        notify::reading(ctx, false);
        let output = output?;

        let Some(destination) = options.store else {
            return Ok(Frame::Array(
                output
                    .into_iter()
                    .map(|value| value.map_or(Frame::Null, Frame::Bulk))
                    .collect(),
            ));
        };
        let stored = output.len() as i64;
        // A list cannot hold null, so a missing value is stored empty.
        let list: VecDeque<Bytes> = output.into_iter().map(Option::unwrap_or_default).collect();
        // With `BY` or `GET` the result depends on other keys and hash
        // fields, some of which may have been past their TTL but not yet
        // reclaimed; on replay nothing expires, so the result is logged.
        let logged = (logging && (options.by.is_some() || !options.gets.is_empty())).then(|| {
            let mut commands = vec![crate::aof::command(&[b"DEL", destination])];
            if !list.is_empty() {
                let mut push: Vec<&[u8]> = vec![b"RPUSH", destination];
                push.extend(list.iter().map(|value| &value[..]));
                commands.push(crate::aof::command(&push));
            }
            commands
        });
        let (db, now) = ctx.db();
        if list.is_empty() {
            if db.remove(destination, now) {
                notify(ctx, GENERIC, "del", destination);
            }
        } else {
            db.set(destination, Value::List(list), SetTtl::Clear, now);
            notify(ctx, LIST, "sortstore", destination);
        }
        if let Some(commands) = logged {
            ctx.log_instead(|| commands);
        }
        Ok(Frame::Integer(stored))
    })())
}

pub(crate) fn sort(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    run(ctx, args, false)
}

/// `SORT_RO`: the same without `STORE`.
pub(crate) fn sort_ro(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    run(ctx, args, true)
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness, WRONGTYPE};
    use rexyra_resp::Frame;

    fn list(items: &[&str]) -> Frame {
        Frame::Array(bulks(items))
    }

    #[test]
    fn sorts_numbers_by_default() {
        let mut h = Harness::new();
        h.run("RPUSH l 10 2 33 -1 2.5 2");
        assert_eq!(h.run("SORT l"), list(&["-1", "2", "2", "2.5", "10", "33"]));
        assert_eq!(
            h.run("SORT l DESC"),
            list(&["33", "10", "2.5", "2", "2", "-1"])
        );
        assert_eq!(h.run("SORT_RO l desc asc"), h.run("SORT l"));
        // The source is untouched.
        assert_eq!(
            h.run("LRANGE l 0 -1"),
            list(&["10", "2", "33", "-1", "2.5", "2"])
        );
        assert_eq!(h.run("SORT nokey"), list(&[]));
    }

    #[test]
    fn alpha_and_number_errors() {
        let mut h = Harness::new();
        h.run("RPUSH l pear apple 10 Banana 9");
        assert_eq!(
            h.run("SORT l"),
            err("ERR One or more scores can't be converted into double")
        );
        assert_eq!(
            h.run("SORT l ALPHA"),
            list(&["10", "9", "Banana", "apple", "pear"])
        );
        assert_eq!(
            h.run("SORT l ALPHA DESC LIMIT 1 2"),
            list(&["apple", "Banana"])
        );
    }

    #[test]
    fn limit() {
        let mut h = Harness::new();
        h.run("RPUSH l 5 4 3 2 1");
        assert_eq!(h.run("SORT l LIMIT 1 2"), list(&["2", "3"]));
        assert_eq!(h.run("SORT l LIMIT 3 -1"), list(&["4", "5"]));
        assert_eq!(h.run("SORT l LIMIT -5 2"), list(&["1", "2"]));
        assert_eq!(h.run("SORT l LIMIT 0 0"), list(&[]));
        assert_eq!(h.run("SORT l LIMIT 9 2"), list(&[]));
        assert_eq!(h.run("SORT l LIMIT 4 100"), list(&["5"]));
    }

    #[test]
    fn sets_and_sorted_sets() {
        let mut h = Harness::new();
        h.run("SADD s 3 1 2");
        h.run("ZADD z 1 c 2 a 3 b");
        assert_eq!(h.run("SORT s"), list(&["1", "2", "3"]));
        assert_eq!(h.run("SORT z ALPHA"), list(&["a", "b", "c"]));
        // Without sorting, a sorted set keeps its score order.
        assert_eq!(h.run("SORT z BY nosort"), list(&["c", "a", "b"]));
        assert_eq!(h.run("SORT z BY nosort DESC"), list(&["b", "a", "c"]));
        assert_eq!(h.run("SORT z BY nosort DESC LIMIT 1 1"), list(&["a"]));
        // A stored set is sorted even when asked not to.
        h.run("SADD words pear apple fig");
        assert_eq!(h.run("SORT words BY nosort STORE out"), Frame::Integer(3));
        assert_eq!(h.run("LRANGE out 0 -1"), list(&["apple", "fig", "pear"]));
    }

    #[test]
    fn by_and_get_patterns() {
        let mut h = Harness::new();
        h.run("RPUSH ids 1 2 3 4");
        h.run("MSET w_1 30 w_2 10 w_3 20 name_1 one name_2 two name_3 three");
        h.run("HSET obj_1 rank 2 label first");
        h.run("HSET obj_2 rank 3");
        h.run("HSET obj_3 rank 1 label third");

        // `w_4` is missing and weighs zero.
        assert_eq!(h.run("SORT ids BY w_*"), list(&["4", "2", "3", "1"]));
        assert_eq!(h.run("SORT ids BY w_* DESC"), list(&["1", "3", "2", "4"]));
        assert_eq!(
            h.run("SORT ids BY obj_*->rank"),
            list(&["4", "3", "1", "2"])
        );
        // Missing text keys sort first.
        assert_eq!(
            h.run("SORT ids BY name_* ALPHA"),
            list(&["4", "1", "3", "2"])
        );
        assert_eq!(h.run("SORT ids BY nosort"), list(&["1", "2", "3", "4"]));
        assert_eq!(
            h.run("SORT ids BY nosort DESC LIMIT 0 3"),
            list(&["4", "3", "2"])
        );
        // Once sorting is off, a later pattern does not turn it back on.
        assert_eq!(
            h.run("SORT ids BY nosort BY w_*"),
            list(&["1", "2", "3", "4"])
        );

        assert_eq!(
            h.run("SORT ids GET name_*"),
            Frame::Array(vec![bulk("one"), bulk("two"), bulk("three"), Frame::Null])
        );
        assert_eq!(
            h.run("SORT ids LIMIT 0 2 GET # GET obj_*->label GET w_*"),
            Frame::Array(vec![
                bulk("1"),
                bulk("first"),
                bulk("30"),
                bulk("2"),
                Frame::Null,
                bulk("10"),
            ])
        );
        // No `*`, a key of the wrong type, and an empty field name.
        h.run("SET obj_1-> plain");
        assert_eq!(
            h.run("SORT ids LIMIT 0 1 GET fixed GET obj_* GET obj_*->"),
            Frame::Array(vec![Frame::Null, Frame::Null, bulk("plain")])
        );
        h.run("SET w_2 x");
        assert_eq!(
            h.run("SORT ids BY w_*"),
            err("ERR One or more scores can't be converted into double")
        );
    }

    #[test]
    fn store() {
        let mut h = Harness::new();
        h.run("RPUSH l 3 1 2");
        h.run("SET dst old");
        h.run("EXPIRE dst 100");
        assert_eq!(h.run("SORT l STORE dst"), Frame::Integer(3));
        assert_eq!(h.run("LRANGE dst 0 -1"), list(&["1", "2", "3"]));
        assert_eq!(h.run("TTL dst"), Frame::Integer(-1));
        // Missing values are stored as empty strings.
        h.run("SET v_1 x");
        assert_eq!(h.run("SORT l GET v_* STORE dst"), Frame::Integer(3));
        assert_eq!(h.run("LRANGE dst 0 -1"), list(&["x", "", ""]));
        // In place.
        assert_eq!(h.run("SORT l DESC STORE l"), Frame::Integer(3));
        assert_eq!(h.run("LRANGE l 0 -1"), list(&["3", "2", "1"]));
        // An empty result deletes the destination.
        assert_eq!(h.run("SORT nokey STORE dst"), Frame::Integer(0));
        assert_eq!(h.run("EXISTS dst"), Frame::Integer(0));
    }

    #[test]
    fn errors() {
        let mut h = Harness::new();
        h.run("RPUSH l 1 2");
        h.run("SET s v");
        h.run("HSET hash f v");
        for (command, message) in [
            ("SORT s", WRONGTYPE),
            ("SORT hash", WRONGTYPE),
            ("SORT l BOGUS", "ERR syntax error"),
            ("SORT l LIMIT 0", "ERR syntax error"),
            ("SORT l BY", "ERR syntax error"),
            ("SORT l GET", "ERR syntax error"),
            ("SORT l STORE", "ERR syntax error"),
            (
                "SORT l LIMIT a 1",
                "ERR value is not an integer or out of range",
            ),
            ("SORT_RO l STORE dst", "ERR syntax error"),
            ("SORT", "ERR wrong number of arguments for 'sort' command"),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        assert_eq!(h.run("EXISTS dst"), Frame::Integer(0));
    }
}
