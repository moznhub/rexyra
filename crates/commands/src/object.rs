//! `OBJECT` — introspection of how a value is stored.

use bytes::Bytes;
use rexyra_engine::Value;
use rexyra_resp::{parse_i64, Frame};

use crate::{is, unknown_subcommand, wrong_subcommand_arity, Ctx};

/// Longest string Redis stores in its compact "embedded" form.
const EMBSTR_MAX: usize = 44;
/// Approximate size up to which Redis keeps a list in a single listpack.
const LIST_LISTPACK_BYTES: usize = 8192;

/// The encoding name Redis would report for this value. Client tooling and
/// test suites inspect it, so the names follow Redis even though the
/// in-memory representation here is different.
fn encoding(value: &Value) -> &'static str {
    match value {
        Value::String(s) if parse_i64(s).is_some() => "int",
        Value::String(s) if s.len() <= EMBSTR_MAX => "embstr",
        Value::String(_) => "raw",
        Value::List(list) => {
            // Redis keeps a list in one listpack while it fits in 8 KB.
            let bytes: usize = list.iter().map(|element| element.len() + 11).sum();
            if bytes <= LIST_LISTPACK_BYTES {
                "listpack"
            } else {
                "quicklist"
            }
        }
        // `listpackex` once a field TTL has been set, as in Redis.
        Value::Hash(hash) if hash.is_compact() && hash.has_field_ttls() => "listpackex",
        Value::Hash(hash) if hash.is_compact() => "listpack",
        Value::Hash(_) => "hashtable",
        Value::Set(set) => set.encoding(),
        Value::ZSet(zset) if zset.is_compact() => "listpack",
        Value::ZSet(_) => "skiplist",
    }
}

/// `OBJECT <ENCODING | REFCOUNT | IDLETIME | FREQ> key`
///
/// None of these count as using the key: its idle time and access
/// frequency stay as they were.
pub(crate) fn object(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let sub = &args[1];
    let known =
        is(sub, "ENCODING") || is(sub, "REFCOUNT") || is(sub, "FREQ") || is(sub, "IDLETIME");
    if !known {
        return unknown_subcommand("OBJECT", sub);
    }
    if args.len() != 3 {
        return wrong_subcommand_arity("object", sub);
    }
    let lfu = ctx.server.maxmemory_policy.is_lfu();
    let (db, now) = ctx.db();
    let Some(value) = db.peek(&args[2], now) else {
        return Frame::Null;
    };
    if is(sub, "ENCODING") {
        return Frame::bulk(encoding(value));
    }
    if is(sub, "REFCOUNT") {
        return Frame::Integer(1);
    }
    let Some(access) = db.access_info(&args[2], now) else {
        return Frame::Null;
    };
    // Redis keeps either the idle time or the frequency of a key, depending
    // on the policy, and refuses to report the other. Both are kept here,
    // but the replies follow Redis.
    match (is(sub, "FREQ"), lfu) {
        (true, true) => Frame::Integer(i64::from(access.frequency)),
        (false, false) => Frame::Integer(access.idle_seconds as i64),
        (true, false) => Frame::error(
            "ERR An LFU maxmemory policy is not selected, access frequency not tracked. \
             Please note that when switching between policies at runtime LRU and LFU \
             data will take some time to adjust.",
        ),
        (false, true) => Frame::error(
            "ERR An LFU maxmemory policy is selected, idle time not tracked. \
             Please note that when switching between policies at runtime LRU and LFU \
             data will take some time to adjust.",
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, err, Harness};
    use rexyra_resp::Frame;

    #[test]
    fn encoding() {
        let mut h = Harness::new();
        h.run("SET n 12345");
        h.run("SET neg -7");
        h.run("SET short hello");
        h.run("SET notint 007");
        h.cmd(&["SET", "long", &"x".repeat(45)]);
        h.cmd(&["SET", "edge", &"x".repeat(44)]);
        assert_eq!(h.run("OBJECT ENCODING n"), bulk("int"));
        assert_eq!(h.run("OBJECT ENCODING neg"), bulk("int"));
        assert_eq!(h.run("OBJECT ENCODING short"), bulk("embstr"));
        assert_eq!(h.run("OBJECT ENCODING notint"), bulk("embstr"));
        assert_eq!(h.run("OBJECT ENCODING edge"), bulk("embstr"));
        assert_eq!(h.run("OBJECT ENCODING long"), bulk("raw"));
        assert_eq!(h.run("object encoding missing"), Frame::Null);
    }

    #[test]
    fn refcount_and_freq() {
        let mut h = Harness::new();
        h.run("SET k v");
        assert_eq!(h.run("OBJECT REFCOUNT k"), Frame::Integer(1));
        assert_eq!(h.run("OBJECT REFCOUNT missing"), Frame::Null);
        assert!(
            matches!(h.run("OBJECT FREQ k"), Frame::Error(e) if e.starts_with("ERR An LFU maxmemory policy is not selected"))
        );
        assert_eq!(h.run("OBJECT FREQ missing"), Frame::Null);
    }

    #[test]
    fn errors() {
        let mut h = Harness::new();
        assert_eq!(
            h.run("OBJECT NOPE k"),
            err("ERR unknown subcommand 'NOPE'. Try OBJECT HELP.")
        );
        assert_eq!(
            h.run("OBJECT ENCODING"),
            err("ERR wrong number of arguments for 'object|encoding' command")
        );
        assert_eq!(
            h.run("OBJECT ENCODING a b"),
            err("ERR wrong number of arguments for 'object|encoding' command")
        );
    }
}
