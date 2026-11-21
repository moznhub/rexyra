//! The memory limit: `maxmemory`, the eviction policies, and freeing large
//! things on a background thread.
//!
//! Before each command the server checks its memory use against
//! `maxmemory`. If it is over, keys are evicted according to the policy
//! until it is under again. If nothing can be evicted, commands that could
//! add data are refused with an `OOM` error; reads and deletes still work.
//!
//! Like Redis, the LRU and LFU policies are approximate: instead of keeping
//! every key in order, a few keys are sampled and the best candidate among
//! them goes. More samples (`maxmemory-samples`) make it more precise.

use std::any::Any;
use std::sync::mpsc;

use bytes::Bytes;
use rexyra_engine::{Candidate, Keyspace};

use crate::{admin, Ctx, Server};

/// At most this many keys are evicted on behalf of one command, so that a
/// large overshoot is worked off over several commands instead of stalling
/// one of them.
const MAX_EVICTIONS_PER_COMMAND: usize = 1_000;
/// Collections with more elements than this are worth freeing in the
/// background when a key is unlinked.
const LAZY_FREE_ELEMENTS: usize = 64;

pub(crate) const ERR_OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";

/// Which keys go when memory is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// None: writes are refused instead.
    NoEviction,
    /// The least recently used key ...
    AllKeysLru,
    /// ... among keys with a TTL.
    VolatileLru,
    /// The least frequently used key ...
    AllKeysLfu,
    /// ... among keys with a TTL.
    VolatileLfu,
    /// Any key ...
    AllKeysRandom,
    /// ... among keys with a TTL.
    VolatileRandom,
    /// The key with a TTL that expires soonest.
    VolatileTtl,
}

const POLICIES: [(&str, Policy); 8] = [
    ("noeviction", Policy::NoEviction),
    ("allkeys-lru", Policy::AllKeysLru),
    ("volatile-lru", Policy::VolatileLru),
    ("allkeys-lfu", Policy::AllKeysLfu),
    ("volatile-lfu", Policy::VolatileLfu),
    ("allkeys-random", Policy::AllKeysRandom),
    ("volatile-random", Policy::VolatileRandom),
    ("volatile-ttl", Policy::VolatileTtl),
];

impl Policy {
    pub fn parse(text: &str) -> Option<Policy> {
        POLICIES
            .iter()
            .find(|(name, _)| text.eq_ignore_ascii_case(name))
            .map(|(_, policy)| *policy)
    }

    pub fn name(self) -> &'static str {
        POLICIES
            .iter()
            .find(|(_, policy)| *policy == self)
            .map_or("noeviction", |(name, _)| name)
    }

    /// Whether access frequency, rather than recency, decides.
    pub fn is_lfu(self) -> bool {
        matches!(self, Policy::AllKeysLfu | Policy::VolatileLfu)
    }

    fn volatile_only(self) -> bool {
        matches!(
            self,
            Policy::VolatileLru
                | Policy::VolatileLfu
                | Policy::VolatileRandom
                | Policy::VolatileTtl
        )
    }
}

/// Where the figure for memory in use comes from.
#[derive(Clone, Copy)]
pub enum MemoryMeter {
    /// The process's allocator, which knows exactly.
    Allocator(fn() -> u64),
    /// Adding up the sizes of keys and values. Slow (it visits everything)
    /// and approximate; for use where no counting allocator is installed.
    Estimate,
}

/// Memory in use, in bytes.
pub fn used_memory(server: &Server, keyspace: &mut Keyspace) -> u64 {
    match server.meter {
        MemoryMeter::Allocator(allocated) => allocated(),
        MemoryMeter::Estimate => (0..keyspace.len())
            .map(|index| {
                keyspace
                    .db(index)
                    .iter()
                    .map(|(key, entry)| {
                        (admin::KEY_OVERHEAD + key.len() + admin::value_size(&entry.value, 0))
                            as u64
                    })
                    .sum::<u64>()
            })
            .sum(),
    }
}

/// Chooses the key to evict: the best of a few samples from each database.
fn pick_victim(server: &Server, keyspace: &mut Keyspace, now: u64) -> Option<(usize, Bytes)> {
    let policy = server.maxmemory_policy;
    let volatile_only = policy.volatile_only();
    // Higher is a better candidate for eviction.
    let rank = |candidate: &Candidate| -> u64 {
        match policy {
            Policy::AllKeysLru | Policy::VolatileLru => candidate.idle_seconds,
            // Rarely used first; among equals, the one unused for longest.
            Policy::AllKeysLfu | Policy::VolatileLfu => {
                ((255 - u64::from(candidate.frequency)) << 32)
                    | candidate.idle_seconds.min(u64::from(u32::MAX))
            }
            Policy::VolatileTtl => u64::MAX - candidate.expire_at.unwrap_or(u64::MAX),
            Policy::AllKeysRandom | Policy::VolatileRandom => rexyra_engine::random_u64(),
            Policy::NoEviction => 0,
        }
    };
    let mut best: Option<(u64, usize, Bytes)> = None;
    for index in 0..keyspace.len() {
        let db = keyspace.db(index);
        let samples = if policy == Policy::VolatileTtl {
            1
        } else {
            server.maxmemory_samples
        };
        for _ in 0..samples {
            let candidate = if policy == Policy::VolatileTtl {
                // The expiry index knows the answer exactly.
                db.soonest_to_expire(now)
            } else {
                db.sample(volatile_only, now)
            };
            let Some(candidate) = candidate else {
                break;
            };
            let score = rank(&candidate);
            if best.as_ref().is_none_or(|(top, _, _)| score > *top) {
                best = Some((score, index, candidate.key));
            }
        }
    }
    best.map(|(_, index, key)| (index, key))
}

/// Evicts keys until memory use is within `maxmemory`. Returns `true` if it
/// is still over the limit and nothing more can be evicted, in which case
/// commands that may add data must be refused.
pub(crate) fn free_memory(ctx: &mut Ctx<'_>) -> bool {
    let limit = ctx.server.maxmemory;
    if limit == 0 || ctx.server.loading {
        return false;
    }
    for _ in 0..MAX_EVICTIONS_PER_COMMAND {
        if used_memory(ctx.server, ctx.keyspace) <= limit {
            return false;
        }
        if ctx.server.maxmemory_policy == Policy::NoEviction {
            return true;
        }
        let Some((db, key)) = pick_victim(ctx.server, ctx.keyspace, ctx.now) else {
            return true;
        };
        if ctx.keyspace.db(db).remove(&key, ctx.now) {
            ctx.server.stats.evicted_keys += 1;
            // The key is gone for good, so the log has to say so.
            if let Some(aof) = &mut ctx.server.aof {
                aof.log(db, &[&b"DEL"[..], &key[..]]);
            }
            crate::notify::notify_in(ctx, db, crate::notify::EVICTED, "evicted", &key);
        }
    }
    // Still over, but making progress: the next command continues.
    false
}

/// Drops `garbage` on a background thread, so that freeing something large
/// does not hold up commands.
pub(crate) fn lazy_free(server: &mut Server, garbage: Box<dyn Any + Send>) {
    if server.lazy_free.is_none() {
        let (tx, rx) = mpsc::channel::<Box<dyn Any + Send>>();
        let spawned = std::thread::Builder::new()
            .name("lazy-free".to_owned())
            .spawn(move || {
                for garbage in rx {
                    drop(garbage);
                }
            });
        if spawned.is_ok() {
            server.lazy_free = Some(tx);
        }
    }
    // If there is no thread to take it, it is simply dropped here.
    if let Some(tx) = &server.lazy_free {
        let _ = tx.send(garbage);
    }
}

/// Whether a value is big enough that freeing it is better done in the
/// background.
pub(crate) fn worth_lazy_free(value: &rexyra_engine::Value) -> bool {
    use rexyra_engine::Value;
    let elements = match value {
        Value::String(_) => 0,
        Value::List(list) => list.len(),
        Value::Hash(hash) => hash.len(),
        Value::Set(set) => set.len(),
        Value::ZSet(zset) => zset.len(),
    };
    elements > LAZY_FREE_ELEMENTS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, err, Harness};
    use rexyra_resp::Frame;

    const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";

    fn dbsize(h: &mut Harness) -> i64 {
        match h.run("DBSIZE") {
            Frame::Integer(n) => n,
            other => panic!("{other:?}"),
        }
    }

    /// Fills the server with `count` keys of about 100 bytes each.
    fn fill(h: &mut Harness, count: usize) {
        let value = "x".repeat(100);
        for n in 0..count {
            h.cmd(&["SET", &format!("key:{n:04}"), &value]);
        }
    }

    #[test]
    fn policy_names() {
        for (name, policy) in POLICIES {
            assert_eq!(Policy::parse(name), Some(policy));
            assert_eq!(policy.name(), name);
        }
        assert_eq!(Policy::parse("ALLKEYS-LRU"), Some(Policy::AllKeysLru));
        assert_eq!(Policy::parse("sometimes"), None);
        assert!(Policy::VolatileLfu.is_lfu() && !Policy::VolatileLru.is_lfu());
    }

    #[test]
    fn without_a_limit_nothing_is_evicted_or_refused() {
        let mut h = Harness::new();
        fill(&mut h, 500);
        assert_eq!(dbsize(&mut h), 500);
        assert_eq!(h.server.stats.evicted_keys, 0);
    }

    #[test]
    fn noeviction_refuses_writes_but_not_reads_or_deletes() {
        let mut h = Harness::new();
        fill(&mut h, 100);
        h.run("CONFIG SET maxmemory 5000");
        assert_eq!(h.run("SET another v"), err(OOM));
        assert_eq!(h.run("RPUSH list v"), err(OOM));
        assert_eq!(h.run("APPEND key:0000 more"), err(OOM));
        // Reading, deleting and administration still work ...
        assert!(matches!(h.run("GET key:0000"), Frame::Bulk(_)));
        assert_eq!(h.run("DEL key:0000"), Frame::Integer(1));
        assert_eq!(h.run("EXPIRE key:0001 100"), Frame::Integer(1));
        assert_eq!(dbsize(&mut h), 99);
        // ... and a transaction that would write is refused as a whole.
        h.run("MULTI");
        assert_eq!(h.run("SET another v"), err(OOM));
        assert_eq!(
            h.run("EXEC"),
            err("EXECABORT Transaction discarded because of previous errors.")
        );
        // Deleting enough makes room again.
        for n in 1..80 {
            h.cmd(&["DEL", &format!("key:{n:04}")]);
        }
        assert_eq!(h.run("SET another v"), Frame::ok());
        assert_eq!(h.server.stats.evicted_keys, 0);
    }

    #[test]
    fn allkeys_policies_keep_memory_under_the_limit() {
        for policy in ["allkeys-lru", "allkeys-lfu", "allkeys-random"] {
            let mut h = Harness::new();
            h.cmd(&["CONFIG", "SET", "maxmemory-policy", policy]);
            h.run("CONFIG SET maxmemory 20000");
            fill(&mut h, 1000);
            let used = used_memory(&h.server, &mut h.keyspace);
            // The last write may have gone over; the next command trims.
            assert!(used < 20_500, "{policy}: {used}");
            let left = dbsize(&mut h);
            assert!((50..200).contains(&left), "{policy}: {left} keys left");
            assert_eq!(h.server.stats.evicted_keys as i64, 1000 - left, "{policy}");
        }
    }

    #[test]
    fn hashes_with_field_ttls_are_counted_and_evicted_like_others() {
        let mut h = Harness::new();
        h.run("CONFIG SET maxmemory-policy allkeys-random");
        h.run("CONFIG SET maxmemory 30000");
        let value = "x".repeat(100);
        for n in 0..300 {
            let key = format!("h:{n:04}");
            h.cmd(&["HSET", &key, "a", &value, "b", &value]);
            h.cmd(&["HPEXPIRE", &key, "1000", "FIELDS", "1", "a"]);
        }
        assert!(used_memory(&h.server, &mut h.keyspace) < 31_000);
        let left = dbsize(&mut h);
        assert!(left < 150, "{left} keys left");
        assert_eq!(h.server.stats.evicted_keys as i64, 300 - left);
        // Evicted hashes left nothing behind for the field reclaimer.
        let Frame::Verbatim { data, .. } = h.run("INFO keyspace") else {
            panic!("no INFO")
        };
        let expected = format!("subexpiry={left}\r\n");
        assert!(data.ends_with(expected.as_bytes()), "{data:?}");
        h.advance(2_000);
        h.keyspace.cron(h.now, 10_000);
        assert_eq!(dbsize(&mut h), left);
        let Frame::Bulk(survivor) = h.run("RANDOMKEY") else {
            panic!("no key left")
        };
        let survivor = String::from_utf8(survivor.to_vec()).unwrap();
        assert_eq!(h.cmd(&["HLEN", &survivor]), Frame::Integer(1));
    }

    #[test]
    fn lru_evicts_what_was_not_used_recently() {
        let mut h = Harness::new();
        h.run("CONFIG SET maxmemory-policy allkeys-lru");
        h.run("CONFIG SET maxmemory-samples 64");
        fill(&mut h, 100);
        // Much later, half of the keys are read.
        h.advance(100_000);
        for n in 0..50 {
            h.cmd(&["GET", &format!("key:{n:04}")]);
        }
        let used = used_memory(&h.server, &mut h.keyspace);
        h.cmd(&["CONFIG", "SET", "maxmemory", &(used * 3 / 4).to_string()]);
        h.run("PING");
        let survivors = |h: &mut Harness, range: std::ops::Range<usize>| {
            range
                .filter(|n| h.cmd(&["EXISTS", &format!("key:{n:04}")]) == Frame::Integer(1))
                .count()
        };
        assert_eq!(
            survivors(&mut h, 0..50),
            50,
            "recently used keys were evicted"
        );
        assert!(survivors(&mut h, 50..100) < 30);
    }

    #[test]
    fn lfu_evicts_what_is_rarely_used() {
        let mut h = Harness::new();
        h.run("CONFIG SET maxmemory-policy allkeys-lfu");
        h.run("CONFIG SET maxmemory-samples 64");
        fill(&mut h, 100);
        for _ in 0..200 {
            for n in 0..50 {
                h.cmd(&["GET", &format!("key:{n:04}")]);
            }
        }
        // The rarely used half is touched last, so recency favours it.
        for n in 50..100 {
            h.cmd(&["GET", &format!("key:{n:04}")]);
        }
        let used = used_memory(&h.server, &mut h.keyspace);
        h.cmd(&["CONFIG", "SET", "maxmemory", &(used * 3 / 4).to_string()]);
        h.run("PING");
        let survivors = (0..50)
            .filter(|n| h.cmd(&["EXISTS", &format!("key:{n:04}")]) == Frame::Integer(1))
            .count();
        assert_eq!(survivors, 50, "frequently used keys were evicted");
    }

    #[test]
    fn volatile_policies_only_touch_keys_with_a_ttl() {
        for policy in [
            "volatile-lru",
            "volatile-lfu",
            "volatile-random",
            "volatile-ttl",
        ] {
            let mut h = Harness::new();
            h.cmd(&["CONFIG", "SET", "maxmemory-policy", policy]);
            fill(&mut h, 100);
            for n in 0..40 {
                h.cmd(&[
                    "EXPIRE",
                    &format!("key:{n:04}"),
                    &(1000 + n * 10).to_string(),
                ]);
            }
            let used = used_memory(&h.server, &mut h.keyspace);
            h.cmd(&["CONFIG", "SET", "maxmemory", &(used * 4 / 5).to_string()]);
            h.run("PING");
            for n in 40..100 {
                assert_eq!(
                    h.cmd(&["EXISTS", &format!("key:{n:04}")]),
                    Frame::Integer(1),
                    "{policy} evicted a key without a TTL"
                );
            }
            let left = dbsize(&mut h);
            assert!((78..=82).contains(&left), "{policy}: {left}");

            // With every key that has a TTL gone, writes are refused.
            h.cmd(&["CONFIG", "SET", "maxmemory", &(used / 4).to_string()]);
            assert_eq!(h.run("SET more v"), err(OOM), "{policy}");
            assert_eq!(dbsize(&mut h), 60, "{policy}");
        }
    }

    #[test]
    fn volatile_ttl_evicts_the_soonest_to_expire_first() {
        let mut h = Harness::new();
        h.run("CONFIG SET maxmemory-policy volatile-ttl");
        fill(&mut h, 20);
        for n in 0..20 {
            h.cmd(&[
                "EXPIRE",
                &format!("key:{n:04}"),
                &(1000 + n * 100).to_string(),
            ]);
        }
        let used = used_memory(&h.server, &mut h.keyspace);
        h.cmd(&["CONFIG", "SET", "maxmemory", &(used / 2).to_string()]);
        h.run("PING");
        let left = dbsize(&mut h) as usize;
        assert!((9..=11).contains(&left), "{left}");
        // Exactly the ones with the longest TTLs remain.
        for n in 20 - left..20 {
            assert_eq!(
                h.cmd(&["EXISTS", &format!("key:{n:04}")]),
                Frame::Integer(1)
            );
        }
    }

    #[test]
    fn eviction_spans_databases() {
        let mut h = Harness::new();
        h.run("CONFIG SET maxmemory-policy allkeys-random");
        fill(&mut h, 50);
        h.run("SELECT 3");
        fill(&mut h, 50);
        let used = used_memory(&h.server, &mut h.keyspace);
        h.cmd(&["CONFIG", "SET", "maxmemory", &(used / 5).to_string()]);
        h.run("PING");
        let in_three = dbsize(&mut h);
        h.run("SELECT 0");
        let in_zero = dbsize(&mut h);
        assert!(in_three < 30 && in_zero < 30, "{in_zero} {in_three}");
        assert!(in_three + in_zero <= 22);
    }

    #[test]
    fn object_idletime_and_freq_follow_the_policy() {
        let mut h = Harness::new();
        h.run("SET k v");
        h.advance(42_000);
        assert_eq!(h.run("OBJECT IDLETIME k"), Frame::Integer(42));
        // Asking does not count as using the key ...
        assert_eq!(h.run("OBJECT ENCODING k"), bulk("embstr"));
        assert_eq!(h.run("OBJECT IDLETIME k"), Frame::Integer(42));
        // ... reading it does.
        h.run("GET k");
        assert_eq!(h.run("OBJECT IDLETIME k"), Frame::Integer(0));
        assert_eq!(h.run("OBJECT IDLETIME nokey"), Frame::Null);
        assert!(matches!(h.run("OBJECT FREQ k"), Frame::Error(text) if text.contains("LFU")));

        h.run("CONFIG SET maxmemory-policy allkeys-lfu");
        assert!(matches!(h.run("OBJECT FREQ k"), Frame::Integer(n) if n >= 5));
        assert!(matches!(h.run("OBJECT IDLETIME k"), Frame::Error(text) if text.contains("LFU")));
        for _ in 0..1000 {
            h.run("GET k");
        }
        assert!(matches!(h.run("OBJECT FREQ k"), Frame::Integer(n) if n > 10));
        assert_eq!(h.run("OBJECT FREQ nokey"), Frame::Null);
    }

    #[test]
    fn memory_settings() {
        let mut h = Harness::new();
        let get = |h: &mut Harness, name: &str| match h.cmd(&["CONFIG", "GET", name]) {
            Frame::Map(pairs) => pairs[0].1.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(get(&mut h, "maxmemory"), bulk("0"));
        assert_eq!(get(&mut h, "maxmemory-policy"), bulk("noeviction"));
        assert_eq!(get(&mut h, "maxmemory-samples"), bulk("5"));
        assert_eq!(h.run("CONFIG SET maxmemory 100mb"), Frame::ok());
        assert_eq!(get(&mut h, "maxmemory"), bulk("104857600"));
        assert_eq!(
            h.run("CONFIG SET maxmemory-policy volatile-ttl"),
            Frame::ok()
        );
        assert_eq!(get(&mut h, "maxmemory-policy"), bulk("volatile-ttl"));
        assert_eq!(
            h.run("CONFIG SET lfu-log-factor 20 lfu-decay-time 5"),
            Frame::ok()
        );
        assert_eq!(get(&mut h, "lfu-decay-time"), bulk("5"));
        for (command, why) in [
            (
                "CONFIG SET maxmemory-policy sometimes",
                "argument(s) must be one of the following",
            ),
            (
                "CONFIG SET maxmemory lots",
                "argument must be a memory value",
            ),
            (
                "CONFIG SET maxmemory-samples 0",
                "argument must be between 1 and 64 inclusive",
            ),
        ] {
            assert!(
                matches!(h.run(command), Frame::Error(text) if text.contains(why)),
                "{command}"
            );
        }
        let Frame::Verbatim { data, .. } = h.run("INFO memory stats") else {
            panic!("not text")
        };
        let info = String::from_utf8(data.to_vec()).unwrap();
        for line in [
            "maxmemory:104857600",
            "maxmemory_human:100.00M",
            "maxmemory_policy:volatile-ttl",
            "evicted_keys:0",
        ] {
            assert!(info.contains(line), "{line} not in {info}");
        }
    }

    #[test]
    fn unlink_and_async_flush_free_in_the_background() {
        let mut h = Harness::new();
        let mut big = vec!["RPUSH".to_owned(), "big".to_owned()];
        big.extend((0..500).map(|n| n.to_string()));
        h.cmd(&big.iter().map(String::as_str).collect::<Vec<_>>());
        h.run("SET small v");
        assert_eq!(h.run("UNLINK big small missing"), Frame::Integer(2));
        assert_eq!(dbsize(&mut h), 0);
        // The large list went to the background thread, which now exists.
        assert!(h.server.lazy_free.is_some());

        fill(&mut h, 100);
        h.run("SELECT 2");
        fill(&mut h, 100);
        assert_eq!(h.run("FLUSHDB ASYNC"), Frame::ok());
        assert_eq!(dbsize(&mut h), 0);
        h.run("SELECT 0");
        assert_eq!(dbsize(&mut h), 100);
        assert_eq!(h.run("FLUSHALL ASYNC"), Frame::ok());
        assert_eq!(dbsize(&mut h), 0);
        assert_eq!(h.run("FLUSHALL NOW"), err("ERR syntax error"));
        // The server is fully usable afterwards.
        assert_eq!(h.run("SET k v"), Frame::ok());
        assert_eq!(h.run("GET k"), bulk("v"));
    }
}
