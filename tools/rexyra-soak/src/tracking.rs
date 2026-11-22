//! A client that caches what it reads with client-side caching
//! (`CLIENT TRACKING`), and a check that it never keeps a stale value.
//!
//! Three connections: `reader` turns tracking on and reads keys, keeping
//! each value it reads; `listener` is subscribed to `__redis__:invalidate`
//! and receives the reader's invalidations (`REDIRECT`, so all three speak
//! RESP2); `writer` changes the keys. One round:
//!
//! 1. the reader reads a key and keeps the value;
//! 2. the writer changes one or more keys (`SET`, `MSET`, `INCR`, `APPEND`,
//!    `DEL`, or a write that changes nothing);
//! 3. the listener sends `PING`. The writer has had its reply, so the
//!    server has already queued any invalidation that write causes, and it
//!    reaches the listener before the `PONG`. Every key named is dropped
//!    from the cache;
//! 4. the writer reads, without tracking, a sample of the keys still cached
//!    and the keys it just wrote. Any cached value that differs from the
//!    server's is a stale value: an invalidation the server failed to send.
//!
//! The keys are this check's own (`soak-tracking:<n>`), apart from the
//! workers', and nothing gives them a time to live, so nothing changes them
//! but the writer.

use std::collections::hash_map::{Entry, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use serde_json::json;

use crate::model::Rng;
use crate::resp::Reply;
use crate::{connect, other, unix_now, Options, Shared};

/// How many keys the check reads and writes.
const KEYS: usize = 500;
/// How many cached keys are compared with the server after each round.
const SAMPLE: usize = 8;
const CHANNEL: &str = "__redis__:invalidate";

#[derive(Default)]
pub struct Counts {
    /// Rounds completed.
    pub rounds: AtomicU64,
    /// Cached values compared with the server's.
    pub checks: AtomicU64,
    /// Keys named in invalidation messages received.
    pub invalidated: AtomicU64,
    /// Cached values that differed from the server's: the failure this
    /// check exists to find.
    pub stale: AtomicU64,
    /// Connection or protocol errors.
    pub errors: AtomicU64,
}

fn key(n: usize) -> Vec<u8> {
    format!("soak-tracking:{n}").into_bytes()
}

/// Runs rounds until the soak stops. Returns the error that ended it early.
pub fn run(o: Arc<Options>, shared: Arc<Shared>) -> io::Result<()> {
    let counts = &shared.tracking;
    let mut rng = Rng::new(o.seed ^ 0x7472_6163_6b69_6e67);
    let mut writer = connect(&o)?;
    let mut listener = connect(&o)?;
    let mut reader = connect(&o)?;

    // Start from nothing, whatever an earlier run left.
    for n in 0..KEYS {
        writer.call(&[b"DEL", &key(n)])?;
    }
    let listener_id = listener.call(&[b"CLIENT", b"ID"])?.int()?;
    let subscribed = listener.call(&[b"SUBSCRIBE", CHANNEL.as_bytes()])?;
    if subscribed.is_error() {
        return Err(other(format!("SUBSCRIBE: {}", subscribed.short())));
    }
    let id = listener_id.to_string();
    let on = reader.call(&[b"CLIENT", b"TRACKING", b"ON", b"REDIRECT", id.as_bytes()])?;
    if on != Reply::Simple("OK".into()) {
        return Err(other(format!("CLIENT TRACKING ON: {}", on.short())));
    }

    // What the reader holds: key number to the value it read.
    let mut cache: HashMap<usize, Option<Vec<u8>>> = HashMap::new();
    let mut counter = 0u64;
    while !shared.stop.load(Relaxed) {
        // 1. Read one key, through the cache.
        let n = rng.below(KEYS);
        if let Entry::Vacant(slot) = cache.entry(n) {
            slot.insert(bulk(reader.call(&[b"GET", &key(n)])?)?);
        }

        // 2. Change one or more keys (or not, for a write that changes
        // nothing).
        let mut written = Vec::new();
        let target = rng.below(KEYS);
        counter += 1;
        let value = format!("v{counter}").into_bytes();
        let reply = match rng.below(10) {
            0..=3 => {
                written.push(target);
                writer.call(&[b"SET", &key(target), &value])?
            }
            4 => {
                let second = rng.below(KEYS);
                written.extend([target, second]);
                writer.call(&[b"MSET", &key(target), &value, &key(second), &value])?
            }
            5 => {
                written.push(target);
                writer.call(&[b"APPEND", &key(target), b"+"])?
            }
            6 => {
                written.push(target);
                writer.call(&[b"DEL", &key(target)])?
            }
            7 => {
                // A counter: refused on a key holding text, which changes
                // nothing.
                written.push(target);
                writer.call(&[b"INCR", &key(target)])?
            }
            _ => {
                // Set only if missing: often changes nothing.
                written.push(target);
                writer.call(&[b"SET", &key(target), &value, b"NX"])?
            }
        };
        if let Reply::Error(text) = &reply {
            if !text.starts_with("ERR value is not an integer") {
                return Err(other(format!("writer: {text}")));
            }
        }

        // 3. Collect the invalidations that write caused.
        listener.send(&ping())?;
        loop {
            match listener.read()? {
                Reply::Array(Some(parts)) if is_pong(&parts) => break,
                Reply::Array(Some(parts)) if is_invalidation(&parts) => match parts.get(2) {
                    Some(Reply::Array(Some(keys))) => {
                        for k in keys {
                            counts.invalidated.fetch_add(1, Relaxed);
                            if let Some(n) = key_number(k) {
                                cache.remove(&n);
                            }
                        }
                    }
                    // A flush: everything goes.
                    Some(Reply::Bulk(None) | Reply::Array(None)) => cache.clear(),
                    other => {
                        return Err(crate::other(format!(
                            "unexpected invalidation payload {other:?}"
                        )))
                    }
                },
                other => {
                    return Err(crate::other(format!(
                        "unexpected message on the listener: {}",
                        other.short()
                    )))
                }
            }
        }

        // 4. Whatever is still cached must be what the server holds.
        let mut sample: Vec<usize> = written
            .iter()
            .copied()
            .filter(|n| cache.contains_key(n))
            .collect();
        let cached: Vec<usize> = cache.keys().copied().collect();
        for _ in 0..SAMPLE.min(cached.len()) {
            sample.push(cached[rng.below(cached.len())]);
        }
        for n in sample {
            let actual = bulk(writer.call(&[b"GET", &key(n)])?)?;
            counts.checks.fetch_add(1, Relaxed);
            let Some(held) = cache.get(&n) else { continue };
            if *held != actual {
                if counts.stale.fetch_add(1, Relaxed) < 50 {
                    shared.note(json!({
                        "event": "tracking-stale",
                        "at": unix_now(),
                        "key": String::from_utf8_lossy(&key(n)),
                        "cached": held.as_deref().map(String::from_utf8_lossy),
                        "server": actual.as_deref().map(String::from_utf8_lossy),
                    }));
                }
                // Read again, so one miss is counted once.
                cache.remove(&n);
            }
        }
        counts.rounds.fetch_add(1, Relaxed);
    }
    Ok(())
}

fn ping() -> Vec<u8> {
    let mut buf = Vec::new();
    crate::resp::encode(&mut buf, &[b"PING"]);
    buf
}

/// A string value, or none for a missing key. Reading a key that holds
/// something else (a counter is still a string) is an error.
fn bulk(reply: Reply) -> io::Result<Option<Vec<u8>>> {
    match reply {
        Reply::Bulk(value) => Ok(value),
        other => Err(crate::other(format!("GET replied {}", other.short()))),
    }
}

fn text(reply: &Reply) -> Option<&[u8]> {
    match reply {
        Reply::Bulk(Some(bytes)) => Some(bytes),
        Reply::Simple(s) => Some(s.as_bytes()),
        _ => None,
    }
}

/// `PING` in subscribed mode replies `pong` and an empty string.
fn is_pong(parts: &[Reply]) -> bool {
    parts.first().and_then(text) == Some(b"pong")
}

fn is_invalidation(parts: &[Reply]) -> bool {
    parts.first().and_then(text) == Some(b"message")
        && parts.get(1).and_then(text) == Some(CHANNEL.as_bytes())
}

fn key_number(reply: &Reply) -> Option<usize> {
    let name = text(reply)?;
    std::str::from_utf8(name.strip_prefix(b"soak-tracking:")?)
        .ok()?
        .parse()
        .ok()
}

/// What `run` ended with, recorded where the soak keeps its events.
pub fn finished(shared: &Shared, result: io::Result<()>) {
    if let Err(e) = result {
        shared.tracking.errors.fetch_add(1, Relaxed);
        shared.note(json!({"event": "tracking-error", "at": unix_now(), "error": e.to_string()}));
    }
}
