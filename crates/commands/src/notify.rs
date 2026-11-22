//! Keyspace notifications (`notify-keyspace-events`).
//!
//! A command that changes a key reports it with [`notify`]: an event class
//! (one of the flags below), an event name such as `set` or `lpush`, and
//! the key. When the class is enabled, the event is published through the
//! ordinary Pub/Sub machinery ([`crate::pubsub::publish_message`]) on up to
//! two channels, as in Redis:
//!
//! * `__keyspace@<db>__:<key>` with the event name as the message (`K`);
//! * `__keyevent@<db>__:<event>` with the key as the message (`E`).
//!
//! So patterns, RESP2 and RESP3, ACL channel rules and output limits all
//! apply as they do to `PUBLISH`, and the messages reach their receivers
//! after everything already sent to them, ahead of the next reply.
//!
//! Three kinds of event are not raised by the command itself but noted by
//! the keyspace as they happen ([`rexyra_engine::Note`]): a key deleted
//! because its TTL passed (`expired`), a read that finds no key
//! (`keymiss`) and a key being created (`new`). [`flush`] publishes those;
//! it runs before every event a command raises, at the end of every
//! command and after the periodic expiry cycle, so each appears exactly
//! where it happened in the stream.
//!
//! [`flush`] also turns the keys the keyspace noted as changed into
//! client-side caching invalidations ([`crate::tracking`]), in the same
//! stream, so each comes where Redis sends it: after an `expired`, before
//! the event of the write that caused it.
//!
//! With the setting off (the default) [`notify`] costs one test of a flag
//! word, and the keyspace notes nothing it would not note anyway.

use bytes::{BufMut, Bytes, BytesMut};
use rexyra_engine::{Keyspace, Note};

use crate::tracking::Peers;
use crate::{Ctx, Server};

/// `K`: publish on `__keyspace@<db>__:<key>`.
pub const KEYSPACE: u32 = 1 << 0;
/// `E`: publish on `__keyevent@<db>__:<event>`.
pub const KEYEVENT: u32 = 1 << 1;
/// `g`: commands that work on any type (`DEL`, `EXPIRE`, `RENAME`, ...).
pub const GENERIC: u32 = 1 << 2;
/// `$`: string commands.
pub const STRING: u32 = 1 << 3;
/// `l`: list commands.
pub const LIST: u32 = 1 << 4;
/// `s`: set commands.
pub const SET: u32 = 1 << 5;
/// `h`: hash commands.
pub const HASH: u32 = 1 << 6;
/// `z`: sorted set commands.
pub const ZSET: u32 = 1 << 7;
/// `x`: a key was deleted because its TTL passed.
pub const EXPIRED: u32 = 1 << 8;
/// `e`: a key was evicted to stay within `maxmemory`.
pub const EVICTED: u32 = 1 << 9;
/// `t`: stream commands (accepted; this server has no streams).
pub const STREAM: u32 = 1 << 10;
/// `m`: a read found no key.
pub const KEY_MISS: u32 = 1 << 11;
/// `d`: module key types (accepted; this server has no modules).
pub const MODULE: u32 = 1 << 13;
/// `n`: a key was created.
pub const NEW: u32 = 1 << 14;
/// `A`: every class except `m` and `n`.
pub const ALL: u32 =
    GENERIC | STRING | LIST | SET | HASH | ZSET | EXPIRED | EVICTED | STREAM | MODULE;

/// The error `CONFIG SET` gives for a letter that is not a class.
pub const ERR_INVALID: &str = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";

/// Parses a `notify-keyspace-events` value, as Redis does: every character
/// must be a class letter; repeats are allowed.
pub fn parse(text: &[u8]) -> Option<u32> {
    text.iter().try_fold(0, |flags, &letter| {
        Some(
            flags
                | match letter {
                    b'A' => ALL,
                    b'g' => GENERIC,
                    b'$' => STRING,
                    b'l' => LIST,
                    b's' => SET,
                    b'h' => HASH,
                    b'z' => ZSET,
                    b'x' => EXPIRED,
                    b'e' => EVICTED,
                    b'K' => KEYSPACE,
                    b'E' => KEYEVENT,
                    b't' => STREAM,
                    b'm' => KEY_MISS,
                    b'd' => MODULE,
                    b'n' => NEW,
                    _ => return None,
                },
        )
    })
}

/// The flags written back the way Redis shows them: `A` for all of its
/// classes (and then, as in Redis, no `n`), otherwise each class in a fixed
/// order, followed by `K`, `E` and `m`.
pub fn format(flags: u32) -> String {
    let mut text = String::new();
    if flags & ALL == ALL {
        text.push('A');
    } else {
        for (flag, letter) in [
            (GENERIC, 'g'),
            (STRING, '$'),
            (LIST, 'l'),
            (SET, 's'),
            (HASH, 'h'),
            (ZSET, 'z'),
            (EXPIRED, 'x'),
            (EVICTED, 'e'),
            (STREAM, 't'),
            (MODULE, 'd'),
            (NEW, 'n'),
        ] {
            if flags & flag != 0 {
                text.push(letter);
            }
        }
    }
    for (flag, letter) in [(KEYSPACE, 'K'), (KEYEVENT, 'E'), (KEY_MISS, 'm')] {
        if flags & flag != 0 {
            text.push(letter);
        }
    }
    text
}

/// Raises event `event` of class `class` for `key` in the client's
/// database. This is the one place commands report a change; it does
/// nothing unless the class is enabled.
#[inline]
pub(crate) fn notify(ctx: &mut Ctx<'_>, class: u32, event: &'static str, key: &[u8]) {
    if ctx.server.notify_flags & class != 0 {
        let db = ctx.client.db;
        raise(ctx, event, key, db);
    }
}

/// [`notify`] for a key in database `db`, which need not be the client's
/// (`MOVE`, `COPY ... DB`, eviction).
#[inline]
pub(crate) fn notify_in(ctx: &mut Ctx<'_>, db: usize, class: u32, event: &'static str, key: &[u8]) {
    if ctx.server.notify_flags & class != 0 {
        raise(ctx, event, key, db);
    }
}

#[inline(never)]
fn raise(ctx: &mut Ctx<'_>, event: &'static str, key: &[u8], db: usize) {
    // What the keyspace noted earlier in the command happened first: keys
    // that expired, and the invalidations of the keys the command changed
    // (sent before the event, as in Redis).
    let peers = Peers {
        others: ctx.clients,
        current: Some(ctx.client),
    };
    flush(ctx.server, ctx.keyspace, &peers);
    publish(ctx.server, event, key, db);
}

/// Publishes one event on the channels the flags ask for.
fn publish(server: &mut Server, event: &'static str, key: &[u8], db: usize) {
    // Nobody to tell: skip building the channel names.
    if !server.pubsub.has_subscribers() {
        return;
    }
    let flags = server.notify_flags;
    let db = db.to_string();
    if flags & KEYSPACE != 0 {
        let mut channel = BytesMut::with_capacity(16 + db.len() + key.len());
        channel.put_slice(b"__keyspace@");
        channel.put_slice(db.as_bytes());
        channel.put_slice(b"__:");
        channel.put_slice(key);
        let message = Bytes::from_static(event.as_bytes());
        crate::pubsub::publish_message(server, &channel.freeze(), &message, false);
    }
    if flags & KEYEVENT != 0 {
        let mut channel = BytesMut::with_capacity(16 + db.len() + event.len());
        channel.put_slice(b"__keyevent@");
        channel.put_slice(db.as_bytes());
        channel.put_slice(b"__:");
        channel.put_slice(event.as_bytes());
        let message = Bytes::copy_from_slice(key);
        crate::pubsub::publish_message(server, &channel.freeze(), &message, false);
    }
}

/// The classes whose events come from what the keyspace notes rather than
/// from the command: `expired`, `keymiss`, `new`, and `hexpired` (with the
/// `del` of a hash whose last fields expired).
const NOTED: u32 = EXPIRED | KEY_MISS | NEW | HASH | GENERIC;

/// Acts on what the keyspace has noted since the last call: logs what was
/// deleted by a TTL to the append-only file (a key as `DEL`, hash fields
/// as `HDEL`), publishes the `expired`, `hexpired`, `keymiss` and `new`
/// events, and invalidates the keys clients cache
/// ([`crate::tracking::changed`]), in the order they happened. `peers`
/// are the clients invalidations may go to.
pub fn flush(server: &mut Server, keyspace: &mut Keyspace, peers: &Peers<'_>) {
    for (db, note, key) in keyspace.take_notes() {
        let (class, event) = match note {
            Note::Changed { by_server } => {
                crate::tracking::changed(server, peers, key, by_server);
                continue;
            }
            Note::Expired => {
                if let Some(aof) = &mut server.aof {
                    aof.log(db, &[&b"DEL"[..], &key[..]]);
                }
                (EXPIRED, "expired")
            }
            Note::Missed => (KEY_MISS, "keymiss"),
            Note::Created => (NEW, "new"),
            Note::FieldsExpired { fields, emptied } => {
                // Deleting a hash's last fields deletes the hash, on
                // replay too.
                if let Some(aof) = &mut server.aof {
                    let mut command: Vec<&[u8]> = vec![b"HDEL", &key];
                    command.extend(fields.iter().map(|field| &field[..]));
                    aof.log(db, &command);
                }
                if server.notify_flags & HASH != 0 {
                    publish(server, "hexpired", &key, db);
                }
                if !emptied {
                    continue;
                }
                (GENERIC, "del")
            }
        };
        if server.notify_flags & class != 0 {
            publish(server, event, &key, db);
        }
    }
}

/// [`flush`], if the keyspace may have noted anything: at the end of a
/// command.
#[inline]
pub(crate) fn settle(ctx: &mut Ctx<'_>) {
    if ctx.server.aof.is_some()
        || ctx.server.notify_flags & NOTED != 0
        || crate::tracking::recording(ctx.server)
    {
        let peers = Peers {
            others: ctx.clients,
            current: Some(ctx.client),
        };
        flush(ctx.server, ctx.keyspace, &peers);
    }
}

/// Makes the keyspace note what the append-only file and the enabled
/// notifications need. Call whenever either changes.
pub fn sync(server: &Server, keyspace: &mut Keyspace) {
    // Expired hash fields raise `hexpired` (h) and, for a hash left
    // empty, `del` (g).
    keyspace.track_expired(
        server.aof.is_some() || server.notify_flags & (EXPIRED | HASH | GENERIC) != 0,
    );
    keyspace.note_created(server.notify_flags & NEW != 0);
}

/// Starts (`on`) or stops counting lookups in the client's database that
/// find no key as `keymiss` events. Redis counts the lookups of commands
/// that read, and those of a few that write (`SET ... GET`, `GETDEL`, the
/// sources of `SORT`, `BITOP`, `COPY` and the `*STORE` commands).
#[inline]
pub(crate) fn reading(ctx: &mut Ctx<'_>, on: bool) {
    if ctx.server.notify_flags & KEY_MISS != 0 {
        ctx.keyspace.db(ctx.client.db).note_misses(on);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, err, Harness};
    use rexyra_resp::Frame;

    #[test]
    fn parsing_and_formatting_match_redis() {
        let cases = [
            ("", ""),
            ("K", "K"),
            ("E", "E"),
            ("A", "A"),
            ("KA", "AK"),
            ("AKE", "AKE"),
            ("EKAnm", "AKEm"),
            ("Kg$lshzxetdnm", "AKm"),
            ("Kxeg", "gxeK"),
            ("lK", "lK"),
            ("KK", "K"),
            ("g$", "g$"),
            ("Km", "Km"),
            ("Kn", "nK"),
            ("KEg$lshzxe", "g$lshzxeKE"),
        ];
        for (given, shown) in cases {
            let flags = parse(given.as_bytes()).unwrap_or_else(|| panic!("{given}"));
            assert_eq!(format(flags), shown, "{given}");
        }
        // `n` stays set even where `A` hides it.
        assert_ne!(parse(b"KAn").unwrap() & NEW, 0);
        for bad in ["KEAb", "kea", "K E", "KE\n"] {
            assert_eq!(parse(bad.as_bytes()), None, "{bad}");
        }
    }

    #[test]
    fn config_set_and_get() {
        let mut h = Harness::new();
        let get = |h: &mut Harness| h.run("CONFIG GET notify-keyspace-events");
        let shown = |text: &str| Frame::Map(vec![(bulk("notify-keyspace-events"), bulk(text))]);
        assert_eq!(get(&mut h), shown(""));
        assert_eq!(h.run("CONFIG SET notify-keyspace-events KEA"), Frame::ok());
        assert_eq!(get(&mut h), shown("AKE"));
        assert_eq!(
            h.run("CONFIG SET notify-keyspace-events KEAb"),
            err("ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'.")
        );
        assert_eq!(get(&mut h), shown("AKE"));
        assert_eq!(
            h.cmd(&["CONFIG", "SET", "notify-keyspace-events", ""]),
            Frame::ok()
        );
        assert_eq!(get(&mut h), shown(""));
        assert_eq!(h.server.notify_flags, 0);
        assert_eq!(
            h.run("CONFIG SET notify-keyspace-events Kg maxmemory-samples 5"),
            Frame::ok()
        );
        assert_eq!(get(&mut h), shown("gK"));
        assert_eq!(
            h.run("CONFIG SET notify-keyspace-events Kg notify-keyspace-events E$"),
            err("ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - duplicate parameter")
        );
    }

    /// A subscriber (client 8) to every notification.
    fn listening(flags: &str) -> Harness {
        let mut h = Harness::new();
        h.cmd(&["CONFIG", "SET", "notify-keyspace-events", flags]);
        let mut other = crate::ClientState::new(8, "127.0.0.1:50001".to_owned());
        let pattern = Bytes::from_static(b"__key*@*__:*");
        other.subscriptions.patterns.insert(pattern.clone());
        h.clients.insert(8, other);
        h.server.pubsub.add_pattern_subscriber(&pattern, 8);
        h
    }

    /// The `(channel, message)` pairs published since the last call.
    fn events(h: &mut Harness) -> Vec<(String, String)> {
        std::mem::take(&mut h.server.pushes)
            .into_iter()
            .map(|(_, frame)| match frame {
                Frame::Push(items) => {
                    let text = |frame: &Frame| match frame {
                        Frame::Bulk(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        other => panic!("{other:?}"),
                    };
                    (text(&items[2]), text(&items[3]))
                }
                other => panic!("{other:?}"),
            })
            .collect()
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn both_channels_are_published_keyspace_first() {
        let mut h = listening("KEA");
        h.run("SET k v");
        assert_eq!(
            events(&mut h),
            pairs(&[("__keyspace@0__:k", "set"), ("__keyevent@0__:set", "k")])
        );
        h.run("SELECT 5");
        h.run("DEL k");
        assert!(events(&mut h).is_empty());
        h.run("LPUSH k a");
        assert_eq!(
            events(&mut h),
            pairs(&[("__keyspace@5__:k", "lpush"), ("__keyevent@5__:lpush", "k")])
        );
    }

    #[test]
    fn only_the_enabled_classes_and_channels_are_published() {
        let mut h = listening("Kl");
        h.run("SET s v");
        h.run("RPUSH l a");
        h.run("LPOP l");
        // The `del` of the emptied list is generic, not a list event.
        assert_eq!(
            events(&mut h),
            pairs(&[("__keyspace@0__:l", "rpush"), ("__keyspace@0__:l", "lpop")])
        );
        let mut h = listening("Eg");
        h.run("SET s v");
        h.run("DEL s");
        assert_eq!(events(&mut h), pairs(&[("__keyevent@0__:del", "s")]));
        // Classes without K or E publish nothing.
        let mut h = listening("A");
        h.run("SET s v");
        assert!(events(&mut h).is_empty());
    }

    #[test]
    fn nothing_is_published_by_default() {
        let mut h = listening("");
        h.run("SET s v");
        h.run("DEL s");
        h.run("GET s");
        assert!(events(&mut h).is_empty());
    }

    #[test]
    fn expired_keymiss_and_new_appear_where_they_happened() {
        let mut h = listening("KEAmn");
        h.run("SET a 1 PX 10");
        events(&mut h);
        h.advance(20);
        h.run("SET a 2");
        assert_eq!(
            events(&mut h),
            pairs(&[
                ("__keyspace@0__:a", "expired"),
                ("__keyevent@0__:expired", "a"),
                ("__keyspace@0__:a", "new"),
                ("__keyevent@0__:new", "a"),
                ("__keyspace@0__:a", "set"),
                ("__keyevent@0__:set", "a"),
            ])
        );
        h.run("PEXPIRE a 10");
        events(&mut h);
        h.advance(20);
        h.run("MGET a nope");
        assert_eq!(
            events(&mut h),
            pairs(&[
                ("__keyspace@0__:a", "expired"),
                ("__keyevent@0__:expired", "a"),
                ("__keyspace@0__:a", "keymiss"),
                ("__keyevent@0__:keymiss", "a"),
                ("__keyspace@0__:nope", "keymiss"),
                ("__keyevent@0__:keymiss", "nope"),
            ])
        );
        // A write does not count its lookups as misses.
        h.run("LPOP nope");
        h.run("DEL nope");
        assert!(events(&mut h).is_empty());
        // A read that fails before it looks anything up misses nothing.
        h.run("GETRANGE nope x y");
        assert!(events(&mut h).is_empty());
    }

    #[test]
    fn without_subscribers_nothing_is_queued() {
        let mut h = Harness::new();
        h.run("CONFIG SET notify-keyspace-events KEAmn");
        h.run("SET a 1");
        h.run("GET b");
        assert!(h.server.pushes.is_empty());
    }

    /// The keyspace events (`[db:]key event`) published since the last
    /// call, leaving out the `__keyevent` copies.
    fn keyspace(h: &mut Harness) -> Vec<String> {
        events(h)
            .into_iter()
            .filter_map(|(channel, event)| {
                let rest = channel.strip_prefix("__keyspace@")?;
                let (db, key) = rest.split_once("__:")?;
                Some(match db {
                    "0" => format!("{key} {event}"),
                    db => format!("{db}:{key} {event}"),
                })
            })
            .collect()
    }

    /// Runs `steps` (a command and the events it must raise) in order.
    /// `+N` advances the clock by N milliseconds.
    fn check(h: &mut Harness, steps: &[(&str, &[&str])]) {
        for (command, expected) in steps {
            if let Some(millis) = command.strip_prefix('+') {
                h.advance(millis.parse().unwrap());
                continue;
            }
            h.run(command);
            assert_eq!(keyspace(h), *expected, "{command}");
        }
    }

    /// The events and their order were taken from Redis 7.4 with
    /// `notify-keyspace-events KEAmn` (see `tools/rexyra-keyevents`).
    #[test]
    fn every_kind_of_change_raises_the_events_redis_does() {
        let mut h = listening("KAmn");
        check(
            &mut h,
            &[
                // strings
                ("SET s1 v", &["s1 new", "s1 set"]),
                ("SET s1 v2 EX 100", &["s1 set", "s1 expire"]),
                ("SET s1 v NX", &[]),
                ("SET s1 v3 KEEPTTL", &["s1 set"]),
                ("SET s2 v GET", &["s2 keymiss", "s2 new", "s2 set"]),
                ("SETEX s3 100 v", &["s3 new", "s3 set", "s3 expire"]),
                ("SETNX s3 v", &[]),
                ("GETSET s2 w", &["s2 set"]),
                ("GETDEL s2", &["s2 del"]),
                ("GETDEL s2", &["s2 keymiss"]),
                ("GETEX s1 PERSIST", &["s1 persist"]),
                ("GETEX s1 PERSIST", &[]),
                ("GETEX s1 PX 100000", &["s1 expire"]),
                ("GETEX s1 PXAT 1", &["s1 del"]),
                ("MSET a 1 b 2", &["a new", "a set", "b new", "b set"]),
                ("MSETNX a 1 c 2", &[]),
                ("INCRBY a 2", &["a incrby"]),
                ("INCRBYFLOAT a 1.5", &["a incrbyfloat"]),
                ("APPEND b x", &["b append"]),
                ("SETRANGE b 1 zz", &["b setrange"]),
                ("MGET a nope a", &["nope keymiss"]),
                // bitmaps
                ("SETBIT bits 1 1", &["bits new", "bits setbit"]),
                ("SETBIT bits 1 1", &[]),
                ("BITOP NOT bnot bits", &["bnot new", "bnot set"]),
                ("BITOP AND bnot nope", &["nope keymiss", "bnot del"]),
                ("BITFIELD bits SET u8 0 64", &["bits setbit"]),
                ("BITFIELD bits OVERFLOW FAIL INCRBY u8 0 300", &[]),
                ("BITFIELD nob GET u8 0", &["nob keymiss"]),
                // generic
                ("EXPIRE a 100", &["a expire"]),
                ("EXPIRE a 100 NX", &[]),
                ("PERSIST a", &["a persist"]),
                ("PERSIST a", &[]),
                ("EXPIRE a -1", &["a del"]),
                ("RENAME b c", &["c new", "b rename_from", "c rename_to"]),
                ("RENAME c c", &[]),
                ("COPY c d", &["d new", "d copy_to"]),
                ("COPY c d", &[]),
                ("COPY c d REPLACE", &["d new", "d copy_to"]),
                ("COPY c d DB 3", &["3:d new", "3:d copy_to"]),
                ("COPY nope d", &["nope keymiss"]),
                ("MOVE d 2", &["2:d new", "d move_from", "2:d move_to"]),
                ("DEL c nope d", &["c del"]),
                ("UNLINK s3", &["s3 del"]),
                ("EXISTS nope s3", &["nope keymiss", "s3 keymiss"]),
                ("TTL nope", &["nope keymiss"]),
                ("OBJECT ENCODING nope", &[]),
                ("MEMORY USAGE nope", &[]),
                ("SCAN 0", &[]),
                // lists
                ("RPUSH l a b c", &["l new", "l rpush"]),
                ("LPUSHX nol a", &[]),
                ("LMOVE l m LEFT RIGHT", &["m new", "m rpush", "l lpop"]),
                ("RPOPLPUSH l m", &["m lpush", "l rpop"]),
                ("LPOP l 5", &["l lpop", "l del"]),
                ("LPOP l", &[]),
                ("LRANGE l 0 -1", &["l keymiss"]),
                ("LSET m 0 x", &["m lset"]),
                ("LINSERT m BEFORE x y", &["m linsert"]),
                ("LINSERT m BEFORE nothing y", &[]),
                ("LREM m 0 y", &["m lrem"]),
                ("LTRIM m 5 6", &["m ltrim", "m del"]),
                ("RPUSH q a", &["q new", "q rpush"]),
                ("LMPOP 2 nol q LEFT", &["q lpop", "q del"]),
                ("RPUSH q a", &["q new", "q rpush"]),
                ("BLPOP q 0", &["q lpop", "q del"]),
                // hashes
                ("HSET h f v g w", &["h new", "h hset"]),
                ("HSET h f v", &["h hset"]),
                ("HSETNX h f v", &[]),
                ("HSETNX h n 1", &["h hset"]),
                ("HINCRBY h n 1", &["h hincrby"]),
                ("HINCRBYFLOAT h n 1.5", &["h hincrbyfloat"]),
                ("HDEL h nofield", &[]),
                ("HDEL h f g n", &["h hdel", "h del"]),
                ("HGET h f", &["h keymiss"]),
                // hash field expiry
                ("HSET e a 1 b 2", &["e new", "e hset"]),
                ("HEXPIRE e 100 FIELDS 1 a", &["e hexpire"]),
                ("HEXPIRE e 100 NX FIELDS 1 a", &[]),
                ("HPERSIST e FIELDS 1 a", &["e hpersist"]),
                ("HPERSIST e FIELDS 1 b", &[]),
                ("HEXPIRE e 0 FIELDS 2 a b", &["e hdel", "e del"]),
                // One miss each, though they look twice.
                ("HTTL e FIELDS 1 a", &["e keymiss"]),
                ("HRANDFIELD e", &["e keymiss"]),
                ("HPERSIST e FIELDS 1 a", &[]),
                // sets
                ("SADD x a b", &["x new", "x sadd"]),
                ("SADD x a", &[]),
                ("SREM x b", &["x srem"]),
                ("SMOVE x y a", &["x srem", "x del", "y new", "y sadd"]),
                ("SADD x b", &["x new", "x sadd"]),
                ("SINTERSTORE dst x y", &[]),
                ("SUNIONSTORE dst x y", &["dst new", "dst sunionstore"]),
                ("SDIFFSTORE dst x nos", &["nos keymiss", "dst sdiffstore"]),
                ("SINTERSTORE dst x nos", &["nos keymiss", "dst del"]),
                ("SPOP y 5", &["y spop", "y del"]),
                // sorted sets
                ("ZADD z 1 a 2 b", &["z new", "z zadd"]),
                ("ZADD z 1 a", &[]),
                ("ZADD z INCR 1 a", &["z zincr"]),
                ("ZINCRBY z 1 b", &["z zincr"]),
                ("ZREM z nomember", &[]),
                ("ZREMRANGEBYSCORE z 100 200", &[]),
                ("ZPOPMIN z", &["z zpopmin"]),
                ("ZRANGESTORE zr z 0 -1", &["zr new", "zr zrangestore"]),
                (
                    "ZUNIONSTORE zu 2 z noz",
                    &["noz keymiss", "zu new", "zu zunionstore"],
                ),
                ("ZINTERSTORE zu 2 z noz", &["noz keymiss", "zu del"]),
                ("ZREMRANGEBYRANK z 0 -1", &["z zremrangebyrank", "z del"]),
                ("ZSCORE z a", &["z keymiss"]),
                // sort
                ("RPUSH sl 2 1", &["sl new", "sl rpush"]),
                (
                    "SORT sl BY w_* STORE out",
                    &["w_2 keymiss", "w_1 keymiss", "out new", "out sortstore"],
                ),
                ("SORT nope STORE out", &["nope keymiss", "out del"]),
                // expiry found by a lookup
                ("SET t v PX 10", &["t new", "t set", "t expire"]),
                ("+20", &[]),
                ("GET t", &["t expired", "t keymiss"]),
                ("SET u v PX 10", &["u new", "u set", "u expire"]),
                ("+20", &[]),
                ("SET u w", &["u expired", "u new", "u set"]),
                ("SET v v PX 10", &["v new", "v set", "v expire"]),
                ("+20", &[]),
                ("SCAN 0 MATCH v", &["v expired"]),
                // the databases as a whole say nothing
                ("FLUSHDB", &[]),
                ("SWAPDB 0 2", &[]),
                ("FLUSHALL", &[]),
            ],
        );
    }

    #[test]
    fn transactions_and_scripts_raise_the_events_of_their_commands() {
        let mut h = listening("KA");
        check(
            &mut h,
            &[
                ("MULTI", &[]),
                ("SET a 1", &[]),
                ("INCR a", &[]),
                ("DEL a", &[]),
                ("EXEC", &["a set", "a incrby", "a del"]),
            ],
        );
        h.cmd(&[
            "EVAL",
            "redis.call('LPUSH', KEYS[1], 'x'); return redis.call('RPOP', KEYS[1])",
            "1",
            "l",
        ]);
        assert_eq!(keyspace(&mut h), ["l lpush", "l rpop", "l del"]);
    }

    #[test]
    fn restore_is_a_new_key_even_when_it_replaces_one() {
        let mut h = listening("KAn");
        h.run("SET k v");
        let Frame::Bulk(payload) = h.run("DUMP k") else {
            panic!("DUMP");
        };
        keyspace(&mut h);
        let restore = |h: &mut Harness, ttl: &str, extra: &[&str]| {
            let mut args = vec![
                Bytes::from_static(b"RESTORE"),
                Bytes::from_static(b"k"),
                Bytes::copy_from_slice(ttl.as_bytes()),
                payload.clone(),
            ];
            args.extend(
                extra
                    .iter()
                    .map(|arg| Bytes::copy_from_slice(arg.as_bytes())),
            );
            h.raw(&args)
        };
        assert!(restore(&mut h, "0", &[]).is_error());
        assert!(keyspace(&mut h).is_empty());
        assert_eq!(restore(&mut h, "0", &["REPLACE"]), Frame::ok());
        assert_eq!(keyspace(&mut h), ["k new", "k restore"]);
        // Already expired: the key it replaces is deleted.
        assert_eq!(restore(&mut h, "1", &["REPLACE", "ABSTTL"]), Frame::ok());
        assert_eq!(keyspace(&mut h), ["k del"]);
        assert_eq!(restore(&mut h, "1", &["ABSTTL"]), Frame::ok());
        assert!(keyspace(&mut h).is_empty());
    }

    #[test]
    fn eviction_raises_evicted() {
        let mut h = listening("KA");
        h.run("SET victim v EX 100");
        h.run("SET keeper v");
        keyspace(&mut h);
        h.run("CONFIG SET maxmemory-policy volatile-ttl");
        h.run("CONFIG SET maxmemory 1");
        // Every command first makes room, if it can.
        h.run("PING");
        assert_eq!(keyspace(&mut h), ["victim evicted"]);
        h.run("CONFIG SET maxmemory 0");
    }

    #[test]
    fn expired_keys_are_announced_in_order_and_only_when_asked_for() {
        let mut h = listening("KEx");
        h.run("SET a 1 PX 10");
        h.run("SET b 1 PX 10");
        h.advance(20);
        // Found by a lookup: announced with the command.
        h.run("EXISTS a");
        assert_eq!(keyspace(&mut h), ["a expired"]);
        // Found by the periodic cycle: announced when `flush` runs.
        assert_eq!(h.keyspace.cron(h.now, 100), 1);
        flush(&mut h.server, &mut h.keyspace, &Peers::none(&h.clients));
        assert_eq!(keyspace(&mut h), ["b expired"]);
        // Without `x` nothing is noted, so nothing piles up.
        h.run("CONFIG SET notify-keyspace-events KEg");
        h.run("SET c 1 PX 10");
        h.advance(20);
        h.run("GET c");
        assert!(h.keyspace.take_notes().is_empty());
    }
}
