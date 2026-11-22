//! Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`.
//!
//! After `MULTI`, [`crate::dispatch`] queues commands instead of running
//! them. `EXEC` runs the queue in one go; because the server executes
//! commands on a single thread, nothing else can run in between.
//!
//! `WATCH` makes `EXEC` conditional: each watched key's version is recorded
//! (see [`rexyra_engine::Db::watch`]) and `EXEC` runs nothing if any of them has
//! moved on.

use bytes::Bytes;
use rexyra_engine::Keyspace;
use rexyra_resp::Frame;

use crate::{copied, lookup, ClientState, Ctx};

/// Commands queued by a client since `MULTI`.
#[derive(Debug, Default)]
pub struct Transaction {
    queued: Vec<Vec<Bytes>>,
    /// A command was rejected while queueing, so `EXEC` must refuse to run.
    aborted: bool,
}

impl Transaction {
    pub(crate) fn queue(&mut self, args: &[Bytes]) {
        self.queued.push(args.to_vec());
    }

    /// Number of commands queued so far.
    pub fn len(&self) -> usize {
        self.queued.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub(crate) fn abort(&mut self) {
        self.aborted = true;
    }
}

/// A key watched by a client, with the version it had at the time.
#[derive(Debug)]
pub struct Watch {
    db: usize,
    key: Bytes,
    version: u64,
}

/// Whether `spec_name` runs immediately even inside `MULTI`.
pub(crate) fn runs_inside_multi(spec_name: &str) -> bool {
    matches!(
        spec_name,
        "exec" | "discard" | "multi" | "watch" | "quit" | "reset"
    )
}

/// Forgets every key the client watches. Must be called before a client
/// with watches is dropped, or the keyspace keeps tracking those keys.
pub fn unwatch_all(client: &mut ClientState, keyspace: &mut Keyspace) {
    for watch in client.watches.drain(..) {
        keyspace.db(watch.db).unwatch(&watch.key);
    }
}

/// Whether any key the client watches has changed since it was watched.
fn watched_keys_changed(ctx: &mut Ctx<'_>) -> bool {
    let now = ctx.now;
    ctx.client.watches.iter().any(|watch| {
        ctx.keyspace.db(watch.db).watch_version(&watch.key, now) != Some(watch.version)
    })
}

pub(crate) fn multi(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    if ctx.client.multi.is_some() {
        return Frame::error("ERR MULTI calls can not be nested");
    }
    ctx.client.multi = Some(Transaction::default());
    Frame::ok()
}

pub(crate) fn discard(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    if ctx.client.multi.take().is_none() {
        return Frame::error("ERR DISCARD without MULTI");
    }
    unwatch_all(ctx.client, ctx.keyspace);
    Frame::ok()
}

pub(crate) fn exec(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    let Some(transaction) = ctx.client.multi.take() else {
        return Frame::error("ERR EXEC without MULTI");
    };
    let changed = watched_keys_changed(ctx);
    // Watches last until the next `EXEC`, whatever its outcome.
    unwatch_all(ctx.client, ctx.keyspace);
    if transaction.aborted {
        return Frame::error("EXECABORT Transaction discarded because of previous errors.");
    }
    if changed {
        return Frame::NullArray;
    }

    // Logged as one unit, so a log cut short never replays half of it.
    if let Some(aof) = &mut ctx.server.aof {
        aof.begin_batch();
    }
    let mut replies = Vec::with_capacity(transaction.queued.len());
    ctx.server.in_exec = true;
    for args in &transaction.queued {
        // Queueing already checked that the command exists and its arity.
        let frame = match lookup(&args[0]) {
            // Permissions may have changed since the command was queued.
            Some(spec) => match crate::acl::check(ctx, spec, args) {
                Ok(()) => crate::run(ctx, spec, args),
                Err(text) => Frame::error(text),
            },
            None => Frame::error("ERR unknown command"),
        };
        // A blocking command does not wait inside a transaction: it replies
        // at once with what it would say on timing out.
        ctx.block = None;
        replies.push(frame);
    }
    ctx.server.in_exec = false;
    if let Some(aof) = &mut ctx.server.aof {
        aof.end_batch();
    }
    Frame::Array(replies)
}

/// `EXEC` with the wrong number of arguments: the transaction, if any, is
/// thrown away, and the error says so.
pub(crate) fn reject_exec(ctx: &mut Ctx<'_>, error: Frame) -> Frame {
    ctx.client.multi = None;
    unwatch_all(ctx.client, ctx.keyspace);
    let reason = match &error {
        Frame::Error(text) => text.strip_prefix("ERR ").unwrap_or(text),
        _ => "",
    };
    Frame::error(format!(
        "EXECABORT Transaction discarded because of: {reason}"
    ))
}

pub(crate) fn watch(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if ctx.client.multi.is_some() {
        return Frame::error("ERR WATCH inside MULTI is not allowed");
    }
    let (db, now) = (ctx.client.db, ctx.now);
    for key in &args[1..] {
        let already = ctx
            .client
            .watches
            .iter()
            .any(|watch| watch.db == db && watch.key == key);
        if !already {
            let version = ctx.keyspace.db(db).watch(key, now);
            ctx.client.watches.push(Watch {
                db,
                key: copied(key),
                version,
            });
        }
    }
    Frame::ok()
}

pub(crate) fn unwatch(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    unwatch_all(ctx.client, ctx.keyspace);
    Frame::ok()
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, bulks, err, Harness};
    use crate::{ClientState, Ctx};
    use rexyra_resp::Frame;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn queued() -> Frame {
        Frame::simple("QUEUED")
    }

    /// Runs a command as a second client sharing the harness's keyspace.
    fn other(h: &mut Harness, client: &mut ClientState, line: &str) -> Frame {
        let args = crate::testing::args(&line.split_whitespace().collect::<Vec<_>>());
        let mut ctx = Ctx {
            client,
            clients: &mut h.clients,
            server: &mut h.server,
            keyspace: &mut h.keyspace,
            now: h.now,
            block: None,
        };
        crate::dispatch(&mut ctx, &args)
    }

    fn second_client() -> ClientState {
        let mut client = ClientState::new(8, "127.0.0.1:50001".to_owned());
        client.authenticated = true;
        client
    }

    #[test]
    fn commands_are_queued_and_run_by_exec() {
        let mut h = Harness::new();
        assert_eq!(h.run("MULTI"), Frame::ok());
        assert_eq!(h.run("SET k 1"), queued());
        assert_eq!(h.run("INCR k"), queued());
        assert_eq!(h.run("GET k"), queued());
        // Nothing has run yet.
        let mut b = second_client();
        assert_eq!(other(&mut h, &mut b, "EXISTS k"), int(0));
        assert_eq!(
            h.run("EXEC"),
            Frame::Array(vec![Frame::ok(), int(2), bulk("2")])
        );
        assert_eq!(h.run("GET k"), bulk("2"));
        // The transaction is over.
        assert_eq!(h.run("EXEC"), err("ERR EXEC without MULTI"));
    }

    #[test]
    fn empty_transaction_and_discard() {
        let mut h = Harness::new();
        h.run("MULTI");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![]));
        h.run("MULTI");
        assert_eq!(h.run("SET k 1"), queued());
        assert_eq!(h.run("DISCARD"), Frame::ok());
        assert_eq!(h.run("EXISTS k"), int(0));
        assert_eq!(h.run("DISCARD"), err("ERR DISCARD without MULTI"));
    }

    #[test]
    fn a_runtime_error_does_not_stop_the_rest() {
        let mut h = Harness::new();
        h.run("SET s text");
        h.run("MULTI");
        h.run("INCR s");
        h.run("SET after 1");
        assert_eq!(
            h.run("EXEC"),
            Frame::Array(vec![
                err("ERR value is not an integer or out of range"),
                Frame::ok(),
            ])
        );
        assert_eq!(h.run("GET after"), bulk("1"));
    }

    #[test]
    fn a_command_rejected_while_queueing_aborts_the_transaction() {
        let mut h = Harness::new();
        for bad in ["NOSUCHCOMMAND a", "GET"] {
            h.run("MULTI");
            assert_eq!(h.run("SET k 1"), queued());
            assert!(matches!(h.run(bad), Frame::Error(_)), "{bad}");
            assert_eq!(h.run("SET k 2"), queued());
            assert_eq!(
                h.run("EXEC"),
                err("EXECABORT Transaction discarded because of previous errors.")
            );
            assert_eq!(h.run("EXISTS k"), int(0));
            assert_eq!(h.run("EXEC"), err("ERR EXEC without MULTI"));
        }
    }

    #[test]
    fn errors_that_do_not_abort() {
        let mut h = Harness::new();
        h.run("MULTI");
        assert_eq!(h.run("MULTI"), err("ERR MULTI calls can not be nested"));
        assert_eq!(
            h.run("WATCH k"),
            err("ERR WATCH inside MULTI is not allowed")
        );
        h.run("SET k 1");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::ok()]));
    }

    #[test]
    fn watch_aborts_exec_when_another_client_writes() {
        let mut h = Harness::new();
        let mut b = second_client();
        h.run("SET k 1");
        assert_eq!(h.run("WATCH k other"), Frame::ok());
        other(&mut h, &mut b, "SET k 2");
        h.run("MULTI");
        h.run("SET k mine");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
        assert_eq!(h.run("GET k"), bulk("2"));

        // The watch ended with that EXEC.
        other(&mut h, &mut b, "SET k 3");
        h.run("MULTI");
        h.run("SET k mine");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::ok()]));
    }

    #[test]
    fn watch_notices_every_kind_of_change() {
        for change in [
            "SET k 1",
            "DEL k",
            "APPEND k x",
            "EXPIRE k 100",
            "RENAME k elsewhere",
            "RENAME src k",
            "FLUSHDB",
            "FLUSHALL",
            "LPUSH missing a",
        ] {
            let mut h = Harness::new();
            let mut b = second_client();
            h.run("SET k v");
            h.run("SET src v");
            h.run("WATCH k missing");
            other(&mut h, &mut b, change);
            h.run("MULTI");
            h.run("PING");
            assert_eq!(h.run("EXEC"), Frame::NullArray, "{change}");
        }
    }

    #[test]
    fn watch_ignores_writes_that_change_nothing() {
        for no_op in [
            "SADD set a",
            "SREM set nope",
            "SMOVE set other nope",
            "SPOP set 0",
            "HDEL hash nope",
            "HSETNX hash f other",
            "LREM list 0 nope",
            "LINSERT list BEFORE nope x",
            "LPOP list 0",
            "ZADD zset 1 a",
            "ZADD zset NX 5 a",
            "ZREM zset nope",
            "ZINCRBY zset 0 a",
            "ZREMRANGEBYSCORE zset 50 60",
            "ZPOPMIN zset 0",
            "SETRANGE str 0 ",
            "SETBIT str 1 1",
            "BITFIELD str SET u8 0 97",
            "PERSIST str",
            "EXPIRE str 100 XX",
        ] {
            let mut h = Harness::new();
            let mut b = second_client();
            h.run("SADD set a");
            h.run("HSET hash f v");
            h.run("RPUSH list a");
            h.run("ZADD zset 1 a");
            h.run("SET str a");
            h.run("WATCH set hash list zset str");
            // `SETRANGE str 0 ""` needs a real empty argument.
            let mut words: Vec<&str> = no_op.split(' ').collect();
            if words.last() == Some(&"") {
                *words.last_mut().unwrap() = "";
            }
            let args = crate::testing::args(&words);
            let mut ctx = Ctx {
                client: &mut b,
                clients: &mut h.clients,
                server: &mut h.server,
                keyspace: &mut h.keyspace,
                now: h.now,
                block: None,
            };
            let reply = crate::dispatch(&mut ctx, &args);
            assert!(!matches!(reply, Frame::Error(_)), "{no_op}: {reply:?}");
            h.run("MULTI");
            h.run("PING");
            assert_eq!(
                h.run("EXEC"),
                Frame::Array(vec![Frame::simple("PONG")]),
                "{no_op}"
            );
        }
    }

    #[test]
    fn exec_with_arguments_discards_the_transaction() {
        let mut h = Harness::new();
        h.run("WATCH k");
        h.run("MULTI");
        h.run("SET k 1");
        assert_eq!(
            h.run("EXEC now"),
            err("EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command")
        );
        assert_eq!(h.run("EXEC"), err("ERR EXEC without MULTI"));
        assert!(h.client.watches.is_empty());
        assert_eq!(h.run("EXISTS k"), int(0));
    }

    #[test]
    fn watch_ignores_reads_and_other_keys() {
        let mut h = Harness::new();
        let mut b = second_client();
        h.run("SET k v");
        h.run("WATCH k missing");
        for harmless in [
            "GET k",
            "EXISTS k missing",
            "TTL k",
            "SET unrelated 1",
            "DEL missing",
            "SELECT 1",
            "SET k other-database",
            "FLUSHDB",
        ] {
            other(&mut h, &mut b, harmless);
        }
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::simple("PONG")]));
    }

    #[test]
    fn watch_sees_expiry_but_not_a_key_that_had_already_expired() {
        let mut h = Harness::new();
        h.run("SET k v PX 100");
        h.run("WATCH k");
        h.advance(200);
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::NullArray);

        h.run("SET k v PX 100");
        h.advance(200);
        h.run("WATCH k");
        h.advance(200);
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::simple("PONG")]));
    }

    #[test]
    fn unwatch_and_discard_release_watches() {
        let mut h = Harness::new();
        let mut b = second_client();
        h.run("WATCH k");
        assert_eq!(h.run("UNWATCH"), Frame::ok());
        other(&mut h, &mut b, "SET k 1");
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::simple("PONG")]));

        h.run("WATCH k");
        h.run("MULTI");
        assert_eq!(h.run("DISCARD"), Frame::ok());
        other(&mut h, &mut b, "SET k 2");
        h.run("MULTI");
        h.run("PING");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::simple("PONG")]));
        assert!(h.client.watches.is_empty());
    }

    #[test]
    fn own_writes_inside_the_transaction_do_not_abort_it() {
        let mut h = Harness::new();
        h.run("WATCH k");
        h.run("MULTI");
        h.run("SET k 1");
        h.run("INCR k");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::ok(), int(2)]));
        // But a write before MULTI does.
        h.run("WATCH k");
        h.run("SET k 5");
        h.run("MULTI");
        h.run("INCR k");
        assert_eq!(h.run("EXEC"), Frame::NullArray);
    }

    #[test]
    fn blocking_commands_do_not_wait_inside_a_transaction() {
        let mut h = Harness::new();
        h.run("RPUSH l a");
        h.run("MULTI");
        assert_eq!(h.run("BLPOP l 0"), queued());
        assert_eq!(h.run("BLPOP l 0"), queued());
        assert_eq!(h.run("BLMOVE l m LEFT LEFT 0"), queued());
        assert_eq!(h.block, None);
        assert_eq!(
            h.run("EXEC"),
            Frame::Array(vec![
                Frame::Array(bulks(&["l", "a"])),
                Frame::NullArray,
                Frame::NullArray,
            ])
        );
        assert_eq!(h.block, None);
    }

    #[test]
    fn select_inside_a_transaction() {
        let mut h = Harness::new();
        h.run("MULTI");
        h.run("SELECT 2");
        h.run("SET k two");
        h.run("EXEC");
        assert_eq!(h.client.db, 2);
        assert_eq!(h.run("GET k"), bulk("two"));
        h.run("SELECT 0");
        assert_eq!(h.run("EXISTS k"), int(0));
    }
}
