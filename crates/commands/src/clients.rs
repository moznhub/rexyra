//! Commands about other connections: `CLIENT LIST`, `INFO`, `KILL`,
//! `UNBLOCK`, `NO-EVICT`, `NO-TOUCH`, and `RESET`.

use std::fmt::Write as _;

use bytes::Bytes;
use rexyra_resp::{Frame, Protocol};

use crate::tracking::{redirect_of, ClientTracking};
use crate::{
    int, is, lossy, reply, unwatch_all, wrong_subcommand_arity, ClientState, Ctx, Effect,
    ERR_SYNTAX,
};

/// One line of `CLIENT LIST`, given the client's tracking state. Fields
/// this server has no figure for (socket and input buffer sizes) are
/// reported as zero.
fn describe(client: &ClientState, tracking: Option<&ClientTracking>, now: u64, out: &mut String) {
    let mut flags = String::new();
    for (set, flag) in [
        (client.subscriptions.any(), 'P'),
        (client.blocked, 'b'),
        (client.multi.is_some(), 'x'),
        (tracking.is_some(), 't'),
        (tracking.is_some_and(|state| state.broken), 'R'),
        (tracking.is_some_and(|state| state.bcast), 'B'),
        (client.no_evict, 'e'),
        (client.no_touch, 'T'),
    ] {
        if set {
            flags.push(flag);
        }
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let seconds_since = |then: u64| now.saturating_sub(then) / 1000;
    let omem = client
        .output_queued
        .load(std::sync::atomic::Ordering::Relaxed);
    let text = |value: &Option<Bytes>| value.as_deref().map(lossy).unwrap_or_default().into_owned();
    // Writing to a `String` cannot fail.
    let _ = writeln!(
        out,
        "id={} addr={} laddr={} fd=0 name={} age={} idle={} flags={flags} db={} sub={} psub={} \
         ssub={} multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 \
         oll=0 omem={omem} tot-mem={omem} events=r cmd={} user={} redir={} resp={} lib-name={} \
         lib-ver={}",
        client.id,
        client.addr,
        client.laddr,
        text(&client.name),
        seconds_since(client.connected_at),
        seconds_since(client.last_active),
        client.db,
        client.subscriptions.channels.len(),
        client.subscriptions.patterns.len(),
        client.subscriptions.shard.len(),
        client.multi.as_ref().map_or(-1, |tx| tx.len() as i64),
        client.watches.len(),
        client.last_command,
        client.user,
        redirect_of(tracking),
        match client.proto {
            Protocol::Resp2 => 2,
            Protocol::Resp3 => 3,
        },
        text(&client.lib_name),
        text(&client.lib_ver),
    );
}

fn verbatim(text: String) -> Frame {
    Frame::Verbatim {
        format: *b"txt",
        data: Bytes::from(text),
    }
}

/// Every connected client, the caller included, in order of id.
fn all_clients<'a>(ctx: &'a Ctx<'_>) -> Vec<&'a ClientState> {
    let mut clients: Vec<&ClientState> = ctx
        .clients
        .values()
        .chain(std::iter::once(&*ctx.client))
        .collect();
    clients.sort_by_key(|client| client.id);
    clients
}

/// The kinds of client a `TYPE` filter can name. This server has no
/// replication, so there are never any of the others (`master`, `replica`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ClientType {
    Normal,
    PubSub,
    Other,
}

impl ClientType {
    /// A client subscribed to anything counts as `pubsub`, as in Redis.
    fn of(client: &ClientState) -> ClientType {
        if client.subscriptions.any() {
            ClientType::PubSub
        } else {
            ClientType::Normal
        }
    }

    fn parse(kind: &[u8]) -> Result<ClientType, Frame> {
        if is(kind, "normal") {
            Ok(ClientType::Normal)
        } else if is(kind, "pubsub") {
            Ok(ClientType::PubSub)
        } else if ["master", "replica", "slave"]
            .iter()
            .any(|other| is(kind, other))
        {
            Ok(ClientType::Other)
        } else {
            Err(Frame::error(format!(
                "ERR Unknown client type '{}'",
                lossy(kind)
            )))
        }
    }
}

/// `CLIENT LIST [TYPE type] [ID id ...]`
pub(crate) fn list(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let mut ids: Option<Vec<u64>> = None;
        let mut wanted = None;
        match &args[2..] {
            [] => {}
            [opt, kind] if is(opt, "TYPE") => wanted = Some(ClientType::parse(kind)?),
            [opt, rest @ ..] if is(opt, "ID") && !rest.is_empty() => {
                let parsed: Result<Vec<u64>, Frame> = rest
                    .iter()
                    .map(|id| match int(id) {
                        // An id no client can have simply matches nothing.
                        Ok(id) => Ok(u64::try_from(id).unwrap_or(0)),
                        Err(_) => Err(Frame::error("ERR Invalid client ID")),
                    })
                    .collect();
                ids = Some(parsed?);
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
        ctx.client.last_command = "client|list";
        let mut out = String::new();
        for client in all_clients(ctx) {
            if ids.as_ref().is_none_or(|ids| ids.contains(&client.id))
                && wanted.is_none_or(|kind| kind == ClientType::of(client))
            {
                describe(
                    client,
                    ctx.server.tracking.client(client.id),
                    ctx.now,
                    &mut out,
                );
            }
        }
        Ok(verbatim(out))
    })())
}

/// `CLIENT INFO` — the caller's own line of `CLIENT LIST`.
pub(crate) fn info(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len() != 2 {
        return wrong_subcommand_arity("client", &args[1]);
    }
    ctx.client.last_command = "client|info";
    let mut out = String::new();
    let tracking = ctx.server.tracking.client(ctx.client.id);
    describe(ctx.client, tracking, ctx.now, &mut out);
    verbatim(out)
}

/// What `CLIENT KILL` selects by; every given filter must match.
#[derive(Default)]
struct KillFilter<'a> {
    id: Option<u64>,
    addr: Option<&'a [u8]>,
    laddr: Option<&'a [u8]>,
    /// Kill clients at least this many seconds old.
    max_age: Option<u64>,
    user: Option<&'a [u8]>,
    kind: Option<ClientType>,
    skip_me: bool,
}

impl KillFilter<'_> {
    fn matches(&self, client: &ClientState, now: u64) -> bool {
        self.kind.is_none_or(|kind| kind == ClientType::of(client))
            && self.id.is_none_or(|id| id == client.id)
            && self.user.is_none_or(|user| user == client.user.as_bytes())
            && self.addr.is_none_or(|addr| addr == client.addr.as_bytes())
            && self
                .laddr
                .is_none_or(|laddr| laddr == client.laddr.as_bytes())
            && self
                .max_age
                .is_none_or(|age| now.saturating_sub(client.connected_at) / 1000 >= age)
    }
}

fn parse_kill_filter(mut options: &[Bytes]) -> Result<KillFilter<'_>, Frame> {
    let mut filter = KillFilter {
        skip_me: true,
        ..KillFilter::default()
    };
    while let [option, value, rest @ ..] = options {
        if is(option, "ID") {
            filter.id = Some(match int(value) {
                Ok(id) if id > 0 => id as u64,
                _ => return Err(Frame::error("ERR client-id should be greater than 0")),
            });
        } else if is(option, "ADDR") {
            filter.addr = Some(value);
        } else if is(option, "LADDR") {
            filter.laddr = Some(value);
        } else if is(option, "TYPE") {
            filter.kind = Some(ClientType::parse(value)?);
        } else if is(option, "USER") {
            filter.user = Some(value);
        } else if is(option, "SKIPME") {
            filter.skip_me = if is(value, "yes") {
                true
            } else if is(value, "no") {
                false
            } else {
                return Err(Frame::error(ERR_SYNTAX));
            };
        } else if is(option, "MAXAGE") {
            filter.max_age = Some(match int(value) {
                Ok(age) if age > 0 => age as u64,
                Ok(_) => return Err(Frame::error("ERR maxage should be greater than 0")),
                Err(_) => return Err(Frame::error("ERR maxage is not an integer or out of range")),
            });
        } else {
            return Err(Frame::error(ERR_SYNTAX));
        }
        options = rest;
    }
    if options.is_empty() {
        Ok(filter)
    } else {
        Err(Frame::error(ERR_SYNTAX))
    }
}

/// `CLIENT KILL addr:port` (replies `OK`, or an error if there is no such
/// client) or `CLIENT KILL <filter> ...` (replies with the number killed).
pub(crate) fn kill(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let old_form = args.len() == 3;
        let filter = if old_form {
            KillFilter {
                addr: Some(&args[2]),
                ..KillFilter::default()
            }
        } else {
            parse_kill_filter(&args[2..])?
        };
        if let Some(user) = filter.user {
            if ctx.server.acl.user(&lossy(user)).is_none() {
                return Err(Frame::error(format!("ERR No such user '{}'", lossy(user))));
            }
        }

        let now = ctx.now;
        let victims: Vec<u64> = ctx
            .clients
            .values()
            .filter(|client| filter.matches(client, now))
            .map(|client| client.id)
            .collect();
        let mut killed = victims.len();
        ctx.server
            .effects
            .extend(victims.into_iter().map(Effect::Kill));
        // The caller still receives this reply, then its connection closes.
        if !filter.skip_me && filter.matches(ctx.client, now) {
            ctx.client.close_after_reply = true;
            killed += 1;
        }

        if !old_form {
            Ok(Frame::Integer(killed as i64))
        } else if killed == 0 {
            Err(Frame::error("ERR No such client"))
        } else {
            Ok(Frame::ok())
        }
    })())
}

/// `CLIENT UNBLOCK id [TIMEOUT | ERROR]` — ends another client's blocking
/// command. Replies 1 if the client was blocked.
pub(crate) fn unblock(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (id, error) = match &args[2..] {
            [id] => (id, false),
            [id, reason] if is(reason, "TIMEOUT") => (id, false),
            [id, reason] if is(reason, "ERROR") => (id, true),
            [_, _] => {
                return Err(Frame::error(
                    "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR",
                ))
            }
            [] => return Err(wrong_subcommand_arity("client", &args[1])),
            _ => {
                return Err(Frame::error(
                    "ERR unknown subcommand or wrong number of arguments for 'UNBLOCK'. \
                     Try CLIENT HELP.",
                ))
            }
        };
        let id = u64::try_from(int(id)?).unwrap_or(0);
        let blocked = ctx.clients.get(&id).is_some_and(|client| client.blocked);
        if blocked {
            ctx.server.effects.push(Effect::Unblock { id, error });
        }
        Ok(Frame::Integer(i64::from(blocked)))
    })())
}

/// `CLIENT NO-EVICT <ON | OFF>` and `CLIENT NO-TOUCH <ON | OFF>`. The flags
/// are recorded now and take effect when eviction arrives.
pub(crate) fn switch(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let [_, sub, value] = args else {
        return wrong_subcommand_arity("client", &args[1]);
    };
    let on = if is(value, "on") {
        true
    } else if is(value, "off") {
        false
    } else {
        return Frame::error(ERR_SYNTAX);
    };
    if is(sub, "NO-EVICT") {
        ctx.client.no_evict = on;
    } else {
        ctx.client.no_touch = on;
    }
    Frame::ok()
}

/// `RESET` — returns the connection to the state of a new one.
pub(crate) fn reset(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    ctx.client.multi = None;
    unwatch_all(ctx.client, ctx.keyspace);
    ctx.client.db = 0;
    ctx.client.proto = Protocol::Resp2;
    ctx.client.name = None;
    ctx.client.no_evict = false;
    ctx.client.no_touch = false;
    crate::pubsub::forget(ctx.server, ctx.client);
    crate::tracking::forget(ctx.server, ctx.client.id);
    crate::acl::start_session(ctx.server, ctx.client);
    Frame::simple("RESET")
}

#[cfg(test)]
mod tests {
    use crate::testing::{bulk, err, Harness};
    use crate::{ClientState, Effect};
    use rexyra_resp::{Frame, Protocol};

    fn text(frame: Frame) -> String {
        match frame {
            Frame::Verbatim { data, .. } => String::from_utf8(data.to_vec()).unwrap(),
            other => panic!("not text: {other:?}"),
        }
    }

    /// A harness with two more clients connected: 8 (blocked) and 9.
    fn three_clients() -> Harness {
        let mut h = Harness::new();
        let mut blocked = ClientState::new(8, "10.0.0.8:1000".to_owned());
        blocked.laddr = "10.0.0.1:6379".to_owned();
        blocked.blocked = true;
        blocked.connected_at = h.now - 90_000;
        blocked.last_active = h.now - 30_000;
        let mut other = ClientState::new(9, "10.0.0.9:1000".to_owned());
        other.laddr = "10.0.0.1:6379".to_owned();
        other.connected_at = h.now;
        other.last_active = h.now;
        h.clients.insert(8, blocked);
        h.clients.insert(9, other);
        h.client.connected_at = h.now - 5_000;
        h
    }

    #[test]
    fn client_info_describes_the_caller() {
        let mut h = Harness::new();
        h.client.connected_at = h.now;
        h.client.laddr = "127.0.0.1:6379".to_owned();
        h.run("CLIENT SETNAME app");
        h.run("CLIENT SETINFO LIB-NAME mylib");
        h.run("SELECT 2");
        h.run("WATCH a b");
        h.advance(3_000);
        assert_eq!(
            text(h.run("CLIENT INFO")),
            "id=7 addr=127.0.0.1:50000 laddr=127.0.0.1:6379 fd=0 name=app age=3 idle=0 flags=N \
             db=2 sub=0 psub=0 ssub=0 multi=-1 watch=2 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 \
             rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client|info user=default \
             redir=-1 resp=2 lib-name=mylib lib-ver=\n"
        );
    }

    #[test]
    fn client_list() {
        let mut h = three_clients();
        let list = text(h.run("CLIENT LIST"));
        let lines: Vec<&str> = list.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("id=7 addr=127.0.0.1:50000 "));
        assert!(lines[0].contains(" age=5 idle=0 flags=N "));
        assert!(lines[0].contains(" cmd=client|list "));
        assert!(lines[1].starts_with("id=8 addr=10.0.0.8:1000 laddr=10.0.0.1:6379 "));
        assert!(lines[1].contains(" age=90 idle=30 flags=b "));
        assert!(lines[2].starts_with("id=9 "));
        assert!(list.ends_with('\n'));

        assert_eq!(text(h.run("CLIENT LIST ID 9 7 55")).lines().count(), 2);
        assert_eq!(text(h.run("CLIENT LIST TYPE normal")).lines().count(), 3);
        assert_eq!(text(h.run("CLIENT LIST TYPE pubsub")), "");
        assert_eq!(
            h.run("CLIENT LIST TYPE nope"),
            err("ERR Unknown client type 'nope'")
        );
        assert_eq!(h.run("CLIENT LIST ID x"), err("ERR Invalid client ID"));
        assert_eq!(text(h.run("CLIENT LIST ID 0 -4")), "");
        assert_eq!(h.run("CLIENT LIST BOGUS"), err("ERR syntax error"));
        assert_eq!(h.run("CLIENT LIST ID"), err("ERR syntax error"));
    }

    #[test]
    fn client_kill_by_address() {
        let mut h = three_clients();
        assert_eq!(h.run("CLIENT KILL 10.0.0.9:1000"), Frame::ok());
        assert_eq!(h.server.effects, [Effect::Kill(9)]);
        assert_eq!(h.run("CLIENT KILL 10.0.0.77:1"), err("ERR No such client"));
        // The old form can kill the caller, which still gets its reply.
        assert_eq!(h.run("CLIENT KILL 127.0.0.1:50000"), Frame::ok());
        assert!(h.client.close_after_reply);
    }

    #[test]
    fn client_kill_by_filter() {
        let kill = |command: &str| {
            let mut h = three_clients();
            let reply = h.run(command);
            let mut killed: Vec<u64> = h
                .server
                .effects
                .iter()
                .map(|effect| match effect {
                    Effect::Kill(id) => *id,
                    other => panic!("{other:?}"),
                })
                .collect();
            killed.sort_unstable();
            (reply, killed, h.client.close_after_reply)
        };
        let n = Frame::Integer;
        assert_eq!(kill("CLIENT KILL ID 8"), (n(1), vec![8], false));
        assert_eq!(kill("CLIENT KILL ID 55"), (n(0), vec![], false));
        assert_eq!(kill("CLIENT KILL ID 7"), (n(0), vec![], false));
        assert_eq!(kill("CLIENT KILL ID 7 SKIPME no"), (n(1), vec![], true));
        assert_eq!(kill("CLIENT KILL TYPE normal"), (n(2), vec![8, 9], false));
        assert_eq!(
            kill("CLIENT KILL TYPE normal SKIPME no"),
            (n(3), vec![8, 9], true)
        );
        assert_eq!(kill("CLIENT KILL TYPE pubsub"), (n(0), vec![], false));
        assert_eq!(
            kill("CLIENT KILL LADDR 10.0.0.1:6379"),
            (n(2), vec![8, 9], false)
        );
        assert_eq!(
            kill("CLIENT KILL LADDR 10.0.0.1:6379 ADDR 10.0.0.8:1000"),
            (n(1), vec![8], false)
        );
        assert_eq!(kill("CLIENT KILL MAXAGE 60"), (n(1), vec![8], false));
        assert_eq!(kill("CLIENT KILL USER default"), (n(2), vec![8, 9], false));

        for (command, message) in [
            ("CLIENT KILL USER nobody", "ERR No such user 'nobody'"),
            ("CLIENT KILL TYPE nope", "ERR Unknown client type 'nope'"),
            ("CLIENT KILL ID 0", "ERR client-id should be greater than 0"),
            ("CLIENT KILL ID x", "ERR client-id should be greater than 0"),
            ("CLIENT KILL SKIPME maybe", "ERR syntax error"),
            ("CLIENT KILL BOGUS 1", "ERR syntax error"),
            ("CLIENT KILL ID 8 ADDR", "ERR syntax error"),
            (
                "CLIENT KILL MAXAGE 0",
                "ERR maxage should be greater than 0",
            ),
            (
                "CLIENT KILL MAXAGE x",
                "ERR maxage is not an integer or out of range",
            ),
        ] {
            let (reply, killed, _) = kill(command);
            assert_eq!(reply, err(message), "{command}");
            assert!(killed.is_empty(), "{command}");
        }
    }

    #[test]
    fn client_unblock() {
        let mut h = three_clients();
        assert_eq!(h.run("CLIENT UNBLOCK 8"), Frame::Integer(1));
        assert_eq!(h.run("CLIENT UNBLOCK 8 error"), Frame::Integer(1));
        assert_eq!(
            h.server.effects,
            [
                Effect::Unblock {
                    id: 8,
                    error: false
                },
                Effect::Unblock { id: 8, error: true }
            ]
        );
        h.server.effects.clear();
        // Not blocked, unknown, and the caller itself.
        for id in ["9", "55", "7", "-3"] {
            assert_eq!(h.cmd(&["CLIENT", "UNBLOCK", id]), Frame::Integer(0));
        }
        assert!(h.server.effects.is_empty());
        assert_eq!(
            h.run("CLIENT UNBLOCK 8 NOW"),
            err("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR")
        );
        assert_eq!(
            h.run("CLIENT UNBLOCK x"),
            err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            h.run("CLIENT UNBLOCK"),
            err("ERR wrong number of arguments for 'client|unblock' command")
        );
    }

    #[test]
    fn client_switches() {
        let mut h = Harness::new();
        assert_eq!(h.run("CLIENT NO-EVICT on"), Frame::ok());
        assert_eq!(h.run("CLIENT NO-TOUCH ON"), Frame::ok());
        assert!(text(h.run("CLIENT INFO")).contains(" flags=eT "));
        assert_eq!(h.run("CLIENT NO-EVICT off"), Frame::ok());
        assert!(text(h.run("CLIENT INFO")).contains(" flags=T "));
        h.run("CLIENT NO-TOUCH off");
        h.run("MULTI");
        h.run("SET a 1");
        h.run("SET b 1");
        h.client.multi.as_mut().unwrap();
        let mut line = String::new();
        super::describe(&h.client, None, h.now, &mut line);
        assert!(line.contains(" flags=x "), "{line}");
        assert!(line.contains(" multi=2 "), "{line}");
        h.run("DISCARD");
        assert_eq!(h.run("CLIENT NO-TOUCH maybe"), err("ERR syntax error"));
        assert_eq!(
            h.run("CLIENT NO-EVICT"),
            err("ERR wrong number of arguments for 'client|no-evict' command")
        );
    }

    #[test]
    fn reset_restores_a_fresh_connection() {
        let mut h = Harness::new();
        h.run("HELLO 3 SETNAME app");
        h.run("SELECT 5");
        h.run("WATCH k");
        h.run("MULTI");
        h.run("SET k 1");
        assert_eq!(h.run("RESET"), Frame::simple("RESET"));
        assert_eq!(h.client.db, 0);
        assert_eq!(h.client.proto, Protocol::Resp2);
        assert_eq!(h.client.name, None);
        assert!(h.client.multi.is_none());
        assert!(h.client.watches.is_empty());
        assert_eq!(h.run("EXEC"), err("ERR EXEC without MULTI"));
        assert_eq!(h.run("GET k"), Frame::Null);
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
        assert_eq!(h.run("ECHO hi"), bulk("hi"));
    }
}
