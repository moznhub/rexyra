//! Connection-group commands: PING, ECHO, HELLO, SELECT, QUIT, CLIENT.

use bytes::Bytes;
use rexyra_resp::{parse_i64, Frame, Protocol};

use crate::{clients, is, lossy, unknown_subcommand, wrong_subcommand_arity, Ctx, COMPAT_VERSION};

pub(crate) fn ping(_ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    match args {
        [_] => Frame::simple("PONG"),
        [_, message] => Frame::Bulk(message.clone()),
        _ => Frame::error("ERR wrong number of arguments for 'ping' command"),
    }
}

pub(crate) fn echo(_ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    Frame::Bulk(args[1].clone())
}

pub(crate) fn select(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let Some(index) = parse_i64(&args[1]) else {
        return Frame::error("ERR value is not an integer or out of range");
    };
    if index < 0 || index as u64 >= ctx.server.databases as u64 {
        return Frame::error("ERR DB index is out of range");
    }
    ctx.client.db = index as usize;
    Frame::ok()
}

pub(crate) fn quit(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    ctx.client.close_after_reply = true;
    Frame::ok()
}

/// `HELLO [protover [AUTH username password] [SETNAME clientname]]`
pub(crate) fn hello(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let mut proto = ctx.client.proto;
    let mut name = None;
    let mut login = None;

    if let Some(version) = args.get(1) {
        proto = match parse_i64(version) {
            Some(2) => Protocol::Resp2,
            Some(3) => Protocol::Resp3,
            Some(_) => return Frame::error("NOPROTO unsupported protocol version"),
            None => return Frame::error("ERR Protocol version is not an integer or out of range"),
        };
    }

    let mut options = args.get(2..).unwrap_or_default();
    loop {
        match options {
            [] => break,
            [opt, username, password, rest @ ..] if is(opt, "AUTH") => {
                login = Some((username, password));
                options = rest;
            }
            [opt, value, rest @ ..] if is(opt, "SETNAME") => {
                if let Err(e) = validate_client_name(value) {
                    return e;
                }
                name = Some(value.clone());
                options = rest;
            }
            [opt, ..] => {
                return Frame::error(format!("ERR Syntax error in HELLO option '{}'", lossy(opt)))
            }
        }
    }

    // Nothing is applied unless the whole command is valid.
    match login {
        Some((username, password)) => {
            if let Err(error) = crate::acl::authenticate(ctx, username, password) {
                return error;
            }
        }
        None if !ctx.client.authenticated => {
            return Frame::error(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise \
                 the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the \
                 client and select the RESP protocol version at the same time",
            );
        }
        None => {}
    }
    ctx.client.proto = proto;
    if let Some(name) = name {
        ctx.client.name = (!name.is_empty()).then_some(name);
    }

    Frame::Map(vec![
        (Frame::bulk("server"), Frame::bulk("redis")),
        (Frame::bulk("version"), Frame::bulk(COMPAT_VERSION)),
        (
            Frame::bulk("proto"),
            Frame::Integer(match proto {
                Protocol::Resp2 => 2,
                Protocol::Resp3 => 3,
            }),
        ),
        (Frame::bulk("id"), Frame::Integer(ctx.client.id as i64)),
        (Frame::bulk("mode"), Frame::bulk("standalone")),
        (Frame::bulk("role"), Frame::bulk("master")),
        (Frame::bulk("modules"), Frame::Array(Vec::new())),
    ])
}

/// `CLIENT ID | GETNAME | SETNAME name | SETINFO <LIB-NAME|LIB-VER> value`,
/// plus the subcommands in [`clients`].
pub(crate) fn client(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let sub = &args[1];
    if is(sub, "ID") {
        if args.len() != 2 {
            return wrong_subcommand_arity("client", sub);
        }
        Frame::Integer(ctx.client.id as i64)
    } else if is(sub, "GETNAME") {
        if args.len() != 2 {
            return wrong_subcommand_arity("client", sub);
        }
        ctx.client.name.clone().map_or(Frame::Null, Frame::Bulk)
    } else if is(sub, "SETNAME") {
        if args.len() != 3 {
            return wrong_subcommand_arity("client", sub);
        }
        if let Err(e) = validate_client_name(&args[2]) {
            return e;
        }
        // An empty name clears it.
        ctx.client.name = (!args[2].is_empty()).then(|| args[2].clone());
        Frame::ok()
    } else if is(sub, "SETINFO") {
        if args.len() != 4 {
            return wrong_subcommand_arity("client", sub);
        }
        let (attr, value) = (&args[2], &args[3]);
        if value.iter().any(|&b| !(b'!'..=b'~').contains(&b)) {
            return Frame::error(format!(
                "ERR {} cannot contain spaces, newlines or special characters.",
                lossy(attr)
            ));
        }
        let slot = if is(attr, "LIB-NAME") {
            &mut ctx.client.lib_name
        } else if is(attr, "LIB-VER") {
            &mut ctx.client.lib_ver
        } else {
            return Frame::error(format!("ERR Unrecognized option '{}'", lossy(attr)));
        };
        *slot = (!value.is_empty()).then(|| value.clone());
        Frame::ok()
    } else if is(sub, "LIST") {
        clients::list(ctx, args)
    } else if is(sub, "INFO") {
        clients::info(ctx, args)
    } else if is(sub, "KILL") {
        if args.len() < 3 {
            return wrong_subcommand_arity("client", sub);
        }
        clients::kill(ctx, args)
    } else if is(sub, "UNBLOCK") {
        clients::unblock(ctx, args)
    } else if is(sub, "NO-EVICT") || is(sub, "NO-TOUCH") {
        clients::switch(ctx, args)
    } else {
        unknown_subcommand("CLIENT", sub)
    }
}

fn validate_client_name(name: &[u8]) -> Result<(), Frame> {
    if name.iter().all(|b| (b'!'..=b'~').contains(b)) {
        Ok(())
    } else {
        Err(Frame::error(
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{one, run};
    use rexyra_resp::{Frame, Protocol};

    #[test]
    fn ping() {
        assert_eq!(one(&["PING"]), Frame::simple("PONG"));
        assert_eq!(one(&["ping", "hello"]), Frame::bulk("hello"));
        assert_eq!(
            one(&["PING", "a", "b"]),
            Frame::error("ERR wrong number of arguments for 'ping' command")
        );
    }

    #[test]
    fn echo() {
        assert_eq!(one(&["ECHO", "hi"]), Frame::bulk("hi"));
    }

    #[test]
    fn select() {
        let (replies, client) = run(&[&["SELECT", "3"]]);
        assert_eq!(replies[0], Frame::ok());
        assert_eq!(client.db, 3);

        assert_eq!(
            one(&["SELECT", "16"]),
            Frame::error("ERR DB index is out of range")
        );
        assert_eq!(
            one(&["SELECT", "-1"]),
            Frame::error("ERR DB index is out of range")
        );
        assert_eq!(
            one(&["SELECT", "x"]),
            Frame::error("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn quit_marks_connection_for_close() {
        let (replies, client) = run(&[&["QUIT"]]);
        assert_eq!(replies[0], Frame::ok());
        assert!(client.close_after_reply);
    }

    fn field<'a>(reply: &'a Frame, key: &str) -> &'a Frame {
        let Frame::Map(pairs) = reply else {
            panic!("expected map, got {reply:?}");
        };
        pairs
            .iter()
            .find(|(k, _)| *k == Frame::bulk(key.to_owned()))
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("no field {key}"))
    }

    #[test]
    fn hello_without_version_keeps_protocol() {
        let (replies, client) = run(&[&["HELLO"]]);
        assert_eq!(field(&replies[0], "proto"), &Frame::Integer(2));
        assert_eq!(field(&replies[0], "id"), &Frame::Integer(7));
        assert_eq!(field(&replies[0], "mode"), &Frame::bulk("standalone"));
        assert_eq!(client.proto, Protocol::Resp2);
    }

    #[test]
    fn hello_switches_protocol() {
        let (replies, client) = run(&[&["HELLO", "3"]]);
        assert_eq!(field(&replies[0], "proto"), &Frame::Integer(3));
        assert_eq!(client.proto, Protocol::Resp3);

        let (_, client) = run(&[&["HELLO", "3"], &["HELLO", "2"]]);
        assert_eq!(client.proto, Protocol::Resp2);
    }

    #[test]
    fn hello_rejects_bad_versions() {
        assert_eq!(
            one(&["HELLO", "4"]),
            Frame::error("NOPROTO unsupported protocol version")
        );
        assert_eq!(
            one(&["HELLO", "abc"]),
            Frame::error("ERR Protocol version is not an integer or out of range")
        );
    }

    #[test]
    fn hello_options() {
        let (replies, client) = run(&[&[
            "HELLO", "3", "AUTH", "default", "secret", "SETNAME", "worker",
        ]]);
        assert!(!replies[0].is_error());
        assert_eq!(client.proto, Protocol::Resp3);
        assert_eq!(client.name.as_deref(), Some(&b"worker"[..]));
    }

    #[test]
    fn hello_failure_changes_nothing() {
        let (replies, client) = run(&[&["HELLO", "3", "SETNAME", "ok", "BOGUS"]]);
        assert_eq!(
            replies[0],
            Frame::error("ERR Syntax error in HELLO option 'BOGUS'")
        );
        assert_eq!(client.proto, Protocol::Resp2);
        assert_eq!(client.name, None);

        let (replies, client) = run(&[&["HELLO", "3", "AUTH", "alice", "pw"]]);
        assert!(matches!(&replies[0], Frame::Error(e) if e.starts_with("WRONGPASS")));
        assert_eq!(client.proto, Protocol::Resp2);
    }

    #[test]
    fn client_name_round_trip() {
        let (replies, _) = run(&[
            &["CLIENT", "GETNAME"],
            &["CLIENT", "SETNAME", "app"],
            &["CLIENT", "GETNAME"],
            &["CLIENT", "SETNAME", ""],
            &["CLIENT", "GETNAME"],
        ]);
        assert_eq!(
            replies,
            vec![
                Frame::Null,
                Frame::ok(),
                Frame::bulk("app"),
                Frame::ok(),
                Frame::Null
            ]
        );
        assert!(one(&["CLIENT", "SETNAME", "has space"]).is_error());
    }

    #[test]
    fn client_id_and_setinfo() {
        assert_eq!(one(&["CLIENT", "ID"]), Frame::Integer(7));

        let (replies, client) = run(&[
            &["CLIENT", "SETINFO", "LIB-NAME", "redis-rs"],
            &["CLIENT", "SETINFO", "lib-ver", "1.0"],
        ]);
        assert_eq!(replies, vec![Frame::ok(), Frame::ok()]);
        assert_eq!(client.lib_name.as_deref(), Some(&b"redis-rs"[..]));
        assert_eq!(client.lib_ver.as_deref(), Some(&b"1.0"[..]));

        assert_eq!(
            one(&["CLIENT", "SETINFO", "bogus", "x"]),
            Frame::error("ERR Unrecognized option 'bogus'")
        );
    }

    #[test]
    fn client_subcommand_errors() {
        assert_eq!(
            one(&["CLIENT", "NOPE"]),
            Frame::error("ERR unknown subcommand 'NOPE'. Try CLIENT HELP.")
        );
        assert_eq!(
            one(&["CLIENT", "ID", "extra"]),
            Frame::error("ERR wrong number of arguments for 'client|id' command")
        );
    }
}
