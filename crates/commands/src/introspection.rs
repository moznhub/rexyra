//! `COMMAND` and its subcommands. Client libraries and `redis-cli` call these
//! on connect to discover what the server supports.

use bytes::Bytes;
use rexyra_resp::Frame;

use crate::keyspec::{self, BeginSearch, FindKeys, Found, KeySpec};
use crate::{is, lookup, unknown_subcommand, wrong_subcommand_arity, CommandSpec, Ctx, COMMANDS};

/// `COMMAND [COUNT | LIST | INFO [name ...] | DOCS [name ...] | GETKEYS
/// command [arg ...] | GETKEYSANDFLAGS command [arg ...]]`
pub(crate) fn command(_ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let Some(sub) = args.get(1) else {
        return Frame::Array(COMMANDS.iter().map(info).collect());
    };
    let names = &args[2..];

    if is(sub, "COUNT") {
        if !names.is_empty() {
            return wrong_subcommand_arity("command", sub);
        }
        Frame::Integer(COMMANDS.len() as i64)
    } else if is(sub, "LIST") {
        if !names.is_empty() {
            // FILTERBY is not supported yet.
            return Frame::error("ERR syntax error");
        }
        Frame::Array(COMMANDS.iter().map(|spec| Frame::bulk(spec.name)).collect())
    } else if is(sub, "INFO") {
        if names.is_empty() {
            Frame::Array(COMMANDS.iter().map(info).collect())
        } else {
            Frame::Array(
                names
                    .iter()
                    .map(|name| lookup(name).map_or(Frame::Null, info))
                    .collect(),
            )
        }
    } else if is(sub, "DOCS") {
        let selected: Vec<&CommandSpec> = if names.is_empty() {
            COMMANDS.iter().collect()
        } else {
            names.iter().filter_map(|name| lookup(name)).collect()
        };
        Frame::Map(
            selected
                .into_iter()
                .map(|spec| (Frame::bulk(spec.name), docs(spec)))
                .collect(),
        )
    } else if is(sub, "GETKEYS") {
        match keys_of(sub, names) {
            Ok(found) => Frame::Array(
                found
                    .into_iter()
                    .map(|key| Frame::Bulk(names[key.index].clone()))
                    .collect(),
            ),
            Err(error) => error,
        }
    } else if is(sub, "GETKEYSANDFLAGS") {
        match keys_of(sub, names) {
            Ok(found) => Frame::Array(
                found
                    .into_iter()
                    .map(|key| {
                        Frame::Array(vec![
                            Frame::Bulk(names[key.index].clone()),
                            flag_set(key.flags),
                        ])
                    })
                    .collect(),
            ),
            Err(error) => error,
        }
    } else {
        unknown_subcommand("COMMAND", sub)
    }
}

/// The keys of a command line for `COMMAND GETKEYS` and `GETKEYSANDFLAGS`,
/// with Redis's errors, checked in Redis's order: an unknown command or
/// subcommand, one without keys, then the wrong number of arguments, then
/// arguments that name no keys where the command needs some.
fn keys_of(sub: &Bytes, command: &[Bytes]) -> Result<Vec<Found>, Frame> {
    let Some(name) = command.first() else {
        return Err(wrong_subcommand_arity("command", sub));
    };
    let invalid_command = || Frame::error("ERR Invalid command specified");
    let spec = lookup(name).ok_or_else(invalid_command)?;
    let (arity, has_keys) = match command.get(1) {
        Some(subname) if crate::acl::is_container(spec.name) => {
            if !crate::acl::is_subcommand(spec.name, &crate::lossy(subname)) {
                return Err(invalid_command());
            }
            match keyspec::subcommand(spec.name, subname) {
                Some((arity, specs)) => (arity, keyspec::names_keys(specs)),
                None => (spec.arity, false),
            }
        }
        _ => (
            spec.arity,
            keyspec::has_keys(spec) && !crate::acl::is_container(spec.name),
        ),
    };
    if !has_keys {
        return Err(Frame::error("ERR The command has no key arguments"));
    }
    let arity = i64::from(arity);
    let given = command.len() as i64;
    if (arity > 0 && given != arity) || (arity < 0 && given < -arity) {
        return Err(Frame::error(
            "ERR Invalid number of arguments specified for command",
        ));
    }
    let found = keyspec::find(spec, command);
    if found.is_empty() && !spec.flags.contains(&"no_mandatory_keys") {
        return Err(Frame::error("ERR Invalid arguments specified for command"));
    }
    Ok(found)
}

/// Key flags as Redis replies with them: a set of simple strings.
fn flag_set(flags: u16) -> Frame {
    Frame::Set(keyspec::flag_names(flags).map(Frame::simple).collect())
}

/// Every command flag, in the order Redis lists them.
const FLAG_ORDER: [&str; 25] = [
    "write",
    "readonly",
    "denyoom",
    "module",
    "admin",
    "pubsub",
    "noscript",
    "blocking",
    "loading",
    "stale",
    "skip_monitor",
    "skip_slowlog",
    "asking",
    "fast",
    "no_auth",
    "may_replicate",
    "sentinel",
    "only_sentinel",
    "no_mandatory_keys",
    "protected",
    "module_no_cluster",
    "no_async_loading",
    "no_multi",
    "movablekeys",
    "allow_busy",
];

/// The flags `COMMAND INFO` reports, in Redis's order: the table's, with
/// `movablekeys` when the key specifications call for it. A container
/// reports none: Redis gives its flags to the subcommands, which the table
/// applies them to here; only `COMMAND` has flags of its own.
fn reported_flags(spec: &CommandSpec) -> Vec<&'static str> {
    if crate::acl::is_container(spec.name) && spec.name != "command" {
        return Vec::new();
    }
    let movable = keyspec::movable(spec).then_some("movablekeys");
    let mut flags: Vec<&'static str> = spec.flags.iter().copied().chain(movable).collect();
    flags.sort_by_key(|flag| FLAG_ORDER.iter().position(|known| known == flag));
    flags
}

/// One key specification as `COMMAND INFO` describes it.
fn key_spec(spec: &KeySpec) -> Frame {
    let pair = |name: &str, value: Frame| (Frame::bulk(name.to_owned()), value);
    let int = |value: i64| Frame::Integer(value);
    let (begin_type, begin) = match spec.begin_search {
        BeginSearch::Index(index) => ("index", vec![pair("index", int(index as i64))]),
        BeginSearch::Keyword { keyword, startfrom } => (
            "keyword",
            vec![
                pair("keyword", Frame::bulk(keyword)),
                pair("startfrom", int(startfrom)),
            ],
        ),
        BeginSearch::Unknown => ("unknown", Vec::new()),
    };
    let (find_type, find) = match spec.find_keys {
        FindKeys::Range {
            lastkey,
            keystep,
            limit,
        } => (
            "range",
            vec![
                pair("lastkey", int(lastkey)),
                pair("keystep", int(keystep as i64)),
                pair("limit", int(limit)),
            ],
        ),
        FindKeys::Keynum {
            keynumidx,
            firstkey,
            keystep,
        } => (
            "keynum",
            vec![
                pair("keynumidx", int(keynumidx as i64)),
                pair("firstkey", int(firstkey as i64)),
                pair("keystep", int(keystep as i64)),
            ],
        ),
        FindKeys::Unknown => ("unknown", Vec::new()),
    };
    let mut fields = Vec::with_capacity(4);
    if !spec.notes.is_empty() {
        fields.push(pair("notes", Frame::bulk(spec.notes)));
    }
    fields.push(pair("flags", flag_set(spec.flags)));
    fields.push(pair(
        "begin_search",
        Frame::Map(vec![
            pair("type", Frame::bulk(begin_type)),
            pair("spec", Frame::Map(begin)),
        ]),
    ));
    fields.push(pair(
        "find_keys",
        Frame::Map(vec![
            pair("type", Frame::bulk(find_type)),
            pair("spec", Frame::Map(find)),
        ]),
    ));
    Frame::Map(fields)
}

/// The ten-element description Redis 7 returns for each command.
fn info(spec: &CommandSpec) -> Frame {
    Frame::Array(vec![
        Frame::bulk(spec.name),
        Frame::Integer(i64::from(spec.arity)),
        Frame::Set(
            reported_flags(spec)
                .into_iter()
                .map(Frame::simple)
                .collect(),
        ),
        Frame::Integer(spec.first_key),
        Frame::Integer(spec.last_key),
        Frame::Integer(spec.key_step),
        Frame::Set(
            crate::acl::categories(spec)
                .into_iter()
                .map(|category| Frame::simple(format!("@{category}")))
                .collect(),
        ),
        Frame::Set(Vec::new()), // tips
        Frame::Set(spec.key_specs.iter().map(key_spec).collect()),
        Frame::Set(Vec::new()), // subcommands
    ])
}

fn docs(spec: &CommandSpec) -> Frame {
    Frame::Map(vec![
        (Frame::bulk("summary"), Frame::bulk(spec.summary)),
        (Frame::bulk("since"), Frame::bulk(spec.since)),
        (Frame::bulk("group"), Frame::bulk(spec.group)),
    ])
}

#[cfg(test)]
mod tests {
    use crate::testing::one;
    use crate::COMMANDS;
    use rexyra_resp::Frame;

    fn keys(names: &[&str]) -> Frame {
        Frame::Array(
            names
                .iter()
                .map(|name| Frame::bulk(name.to_string()))
                .collect(),
        )
    }

    /// A reply written out the way `tools/compare-command-info.py` shows
    /// Redis's, types included: `'bulk'`, `simple`, `[array]`, `~{set}`,
    /// `{map: ...}`.
    fn render(frame: &Frame) -> String {
        let all = |items: &[Frame]| items.iter().map(render).collect::<Vec<_>>().join(", ");
        match frame {
            Frame::Simple(text) => text.clone(),
            Frame::Error(text) => format!("(error) {text}"),
            Frame::Integer(n) => n.to_string(),
            Frame::Bulk(data) => format!("'{}'", String::from_utf8_lossy(data)),
            Frame::Null => "nil".into(),
            Frame::Array(items) => format!("[{}]", all(items)),
            Frame::Set(items) => format!("~{{{}}}", all(items)),
            Frame::Map(pairs) => {
                let pairs: Vec<String> = pairs
                    .iter()
                    .map(|(k, v)| format!("{}: {}", render(k), render(v)))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
            other => format!("{other:?}"),
        }
    }

    fn info_field(command: &str, field: usize) -> String {
        let Frame::Array(entries) = one(&["COMMAND", "INFO", command]) else {
            panic!("expected array");
        };
        let Frame::Array(fields) = &entries[0] else {
            panic!("expected array");
        };
        render(&fields[field])
    }

    /// Key specifications as Redis 7.4's `COMMAND INFO` gives them (in
    /// RESP3; RESP2 writes the same sets and maps as arrays).
    #[test]
    fn info_lists_key_specifications() {
        for (command, expected) in [
            ("PING", "~{}"),
            (
                "GET",
                "~{{'flags': ~{RO, access}, 'begin_search': {'type': 'index', 'spec': \
                 {'index': 1}}, 'find_keys': {'type': 'range', 'spec': {'lastkey': 0, \
                 'keystep': 1, 'limit': 0}}}}",
            ),
            (
                "LMOVE",
                "~{{'flags': ~{RW, access, delete}, 'begin_search': {'type': 'index', 'spec': \
                 {'index': 1}}, 'find_keys': {'type': 'range', 'spec': {'lastkey': 0, \
                 'keystep': 1, 'limit': 0}}}, {'flags': ~{RW, insert}, 'begin_search': \
                 {'type': 'index', 'spec': {'index': 2}}, 'find_keys': {'type': 'range', \
                 'spec': {'lastkey': 0, 'keystep': 1, 'limit': 0}}}}",
            ),
            (
                "EVAL_RO",
                "~{{'notes': 'We cannot tell how the keys will be used so we assume the \
                 worst, RO and ACCESS', 'flags': ~{RO, access}, 'begin_search': {'type': \
                 'index', 'spec': {'index': 2}}, 'find_keys': {'type': 'keynum', 'spec': \
                 {'keynumidx': 0, 'firstkey': 1, 'keystep': 1}}}}",
            ),
            (
                "SORT_RO",
                "~{{'flags': ~{RO, access}, 'begin_search': {'type': 'index', 'spec': \
                 {'index': 1}}, 'find_keys': {'type': 'range', 'spec': {'lastkey': 0, \
                 'keystep': 1, 'limit': 0}}}, {'notes': 'For the optional BY/GET keyword. It \
                 is marked 'unknown' because the key names derive from the content of the \
                 key we sort', 'flags': ~{RO, access}, 'begin_search': {'type': 'unknown', \
                 'spec': {}}, 'find_keys': {'type': 'unknown', 'spec': {}}}}",
            ),
            (
                "SPUBLISH",
                "~{{'flags': ~{not_key}, 'begin_search': {'type': 'index', 'spec': {'index': \
                 1}}, 'find_keys': {'type': 'range', 'spec': {'lastkey': 0, 'keystep': 1, \
                 'limit': 0}}}}",
            ),
        ] {
            assert_eq!(info_field(command, 8), expected, "{command}");
        }
        // Shard channels sit in key positions, as in Redis.
        for (command, positions) in [
            ("SSUBSCRIBE", "1 -1 1"),
            ("SUNSUBSCRIBE", "1 -1 1"),
            ("SPUBLISH", "1 1 1"),
        ] {
            let found: Vec<String> = (3..6).map(|i| info_field(command, i)).collect();
            assert_eq!(found.join(" "), positions, "{command}");
        }
    }

    /// The flags as Redis 7.4 reports them: in its order, with
    /// `movablekeys` where the keys move, and none for a container.
    #[test]
    fn info_reports_flags_as_redis_does() {
        for (command, expected) in [
            ("LMPOP", "~{write, movablekeys}"),
            ("BLMPOP", "~{write, blocking, movablekeys}"),
            ("SORT", "~{write, denyoom, movablekeys}"),
            ("SORT_RO", "~{readonly, movablekeys}"),
            ("ZUNIONSTORE", "~{write, denyoom, movablekeys}"),
            ("SINTERCARD", "~{readonly, movablekeys}"),
            (
                "EVAL",
                "~{noscript, stale, skip_monitor, no_mandatory_keys, movablekeys}",
            ),
            ("GET", "~{readonly, fast}"),
            ("SET", "~{write, denyoom}"),
            ("BZPOPMIN", "~{write, blocking, fast}"),
            ("ECHO", "~{loading, stale, fast}"),
            (
                "AUTH",
                "~{noscript, loading, stale, fast, no_auth, allow_busy}",
            ),
            ("PUBLISH", "~{pubsub, loading, stale, fast}"),
            ("CONFIG", "~{}"),
            ("OBJECT", "~{}"),
            ("COMMAND", "~{loading, stale}"),
        ] {
            assert_eq!(info_field(command, 2), expected, "{command}");
        }
    }

    /// Each compared with Redis 7.4.
    #[test]
    fn getkeysandflags() {
        let reply = |line: &str| {
            let mut args = vec!["COMMAND", "GETKEYSANDFLAGS"];
            args.extend(line.split(' ').filter(|arg| !arg.is_empty()));
            render(&one(&args))
        };
        for (line, expected) in [
            (
                "LMOVE src dst LEFT RIGHT",
                "[['src', ~{RW, access, delete}], ['dst', ~{RW, insert}]]",
            ),
            ("SET a 1", "[['a', ~{OW, update}]]"),
            ("SET a 1 GET", "[['a', ~{RW, access, update}]]"),
            (
                "SORT l BY w_* GET # STORE d",
                "[['l', ~{RO, access}], ['d', ~{OW, update}]]",
            ),
            (
                "ZUNIONSTORE d 2 a b",
                "[['d', ~{OW, update}], ['a', ~{RO, access}], ['b', ~{RO, access}]]",
            ),
            ("ZUNIONSTORE d 1x a", "[['a', ~{}], ['d', ~{}]]"),
            ("EVAL s 1 a", "[['a', ~{RW, access, update}]]"),
            ("EVAL s 0", "[]"),
            ("EVAL s 3 a b", "[]"),
            ("OBJECT ENCODING a", "[['a', ~{RO}]]"),
            ("MEMORY USAGE a SAMPLES 5", "[['a', ~{RO}]]"),
            (
                "ZUNIONSTORE d 3 a b",
                "(error) ERR Invalid arguments specified for command",
            ),
            (
                "LMPOP 0 a LEFT",
                "(error) ERR Invalid arguments specified for command",
            ),
            (
                "",
                "(error) ERR wrong number of arguments for 'command|getkeysandflags' command",
            ),
            ("NOPE a", "(error) ERR Invalid command specified"),
            ("OBJECT NOPE a", "(error) ERR Invalid command specified"),
            (
                "GET",
                "(error) ERR Invalid number of arguments specified for command",
            ),
            (
                "OBJECT ENCODING",
                "(error) ERR Invalid number of arguments specified for command",
            ),
            // No keys is said before a wrong number of arguments.
            ("ECHO", "(error) ERR The command has no key arguments"),
            ("CONFIG GET", "(error) ERR The command has no key arguments"),
            ("OBJECT", "(error) ERR The command has no key arguments"),
            (
                "OBJECT HELP",
                "(error) ERR The command has no key arguments",
            ),
            (
                "SPUBLISH c m",
                "(error) ERR The command has no key arguments",
            ),
        ] {
            assert_eq!(reply(line), expected, "{line}");
        }
        // GETKEYS gives the same keys, and the same errors.
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "ZUNIONSTORE", "d", "2", "a", "b"]),
            keys(&["d", "a", "b"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "ECHO"]),
            Frame::error("ERR The command has no key arguments")
        );
    }

    #[test]
    fn getkeys() {
        // Compared with Redis 7.4.
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"]),
            keys(&["a", "b"])
        );
        assert_eq!(one(&["COMMAND", "GETKEYS", "get", "a"]), keys(&["a"]));
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "EVAL", "return 1", "2", "k1", "k2", "a"]),
            keys(&["k1", "k2"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "EVAL", "return 1", "0"]),
            keys(&[])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "SORT", "l", "STORE", "d"]),
            keys(&["l", "d"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "SORT", "l", "STORE", "a", "STORE", "b"]),
            keys(&["l", "b"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "SORT", "l", "GET", "STORE", "x"]),
            keys(&["l"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "EVAL", "s", "3", "a", "b"]),
            keys(&[])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "EVAL", "s", "1x", "a"]),
            keys(&["a"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "OBJECT", "ENCODING", "a"]),
            keys(&["a"])
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "ZUNION", "3", "a", "b"]),
            Frame::error("ERR Invalid arguments specified for command")
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS"]),
            Frame::error("ERR wrong number of arguments for 'command|getkeys' command")
        );
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "nope", "a"]),
            Frame::error("ERR Invalid command specified")
        );
        for wrong in [
            &["COMMAND", "GETKEYS", "GET"][..],
            &["COMMAND", "GETKEYS", "GET", "a", "b"],
        ] {
            assert_eq!(
                one(wrong),
                Frame::error("ERR Invalid number of arguments specified for command")
            );
        }
        assert_eq!(
            one(&["COMMAND", "GETKEYS", "PING"]),
            Frame::error("ERR The command has no key arguments")
        );
    }

    /// A command that `has_keys` says has none finds none.
    #[test]
    fn every_command_with_keys_in_its_arguments_is_known() {
        for line in [
            &["name", "sub", "2", "k1", "k2", "STORE", "d", "x"][..],
            &["name", "usage", "k1"],
            &["name", "encoding", "k1"],
        ] {
            let args: Vec<bytes::Bytes> = line
                .iter()
                .map(|arg| bytes::Bytes::from_static(arg.as_bytes()))
                .collect();
            for spec in COMMANDS {
                let found = crate::acl::command_keys(spec, &args);
                if !crate::keyspec::has_keys(spec) {
                    assert!(
                        found.is_empty(),
                        "{} reads keys from its arguments",
                        spec.name
                    );
                }
            }
        }
    }

    #[test]
    fn count_matches_table() {
        assert_eq!(
            one(&["COMMAND", "COUNT"]),
            Frame::Integer(COMMANDS.len() as i64)
        );
    }

    #[test]
    fn bare_command_lists_everything() {
        let Frame::Array(entries) = one(&["COMMAND"]) else {
            panic!("expected array");
        };
        assert_eq!(entries.len(), COMMANDS.len());
        for entry in entries {
            let Frame::Array(fields) = entry else {
                panic!("expected array");
            };
            assert_eq!(fields.len(), 10);
        }
    }

    #[test]
    fn info_for_named_commands() {
        let Frame::Array(entries) = one(&["COMMAND", "INFO", "ECHO", "nope"]) else {
            panic!("expected array");
        };
        assert_eq!(entries.len(), 2);
        let Frame::Array(fields) = &entries[0] else {
            panic!("expected array");
        };
        assert_eq!(fields[0], Frame::bulk("echo"));
        assert_eq!(fields[1], Frame::Integer(2));
        assert_eq!(entries[1], Frame::Null);
    }

    /// Every ACL category, as Redis 7.4 lists them, for a sample covering
    /// each rule: the flags, the group, a container, and the exceptions.
    #[test]
    fn info_lists_every_acl_category() {
        for (command, expected) in [
            ("GET", "@read @string @fast"),
            ("BLPOP", "@write @list @slow @blocking"),
            ("DEL", "@keyspace @write @slow"),
            ("FLUSHALL", "@keyspace @write @slow @dangerous"),
            ("SORT", "@write @set @sortedset @list @slow @dangerous"),
            ("LASTSAVE", "@admin @fast @dangerous"),
            ("EVAL_RO", "@slow @scripting"),
            ("PUBLISH", "@pubsub @fast"),
            ("MULTI", "@fast @transaction"),
            ("CONFIG", "@slow"),
            ("COMMAND", "@slow @connection"),
        ] {
            let Frame::Array(entries) = one(&["COMMAND", "INFO", command]) else {
                panic!("expected array");
            };
            let Frame::Array(fields) = &entries[0] else {
                panic!("expected array");
            };
            let Frame::Set(categories) = &fields[6] else {
                panic!("expected set");
            };
            let names: Vec<String> = categories
                .iter()
                .map(|frame| match frame {
                    Frame::Simple(name) => name.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(names.join(" "), expected, "{command}");
        }
    }

    #[test]
    fn list_returns_names() {
        let Frame::Array(names) = one(&["COMMAND", "LIST"]) else {
            panic!("expected array");
        };
        assert!(names.contains(&Frame::bulk("ping")));
    }

    #[test]
    fn docs_is_keyed_by_name() {
        let Frame::Map(pairs) = one(&["COMMAND", "DOCS", "PING"]) else {
            panic!("expected map");
        };
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, Frame::bulk("ping"));

        let Frame::Map(all) = one(&["COMMAND", "DOCS"]) else {
            panic!("expected map");
        };
        assert_eq!(all.len(), COMMANDS.len());
    }

    #[test]
    fn errors() {
        assert_eq!(
            one(&["COMMAND", "NOPE"]),
            Frame::error("ERR unknown subcommand 'NOPE'. Try COMMAND HELP.")
        );
        assert_eq!(
            one(&["COMMAND", "COUNT", "x"]),
            Frame::error("ERR wrong number of arguments for 'command|count' command")
        );
    }
}
