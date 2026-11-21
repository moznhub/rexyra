//! `COMMAND` and its subcommands. Client libraries and `redis-cli` call these
//! on connect to discover what the server supports.

use bytes::Bytes;
use rexyra_resp::Frame;

use crate::{is, lookup, unknown_subcommand, wrong_subcommand_arity, CommandSpec, Ctx, COMMANDS};

/// `COMMAND [COUNT | LIST | INFO [name ...] | DOCS [name ...]]`
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
        getkeys(sub, names)
    } else {
        unknown_subcommand("COMMAND", sub)
    }
}

/// `COMMAND GETKEYS command [arg ...]`: the keys that command would name.
fn getkeys(sub: &Bytes, command: &[Bytes]) -> Frame {
    let Some(name) = command.first() else {
        return wrong_subcommand_arity("command", sub);
    };
    let Some(spec) = lookup(name) else {
        return Frame::error("ERR Invalid command specified");
    };
    let arity = i64::from(spec.arity);
    let given = command.len() as i64;
    if (arity > 0 && given != arity) || (arity < 0 && given < -arity) {
        return Frame::error("ERR Invalid number of arguments specified for command");
    }
    if spec.first_key == 0 && !crate::acl::has_movable_keys(spec) {
        return Frame::error("ERR The command has no key arguments");
    }
    Frame::Array(
        crate::acl::command_keys(spec, command)
            .into_iter()
            .map(|key| Frame::Bulk(key.clone()))
            .collect(),
    )
}

/// The ten-element description Redis 7 returns for each command.
fn info(spec: &CommandSpec) -> Frame {
    Frame::Array(vec![
        Frame::bulk(spec.name),
        Frame::Integer(i64::from(spec.arity)),
        Frame::Set(spec.flags.iter().map(|flag| Frame::simple(*flag)).collect()),
        Frame::Integer(spec.first_key),
        Frame::Integer(spec.last_key),
        Frame::Integer(spec.key_step),
        Frame::Set(
            crate::acl::categories(spec)
                .into_iter()
                .map(|category| Frame::simple(format!("@{category}")))
                .collect(),
        ),
        Frame::Array(Vec::new()), // tips
        Frame::Array(Vec::new()), // key specifications
        Frame::Array(Vec::new()), // subcommands
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

    /// A command without keys in the table either has none or is one whose
    /// keys are read from its arguments, and `has_movable_keys` knows which.
    #[test]
    fn every_command_with_keys_in_its_arguments_is_known() {
        let args: Vec<bytes::Bytes> = ["name", "sub", "2", "k1", "k2", "STORE", "d", "x"]
            .iter()
            .map(|arg| bytes::Bytes::from_static(arg.as_bytes()))
            .collect();
        for spec in COMMANDS {
            let found = crate::acl::command_keys(spec, &args);
            if spec.first_key == 0 && !crate::acl::has_movable_keys(spec) {
                assert!(
                    found.is_empty(),
                    "{} reads keys from its arguments",
                    spec.name
                );
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
