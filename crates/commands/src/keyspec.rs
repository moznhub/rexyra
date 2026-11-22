//! Key specifications: where a command's keys are among its arguments and
//! what the command does with each.
//!
//! This is Redis 7's model, which `COMMAND INFO` reports and `COMMAND
//! GETKEYSANDFLAGS` applies. A command has a list of [`KeySpec`]s. Each
//! finds the first key ([`BeginSearch`]), then the keys from there
//! ([`FindKeys`]), and says how they are used (the flags: `RO`, `RW`, `OW`
//! or `RM`, plus `access`, `update`, `insert`, `delete` and a few more).
//!
//! [`find`] is the one place that works out the keys of a command line: the
//! ACL key checks, `COMMAND GETKEYS` and `GETKEYSANDFLAGS` all use it. Like
//! Redis it reads the key specifications, and for the commands whose keys
//! the specifications cannot describe exactly (`SORT ... STORE`, `SET ...
//! GET`, a `numkeys` that is not a plain number) it runs a procedure of
//! the command's own, which gives the same keys as Redis's.

use bytes::Bytes;

use crate::{is, CommandSpec};

/// Read-only: the command reads the key's value, or its metadata.
pub const RO: u16 = 1 << 0;
/// Read-write: the command modifies the key's value.
pub const RW: u16 = 1 << 1;
/// Overwrite: the command replaces the value without reading it.
pub const OW: u16 = 1 << 2;
/// Remove: the command deletes the key.
pub const RM: u16 = 1 << 3;
/// The value (not only metadata) is returned or used.
pub const ACCESS: u16 = 1 << 4;
/// The value is updated or replaced.
pub const UPDATE: u16 = 1 << 5;
/// Data is added to the value.
pub const INSERT: u16 = 1 << 6;
/// Data is removed from the value.
pub const DELETE: u16 = 1 << 7;
/// Not a key, but routed like one (shard channels).
pub const NOT_KEY: u16 = 1 << 8;
/// The specification may miss keys.
pub const INCOMPLETE: u16 = 1 << 9;
/// The flags depend on the other arguments.
pub const VARIABLE_FLAGS: u16 = 1 << 10;

/// The flags' names, in the order Redis lists them.
const FLAG_NAMES: [(u16, &str); 11] = [
    (RO, "RO"),
    (RW, "RW"),
    (OW, "OW"),
    (RM, "RM"),
    (ACCESS, "access"),
    (UPDATE, "update"),
    (INSERT, "insert"),
    (DELETE, "delete"),
    (NOT_KEY, "not_key"),
    (INCOMPLETE, "incomplete"),
    (VARIABLE_FLAGS, "variable_flags"),
];

/// How the first key of a specification is found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeginSearch {
    /// At this argument index.
    Index(usize),
    /// After the first `keyword`, searched from argument `startfrom` on, or
    /// backwards from the end when `startfrom` is negative.
    Keyword {
        keyword: &'static str,
        startfrom: i64,
    },
    /// Only the command's own procedure knows.
    Unknown,
}

/// How the keys are found from the first one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindKeys {
    /// The last key is `lastkey` arguments after the first, or counts back
    /// from the end when negative (with `limit` > 1, only that fraction of
    /// the remaining arguments); every `keystep`th argument is a key.
    Range {
        lastkey: i64,
        keystep: usize,
        limit: i64,
    },
    /// The argument `keynumidx` after the first says how many keys there
    /// are; they start `firstkey` arguments after the first.
    Keynum {
        keynumidx: usize,
        firstkey: usize,
        keystep: usize,
    },
    /// Only the command's own procedure knows.
    Unknown,
}

/// One key specification of a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySpec {
    /// Redis's explanation, if it gives one; empty otherwise.
    pub notes: &'static str,
    pub flags: u16,
    pub begin_search: BeginSearch,
    pub find_keys: FindKeys,
}

pub type KeySpecs = &'static [KeySpec];

/// A specification: how its keys are used, where the first is, and how
/// the rest follow.
pub const fn spec(flags: u16, begin_search: BeginSearch, find_keys: FindKeys) -> KeySpec {
    KeySpec {
        notes: "",
        flags,
        begin_search,
        find_keys,
    }
}

impl KeySpec {
    /// The same specification with Redis's notes.
    pub const fn notes(self, notes: &'static str) -> KeySpec {
        KeySpec { notes, ..self }
    }
}

/// The first key is argument `index`.
pub const fn at(index: usize) -> BeginSearch {
    BeginSearch::Index(index)
}

/// Keys from the first up to `lastkey` (see [`FindKeys::Range`]).
pub const fn range(lastkey: i64, keystep: usize, limit: i64) -> FindKeys {
    FindKeys::Range {
        lastkey,
        keystep,
        limit,
    }
}

/// A key count, then the keys (see [`FindKeys::Keynum`]).
pub const fn keynum(keynumidx: usize, firstkey: usize, keystep: usize) -> FindKeys {
    FindKeys::Keynum {
        keynumidx,
        firstkey,
        keystep,
    }
}

/// Just one key.
pub const ONE: FindKeys = range(0, 1, 0);
/// Every argument up to the last.
pub const REST: FindKeys = range(-1, 1, 0);

/// The names of a set of key flags, in Redis's order.
pub fn flag_names(flags: u16) -> impl Iterator<Item = &'static str> {
    FLAG_NAMES
        .into_iter()
        .filter(move |(bit, _)| flags & bit != 0)
        .map(|(_, name)| name)
}

/// The subcommands that have keys, with their arity and specifications. A
/// container command (`OBJECT`, `MEMORY`) has none of its own.
const SUBCOMMANDS: &[(&str, &str, i32, KeySpecs)] = &[
    ("object", "encoding", 3, &[spec(RO, at(2), ONE)]),
    ("object", "freq", 3, &[spec(RO, at(2), ONE)]),
    ("object", "idletime", 3, &[spec(RO, at(2), ONE)]),
    ("object", "refcount", 3, &[spec(RO, at(2), ONE)]),
    ("memory", "usage", -3, &[spec(RO, at(2), ONE)]),
];

/// The arity and key specifications of a container's subcommand that has
/// keys.
pub(crate) fn subcommand(command: &str, sub: &[u8]) -> Option<(i32, KeySpecs)> {
    SUBCOMMANDS
        .iter()
        .find(|(name, subname, _, _)| *name == command && is(sub, subname))
        .map(|(_, _, arity, specs)| (*arity, *specs))
}

/// The specifications that apply to a command line: a container's are
/// those of the subcommand named.
fn specs_for(spec: &CommandSpec, args: &[Bytes]) -> KeySpecs {
    match args.get(1) {
        Some(sub) if spec.key_specs.is_empty() => subcommand(spec.name, sub).map_or(&[], |s| s.1),
        _ => spec.key_specs,
    }
}

/// Whether some specification names real keys (not shard channels).
pub(crate) fn names_keys(specs: KeySpecs) -> bool {
    specs.iter().any(|spec| spec.flags & NOT_KEY == 0)
}

/// Whether a command can have keys: from its specifications, its own
/// procedure, or, for a container, a subcommand's.
pub(crate) fn has_keys(spec: &CommandSpec) -> bool {
    names_keys(spec.key_specs)
        || procedure(spec.name).is_some()
        || SUBCOMMANDS.iter().any(|(name, ..)| *name == spec.name)
}

/// Whether the keys' positions depend on the other arguments, which Redis
/// reports as the `movablekeys` flag: some specification is not a fixed
/// index with a range after it.
pub(crate) fn movable(spec: &CommandSpec) -> bool {
    spec.key_specs.iter().any(|spec| {
        !matches!(
            (spec.begin_search, spec.find_keys),
            (BeginSearch::Index(_), FindKeys::Range { .. })
        )
    })
}

/// One key found in a command line: its argument index and how the command
/// uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Found {
    pub index: usize,
    pub flags: u16,
}

/// The keys of a command line, in Redis's order, with Redis's flags. The
/// arguments start with the command name and are assumed to satisfy its
/// arity.
pub(crate) fn find(spec: &CommandSpec, args: &[Bytes]) -> Vec<Found> {
    let specs = specs_for(spec, args);
    let variable = specs.iter().any(|spec| spec.flags & VARIABLE_FLAGS != 0);
    if names_keys(specs) && !variable {
        if let Some(found) = from_specs(specs, args) {
            return found;
        }
    }
    // The specifications cannot tell, or the arguments do not fit them.
    procedure(spec.name).map_or_else(Vec::new, |procedure| procedure(args))
}

/// The keys as the specifications describe them, or `None` if the
/// arguments do not fit (a key count that is not a number, or larger than
/// the arguments there are) or a specification cannot be read.
fn from_specs(specs: KeySpecs, args: &[Bytes]) -> Option<Vec<Found>> {
    let argc = args.len();
    let mut found = Vec::new();
    for spec in specs.iter().filter(|spec| spec.flags & NOT_KEY == 0) {
        let first = match spec.begin_search {
            BeginSearch::Index(index) => index,
            BeginSearch::Keyword { keyword, startfrom } => {
                let matches = |&i: &usize| args.get(i).is_some_and(|arg| is(arg, keyword));
                let at = if startfrom >= 0 {
                    ((startfrom.max(1) as usize)..argc).find(matches)
                } else {
                    let from = (argc as i64 + startfrom).max(0) as usize;
                    (1..=from).rev().find(matches)
                };
                match at {
                    Some(i) => i + 1,
                    // No keyword, so no keys from this specification.
                    None => continue,
                }
            }
            BeginSearch::Unknown => return None,
        };
        let (first, last, step) = match spec.find_keys {
            FindKeys::Range {
                lastkey,
                keystep,
                limit,
            } => {
                let last = if lastkey >= 0 {
                    first as i64 + lastkey
                } else if limit <= 1 {
                    argc as i64 + lastkey
                } else {
                    first as i64 + (argc as i64 - first as i64) / limit + lastkey
                };
                (first as i64, last, keystep)
            }
            FindKeys::Keynum {
                keynumidx,
                firstkey,
                keystep,
            } => {
                let count = rexyra_resp::parse_i64(args.get(first + keynumidx)?)?;
                if count < 0 {
                    return None;
                }
                let start = (first + firstkey) as i64;
                (start, start + count * keystep as i64 - 1, keystep)
            }
            FindKeys::Unknown => return None,
        };
        if last >= argc as i64 || last < first {
            if spec.flags & INCOMPLETE != 0 {
                continue;
            }
            return None;
        }
        found.extend(
            (first as usize..=last as usize)
                .step_by(step.max(1))
                .map(|index| Found {
                    index,
                    flags: spec.flags,
                }),
        );
    }
    Some(found)
}

type Procedure = fn(&[Bytes]) -> Vec<Found>;

/// The commands that find their keys with a procedure of their own when
/// their specifications cannot: Redis's `getkeys_proc`s.
fn procedure(command: &str) -> Option<Procedure> {
    Some(match command {
        "set" => set_keys,
        "bitfield" => bitfield_keys,
        "sort" => sort_keys,
        "sort_ro" => |_: &[Bytes]| vec![key(1, RO | ACCESS)],
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro" | "blmpop"
        | "bzmpop" => |args: &[Bytes]| counted(args, 2),
        "zunion" | "zinter" | "zdiff" | "sintercard" | "zintercard" | "lmpop" | "zmpop" => {
            |args: &[Bytes]| counted(args, 1)
        }
        "zunionstore" | "zinterstore" | "zdiffstore" => |args: &[Bytes]| {
            let mut keys = counted(args, 2);
            if !keys.is_empty() {
                keys.push(key(1, 0));
            }
            keys
        },
        _ => return None,
    })
}

const fn key(index: usize, flags: u16) -> Found {
    Found { index, flags }
}

/// `SET key value [... GET ...]`: read as well as written with `GET`.
fn set_keys(args: &[Bytes]) -> Vec<Found> {
    let get = args.iter().skip(3).any(|arg| is(arg, "GET"));
    vec![key(
        1,
        if get {
            RW | ACCESS | UPDATE
        } else {
            OW | UPDATE
        },
    )]
}

/// `BITFIELD key [GET type offset | OVERFLOW how | ...]`: only read if
/// every operation is a complete `GET` or `OVERFLOW`.
fn bitfield_keys(args: &[Bytes]) -> Vec<Found> {
    let mut i = 2;
    let mut readonly = true;
    while i < args.len() {
        let remaining = args.len() - i - 1;
        if is(&args[i], "GET") && remaining >= 2 {
            i += 3;
        } else if is(&args[i], "OVERFLOW") && remaining >= 1 {
            i += 2;
        } else {
            readonly = false;
            break;
        }
    }
    let flags = if readonly {
        RO | ACCESS
    } else {
        RW | ACCESS | UPDATE
    };
    vec![key(1, flags)]
}

/// `SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [STORE
/// destination]`: the key, and the destination of the last `STORE` that
/// has one. The patterns are not keys.
fn sort_keys(args: &[Bytes]) -> Vec<Found> {
    let mut keys = vec![key(1, RO | ACCESS)];
    let mut store = None;
    let mut i = 2;
    while i < args.len() {
        let arg = &args[i];
        if is(arg, "LIMIT") {
            i += 2;
        } else if is(arg, "GET") || is(arg, "BY") {
            i += 1;
        } else if is(arg, "STORE") && i + 1 < args.len() {
            store = Some(i + 1);
            i += 1;
        }
        i += 1;
    }
    keys.extend(store.map(|index| key(index, OW | UPDATE)));
    keys
}

/// A key count at `count_at`, read as C's `atoi` reads it, then that many
/// keys, without flags: what Redis falls back to when the count is not a
/// plain number. No keys if the count is below one or above the arguments
/// left.
fn counted(args: &[Bytes], count_at: usize) -> Vec<Found> {
    let count = args.get(count_at).map_or(0, |arg| atoi(arg));
    let first = count_at + 1;
    if count < 1 || count > args.len().saturating_sub(first) as i64 {
        return Vec::new();
    }
    (first..first + count as usize).map(|i| key(i, 0)).collect()
}

/// C's `atoi`: leading spaces, a sign, then as many digits as there are.
fn atoi(bytes: &[u8]) -> i64 {
    let mut rest = bytes.trim_ascii_start();
    let negative = rest.first() == Some(&b'-');
    if matches!(rest.first(), Some(b'-' | b'+')) {
        rest = &rest[1..];
    }
    let mut value: i64 = 0;
    for &b in rest.iter().take_while(|b| b.is_ascii_digit()) {
        value = value.saturating_mul(10).saturating_add(i64::from(b - b'0'));
    }
    if negative {
        -value
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lookup, COMMANDS};

    fn args(line: &str) -> Vec<Bytes> {
        line.split(' ')
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect()
    }

    /// The keys and flags of a command line, as `name:FLAG,FLAG` words.
    fn keys(line: &str) -> String {
        let args = args(line);
        let spec = lookup(&args[0]).unwrap();
        find(spec, &args)
            .iter()
            .map(|found| {
                let name = String::from_utf8_lossy(&args[found.index]);
                let flags: Vec<&str> = flag_names(found.flags).collect();
                format!("{name}:{}", flags.join(","))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn flags_are_named_in_redis_order() {
        let names: Vec<&str> = flag_names(VARIABLE_FLAGS | DELETE | RW | ACCESS).collect();
        assert_eq!(names, ["RW", "access", "delete", "variable_flags"]);
        assert_eq!(flag_names(0).count(), 0);
    }

    #[test]
    fn helpers_build_redis_specifications() {
        assert_eq!(
            spec(RO | ACCESS, at(1), ONE),
            KeySpec {
                notes: "",
                flags: RO | ACCESS,
                begin_search: BeginSearch::Index(1),
                find_keys: FindKeys::Range {
                    lastkey: 0,
                    keystep: 1,
                    limit: 0
                },
            }
        );
        assert_eq!(
            spec(RW, at(2), keynum(0, 1, 1)).notes("why").find_keys,
            FindKeys::Keynum {
                keynumidx: 0,
                firstkey: 1,
                keystep: 1
            }
        );
        assert_eq!(spec(RW, at(2), keynum(0, 1, 1)).notes("why").notes, "why");
    }

    /// Each case compared with `COMMAND GETKEYSANDFLAGS` in Redis 7.4.
    #[test]
    fn keys_and_flags_match_redis() {
        for (line, expected) in [
            ("GET a", "a:RO,access"),
            (
                "LMOVE src dst LEFT RIGHT",
                "src:RW,access,delete dst:RW,insert",
            ),
            ("MSET a 1 b 2", "a:OW,update b:OW,update"),
            ("MSET a 1 b", "a:OW,update b:OW,update"),
            ("BLPOP a b 0", "a:RW,access,delete b:RW,access,delete"),
            ("BITOP AND d a b", "d:OW,update a:RO,access b:RO,access"),
            ("DEL a a", "a:RM,delete a:RM,delete"),
            ("SET a 1", "a:OW,update"),
            ("SET a 1 get", "a:RW,access,update"),
            ("SET a GET", "a:OW,update"),
            ("SET a 1 EX GET", "a:RW,access,update"),
            ("BITFIELD a GET u8 0", "a:RO,access"),
            ("BITFIELD a", "a:RO,access"),
            ("BITFIELD a OVERFLOW SET", "a:RO,access"),
            ("BITFIELD a GET SET 0", "a:RO,access"),
            ("BITFIELD a GET u8", "a:RW,access,update"),
            ("BITFIELD a x SET", "a:RW,access,update"),
            ("BITFIELD a SET u8 0 1", "a:RW,access,update"),
            ("SORT l", "l:RO,access"),
            ("SORT l store d", "l:RO,access d:OW,update"),
            ("SORT l GET STORE STORE d", "l:RO,access d:OW,update"),
            ("SORT l STORE a STORE b", "l:RO,access b:OW,update"),
            ("SORT l LIMIT 0 STORE d", "l:RO,access"),
            ("SORT l STORE d LIMIT 1 2 STORE", "l:RO,access d:OW,update"),
            ("SORT STORE STORE d", "STORE:RO,access d:OW,update"),
            ("SORT_RO l STORE d", "l:RO,access"),
            ("EVAL s 2 a b c", "a:RW,access,update b:RW,access,update"),
            ("EVAL_RO s 1 a", "a:RO,access"),
            ("EVAL s 0", ""),
            ("EVAL s 3 a b", ""),
            ("EVAL s -1", ""),
            ("EVAL s 1x a", "a:"),
            ("EVAL s 01 a", "a:"),
            ("EVAL s +1 a", "a:"),
            ("FCALL f 1x a", "a:"),
            ("ZUNIONSTORE d 2 a b", "d:OW,update a:RO,access b:RO,access"),
            ("ZUNIONSTORE d 1x a", "a: d:"),
            ("ZUNIONSTORE d 3 a b", ""),
            ("ZUNIONSTORE d 0 a", ""),
            ("ZUNION 2 a b", "a:RO,access b:RO,access"),
            ("ZUNION x a", ""),
            ("ZDIFF 01 a", "a:"),
            (
                "LMPOP 3 a b LEFT",
                "a:RW,access,delete b:RW,access,delete LEFT:RW,access,delete",
            ),
            ("LMPOP 3 a b", ""),
            ("LMPOP 1x a LEFT", "a:"),
            (
                "BLMPOP 0 2 a b LEFT",
                "a:RW,access,delete b:RW,access,delete",
            ),
            ("BLMPOP 0 4 a b LEFT", ""),
            ("SINTERCARD 2 a b", "a:RO,access b:RO,access"),
            ("SINTERCARD 3 a b", ""),
            ("OBJECT ENCODING a", "a:RO"),
            ("OBJECT encoding a", "a:RO"),
            ("OBJECT HELP", ""),
            ("MEMORY USAGE a SAMPLES 5", "a:RO"),
            ("SPUBLISH c m", ""),
            ("SSUBSCRIBE c d", ""),
        ] {
            assert_eq!(keys(line), expected, "{line}");
        }
    }

    #[test]
    fn a_keyword_begins_the_keys_after_it() {
        const SPECS: KeySpecs = &[
            spec(
                RO,
                BeginSearch::Keyword {
                    keyword: "KEYS",
                    startfrom: 1,
                },
                REST,
            ),
            spec(
                RW,
                BeginSearch::Keyword {
                    keyword: "TO",
                    startfrom: -2,
                },
                ONE,
            ),
        ];
        let line = args("CMD x keys a b TO d");
        assert_eq!(
            from_specs(SPECS, &line),
            Some(vec![
                key(3, RO),
                key(4, RO),
                key(5, RO),
                key(6, RO),
                key(6, RW)
            ])
        );
        // Without the keyword the specification names no keys.
        assert_eq!(from_specs(SPECS, &args("CMD x y")), Some(Vec::new()));
    }

    #[test]
    fn an_incomplete_specification_that_does_not_fit_is_skipped() {
        const SPECS: KeySpecs = &[
            spec(RO, at(1), ONE),
            spec(RW | INCOMPLETE, at(2), range(3, 1, 0)),
        ];
        assert_eq!(from_specs(SPECS, &args("CMD a b")), Some(vec![key(1, RO)]));
        const COMPLETE: KeySpecs = &[spec(RO, at(1), ONE), spec(RW, at(2), range(3, 1, 0))];
        assert_eq!(from_specs(COMPLETE, &args("CMD a b")), None);
    }

    #[test]
    fn atoi_reads_what_c_reads() {
        for (text, value) in [
            ("12", 12),
            ("1x", 1),
            ("01", 1),
            ("+3", 3),
            (" 4", 4),
            ("-2", -2),
            ("x", 0),
            ("", 0),
            ("1.5", 1),
        ] {
            assert_eq!(atoi(text.as_bytes()), value, "{text}");
        }
    }

    /// `movablekeys` is reported for exactly the commands Redis 7.4
    /// reports it for.
    #[test]
    fn movable_keys_follow_from_the_specifications() {
        let movable: Vec<&str> = COMMANDS
            .iter()
            .filter(|spec| movable(spec))
            .map(|spec| spec.name)
            .collect();
        assert_eq!(
            movable,
            [
                "lmpop",
                "blmpop",
                "sort",
                "sort_ro",
                "eval",
                "evalsha",
                "eval_ro",
                "evalsha_ro",
                "fcall",
                "fcall_ro",
                "sintercard",
                "zmpop",
                "bzmpop",
                "zunion",
                "zinter",
                "zdiff",
                "zunionstore",
                "zinterstore",
                "zdiffstore",
                "zintercard"
            ]
        );
    }

    /// Every command with a procedure of its own also has specifications
    /// that name keys, as in Redis, so that both agree on whether it has
    /// keys at all.
    #[test]
    fn procedures_belong_to_commands_with_keys() {
        for spec in COMMANDS {
            if procedure(spec.name).is_some() {
                assert!(names_keys(spec.key_specs), "{}", spec.name);
            }
        }
        for (name, sub, _, _) in SUBCOMMANDS {
            assert!(lookup(name.as_bytes()).is_some(), "{name}");
            assert!(crate::acl::is_subcommand(name, sub), "{name} {sub}");
        }
    }
}
