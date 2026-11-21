//! Users and permissions: `AUTH`, `ACL`, `requirepass`.
//!
//! Every connection acts as a user. A user has passwords (kept only as
//! SHA-256 hashes), the commands it may run and the key patterns it may
//! touch. The `default` user is what a connection is before it
//! authenticates; out of the box it has no password and may do everything,
//! and `requirepass` is simply a password on it.
//!
//! Command permissions are an ordered list of rules (`-@all +get
//! +@list -lpush`); the last rule that applies to a command decides.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use rexyra_engine::glob_match;
use rexyra_resp::Frame;

use crate::sha256::sha256_hex;
use crate::{
    int, is, lookup, lossy, unknown_subcommand, wrong_arity, wrong_subcommand_arity, CommandSpec,
    Ctx, Effect, Server, COMMANDS, ERR_SYNTAX,
};

pub(crate) const DEFAULT_USER: &str = "default";
const ERR_WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

/// Every category Redis knows. Those for features this server does not have
/// (streams, geo, ...) are valid names that match no command.
const CATEGORIES: [&str; 21] = [
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// Commands Redis puts in `@dangerous` besides the administrative ones.
const DANGEROUS: [&str; 9] = [
    "flushall", "flushdb", "keys", "info", "sort", "sort_ro", "restore", "lastsave", "shutdown",
];

type Subcommands = &'static [(&'static str, &'static [&'static str])];

const ADMIN: &[&str] = &["admin", "slow", "dangerous"];
const CONN: &[&str] = &["slow", "connection"];
const CONN_ADMIN: &[&str] = &["admin", "slow", "dangerous", "connection"];
const SCRIPT: &[&str] = &["slow", "scripting"];
const SCRIPT_WRITE: &[&str] = &["write", "slow", "scripting"];
const KEY_READ: &[&str] = &["keyspace", "read", "slow"];
const SLOW: &[&str] = &["slow"];
const PUBSUB_SLOW: &[&str] = &["pubsub", "slow"];

/// Commands that are containers of subcommands, with the categories of
/// each subcommand. Permissions apply to the subcommand.
const CONTAINERS: &[(&str, Subcommands)] = &[
    (
        "acl",
        &[
            ("setuser", ADMIN),
            ("getuser", ADMIN),
            ("deluser", ADMIN),
            ("list", ADMIN),
            ("users", ADMIN),
            ("dryrun", ADMIN),
            ("save", ADMIN),
            ("load", ADMIN),
            ("log", ADMIN),
            ("whoami", SLOW),
            ("cat", SLOW),
            ("genpass", SLOW),
            ("help", SLOW),
        ],
    ),
    (
        "client",
        &[
            ("kill", CONN_ADMIN),
            ("list", CONN_ADMIN),
            ("no-evict", CONN_ADMIN),
            ("unblock", CONN_ADMIN),
            ("id", CONN),
            ("info", CONN),
            ("getname", CONN),
            ("setname", CONN),
            ("setinfo", CONN),
            ("no-touch", CONN),
            ("help", CONN),
        ],
    ),
    (
        "command",
        &[
            ("count", CONN),
            ("docs", CONN),
            ("getkeys", CONN),
            ("info", CONN),
            ("list", CONN),
            ("help", CONN),
        ],
    ),
    (
        "config",
        &[
            ("get", ADMIN),
            ("set", ADMIN),
            ("resetstat", ADMIN),
            ("rewrite", ADMIN),
            ("help", SLOW),
        ],
    ),
    (
        "function",
        &[
            ("load", SCRIPT_WRITE),
            ("delete", SCRIPT_WRITE),
            ("flush", SCRIPT_WRITE),
            ("restore", SCRIPT_WRITE),
            ("list", SCRIPT),
            ("stats", SCRIPT),
            ("dump", SCRIPT),
            ("kill", SCRIPT),
            ("help", SCRIPT),
        ],
    ),
    ("memory", &[("usage", &["read", "slow"]), ("help", SLOW)]),
    (
        "pubsub",
        &[
            ("channels", PUBSUB_SLOW),
            ("numpat", PUBSUB_SLOW),
            ("numsub", PUBSUB_SLOW),
            ("shardchannels", PUBSUB_SLOW),
            ("shardnumsub", PUBSUB_SLOW),
            ("help", SLOW),
        ],
    ),
    (
        "object",
        &[
            ("encoding", KEY_READ),
            ("freq", KEY_READ),
            ("idletime", KEY_READ),
            ("refcount", KEY_READ),
            ("help", &["keyspace", "slow"]),
        ],
    ),
    (
        "script",
        &[
            ("exists", SCRIPT),
            ("flush", SCRIPT),
            ("kill", SCRIPT),
            ("load", SCRIPT),
            ("help", SCRIPT),
        ],
    ),
    (
        "slowlog",
        &[
            ("get", ADMIN),
            ("len", ADMIN),
            ("reset", ADMIN),
            ("help", SLOW),
        ],
    ),
];

fn subcommands(command: &str) -> Option<Subcommands> {
    CONTAINERS
        .iter()
        .find(|(name, _)| *name == command)
        .map(|(_, subs)| *subs)
}

/// Whether a command, or one subcommand of a container, is in a category.
fn in_category(spec: &CommandSpec, sub: Option<&str>, category: &str) -> bool {
    if let (Some(subs), Some(sub)) = (subcommands(spec.name), sub) {
        if let Some((_, categories)) = subs.iter().find(|(name, _)| *name == sub) {
            return categories.contains(&category);
        }
    }
    let flag = |flag: &str| spec.flags.contains(&flag);
    match category {
        "read" => flag("readonly"),
        "write" => flag("write"),
        "fast" => flag("fast"),
        "slow" => !flag("fast"),
        "blocking" => flag("blocking"),
        "admin" => flag("admin"),
        "dangerous" => flag("admin") || DANGEROUS.contains(&spec.name),
        "keyspace" => {
            (spec.group == "generic" && !spec.name.starts_with("sort"))
                || matches!(spec.name, "flushall" | "flushdb" | "dbsize")
        }
        "sortedset" => spec.group == "sorted-set",
        "transaction" => spec.group == "transactions",
        "string" | "list" | "hash" | "set" | "bitmap" | "connection" | "scripting" | "pubsub" => {
            spec.group == category
        }
        _ => false,
    }
}

// --- users ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    All,
    Category(&'static str),
    Command(&'static str),
    /// One subcommand of a container, or for other commands one value of
    /// the first argument.
    Sub(&'static str, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    allow: bool,
    target: Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyPattern {
    pattern: Bytes,
    read: bool,
    write: bool,
}

/// What a command does to the keys it names.
#[derive(Clone, Copy)]
enum Access {
    Read,
    Write,
    /// Not known from the command alone (a script): either permission will
    /// do.
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub enabled: bool,
    /// Any password is accepted.
    pub nopass: bool,
    /// SHA-256 hashes of the passwords, as 64 hexadecimal digits.
    passwords: Vec<String>,
    keys: Vec<KeyPattern>,
    /// The Pub/Sub channels the user may publish and subscribe to, as glob
    /// patterns (`&pattern`), unless it may use any (`allchannels`).
    all_channels: bool,
    channels: Vec<String>,
    rules: Vec<Rule>,
}

/// Why a user may not run a command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Denied {
    /// The command, as `name` or `name|subcommand`.
    Command(String),
    Key(Bytes),
    Channel(Bytes),
}

impl User {
    /// A user as `ACL SETUSER name` creates it: disabled, without
    /// passwords, keys or commands.
    fn new() -> User {
        User {
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            keys: Vec::new(),
            all_channels: false,
            channels: Vec::new(),
            rules: vec![Rule {
                allow: false,
                target: Target::All,
            }],
        }
    }

    fn unrestricted() -> User {
        User {
            enabled: true,
            nopass: true,
            keys: vec![KeyPattern {
                pattern: Bytes::from_static(b"*"),
                read: true,
                write: true,
            }],
            all_channels: true,
            rules: vec![Rule {
                allow: true,
                target: Target::All,
            }],
            ..User::new()
        }
    }

    fn all_commands(&self) -> bool {
        matches!(
            self.rules[..],
            [Rule {
                allow: true,
                target: Target::All
            }]
        )
    }

    fn all_keys(&self) -> bool {
        self.keys
            .iter()
            .any(|key| &key.pattern[..] == b"*" && key.read && key.write)
    }

    fn password_matches(&self, password: &[u8]) -> bool {
        if self.nopass {
            return true;
        }
        let hash = sha256_hex(password);
        // Compared without stopping at the first difference, so that the
        // time taken says nothing about how close a guess was.
        self.passwords.iter().fold(false, |found, known| {
            let differing = known
                .bytes()
                .zip(hash.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b));
            found | (differing == 0 && known.len() == hash.len())
        })
    }

    /// Applies one `ACL SETUSER` rule. On failure returns the reason.
    fn apply(&mut self, rule: &str) -> Result<(), &'static str> {
        const AFTER_STAR: &str = "Adding a pattern after the * pattern (or the 'allkeys' flag) \
             is not valid and does not have any effect. Try 'resetkeys' to start with an empty \
             list of patterns";
        const AFTER_ALL_CHANNELS: &str = "Adding a pattern after the * pattern (or the \
             'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' \
             to start with an empty list of channels";
        const SYNTAX: &str = "Syntax error";

        let lower = rule.to_ascii_lowercase();
        match lower.as_str() {
            "on" => self.enabled = true,
            "off" => self.enabled = false,
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            "resetpass" => {
                self.nopass = false;
                self.passwords.clear();
            }
            "allkeys" => self.set_all_keys(),
            "resetkeys" => self.keys.clear(),
            "allchannels" => {
                self.all_channels = true;
                self.channels.clear();
            }
            "resetchannels" => {
                self.all_channels = false;
                self.channels.clear();
            }
            "allcommands" => self.set_rule(true, Target::All),
            "nocommands" => self.set_rule(false, Target::All),
            // Selectors (a second set of permissions in parentheses) are
            // not supported; there are none to clear.
            "clearselectors" => {}
            "reset" => *self = User::new(),
            _ => {
                let mut chars = rule.chars();
                let kind = chars.next().ok_or(SYNTAX)?;
                let rest = chars.as_str();
                match kind {
                    '>' => self.add_password(sha256_hex(rest.as_bytes())),
                    '<' => self.remove_password(&sha256_hex(rest.as_bytes()))?,
                    '#' | '!' => {
                        let valid = rest.len() == 64
                            && rest
                                .bytes()
                                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c));
                        if !valid {
                            return Err(
                                "The password hash must be exactly 64 characters and contain \
                                 only lowercase hexadecimal characters",
                            );
                        }
                        if kind == '#' {
                            self.add_password(rest.to_owned());
                        } else {
                            self.remove_password(rest)?;
                        }
                    }
                    '~' => {
                        if rest.is_empty() {
                            return Err(SYNTAX);
                        }
                        if rest == "*" {
                            self.set_all_keys();
                        } else {
                            self.add_key(rest, true, true).map_err(|()| AFTER_STAR)?;
                        }
                    }
                    '%' => {
                        let (flags, pattern) = rest.split_once('~').ok_or(SYNTAX)?;
                        let (read, write) = match flags.to_ascii_uppercase().as_str() {
                            "R" => (true, false),
                            "W" => (false, true),
                            "RW" | "WR" => (true, true),
                            _ => return Err(SYNTAX),
                        };
                        if pattern.is_empty() {
                            return Err(SYNTAX);
                        }
                        self.add_key(pattern, read, write)
                            .map_err(|()| AFTER_STAR)?;
                    }
                    '&' => {
                        if rest.is_empty() {
                            return Err(SYNTAX);
                        }
                        if rest == "*" {
                            self.all_channels = true;
                            self.channels.clear();
                        } else if self.all_channels {
                            return Err(AFTER_ALL_CHANNELS);
                        } else if !self.channels.iter().any(|known| known == rest) {
                            self.channels.push(rest.to_owned());
                        }
                    }
                    '+' | '-' => {
                        const UNKNOWN: &str = "Unknown command or category name in ACL";
                        let name = &lower[1..];
                        let target = if let Some(category) = name.strip_prefix('@') {
                            if category == "all" {
                                Target::All
                            } else {
                                let known = CATEGORIES.iter().find(|known| **known == category);
                                Target::Category(known.ok_or(UNKNOWN)?)
                            }
                        } else if let Some((command, sub)) = name.split_once('|') {
                            let spec = lookup(command.as_bytes()).ok_or(UNKNOWN)?;
                            let known = subcommands(spec.name)
                                .is_none_or(|subs| subs.iter().any(|(name, _)| *name == sub));
                            if !known || sub.is_empty() {
                                return Err(UNKNOWN);
                            }
                            Target::Sub(spec.name, sub.to_owned())
                        } else {
                            Target::Command(lookup(name.as_bytes()).ok_or(UNKNOWN)?.name)
                        };
                        self.set_rule(kind == '+', target);
                    }
                    '(' => return Err("Selectors are not supported by this server"),
                    _ => return Err(SYNTAX),
                }
            }
        }
        Ok(())
    }

    fn add_password(&mut self, hash: String) {
        self.nopass = false;
        if !self.passwords.contains(&hash) {
            self.passwords.push(hash);
        }
    }

    fn remove_password(&mut self, hash: &str) -> Result<(), &'static str> {
        let index = self
            .passwords
            .iter()
            .position(|known| known == hash)
            .ok_or("The password you are trying to remove from the user does not exist")?;
        self.passwords.remove(index);
        Ok(())
    }

    fn set_all_keys(&mut self) {
        self.keys = User::unrestricted().keys;
    }

    fn add_key(&mut self, pattern: &str, read: bool, write: bool) -> Result<(), ()> {
        if self.all_keys() {
            return Err(());
        }
        let pattern = KeyPattern {
            pattern: Bytes::copy_from_slice(pattern.as_bytes()),
            read,
            write,
        };
        if !self.keys.contains(&pattern) {
            self.keys.push(pattern);
        }
        Ok(())
    }

    /// Adds a command rule. A rule for everything replaces the list; any
    /// other replaces an earlier rule about the same thing.
    fn set_rule(&mut self, allow: bool, target: Target) {
        if target == Target::All {
            self.rules.clear();
        } else {
            self.rules.retain(|rule| rule.target != target);
        }
        self.rules.push(Rule { allow, target });
    }

    /// Whether the user may run `spec` with these arguments.
    pub(crate) fn permits(&self, spec: &CommandSpec, args: &[Bytes]) -> Result<(), Denied> {
        // Authenticating is open to everyone.
        if spec.flags.contains(&"no_auth") {
            return Ok(());
        }
        if !self.all_commands() {
            let sub = args.get(1).map(|arg| lossy(arg).to_ascii_lowercase());
            let mut allowed = false;
            for rule in &self.rules {
                let applies = match &rule.target {
                    Target::All => true,
                    Target::Category(category) => in_category(spec, sub.as_deref(), category),
                    Target::Command(name) => *name == spec.name,
                    Target::Sub(name, wanted) => {
                        *name == spec.name && sub.as_deref() == Some(wanted)
                    }
                };
                if applies {
                    allowed = rule.allow;
                }
            }
            if !allowed {
                return Err(Denied::Command(match sub {
                    Some(sub) if subcommands(spec.name).is_some() => {
                        format!("{}|{sub}", spec.name)
                    }
                    _ => spec.name.to_owned(),
                }));
            }
        }
        if !self.all_keys() {
            let access = if spec.flags.contains(&"write") {
                Access::Write
            } else if spec.flags.contains(&"readonly") && spec.group != "scripting" {
                Access::Read
            } else {
                Access::Any
            };
            for key in command_keys(spec, args) {
                let permitted = self.keys.iter().any(|pattern| {
                    let suits = match access {
                        Access::Read => pattern.read,
                        Access::Write => pattern.write,
                        Access::Any => true,
                    };
                    suits && glob_match(&pattern.pattern, key)
                });
                if !permitted {
                    return Err(Denied::Key(key.clone()));
                }
            }
        }
        if !self.all_channels {
            let (names, patterns): (&[Bytes], bool) = match spec.name {
                "publish" | "spublish" => (&args[1..2], false),
                "subscribe" | "ssubscribe" => (&args[1..], false),
                "psubscribe" => (&args[1..], true),
                _ => (&[], false),
            };
            for name in names {
                if !self.may_use_channel(name, patterns) {
                    return Err(Denied::Channel(name.clone()));
                }
            }
        }
        Ok(())
    }

    /// Whether the user may publish or subscribe to `name`. A pattern
    /// (`PSUBSCRIBE`) is only allowed if the user's rules contain exactly
    /// that pattern, since it could match channels the user may not see.
    fn may_use_channel(&self, name: &[u8], pattern: bool) -> bool {
        self.all_channels
            || self.channels.iter().any(|allowed| {
                if pattern {
                    allowed.as_bytes() == name
                } else {
                    glob_match(allowed.as_bytes(), name)
                }
            })
    }

    /// Whether a client with these subscriptions could still make them as
    /// this user.
    pub(crate) fn may_keep(&self, subs: &crate::pubsub::Subscriptions) -> bool {
        self.all_channels
            || (subs.channels.iter().all(|c| self.may_use_channel(c, false))
                && subs.shard.iter().all(|c| self.may_use_channel(c, false))
                && subs.patterns.iter().all(|p| self.may_use_channel(p, true)))
    }

    fn describe_keys(&self) -> String {
        let shown: Vec<String> = self
            .keys
            .iter()
            .map(|key| {
                let prefix = match (key.read, key.write) {
                    (true, false) => "%R~",
                    (false, true) => "%W~",
                    _ => "~",
                };
                format!("{prefix}{}", lossy(&key.pattern))
            })
            .collect();
        shown.join(" ")
    }

    fn describe_channels(&self) -> String {
        if self.all_channels {
            return "&*".to_owned();
        }
        let shown: Vec<String> = self.channels.iter().map(|c| format!("&{c}")).collect();
        shown.join(" ")
    }

    fn describe_commands(&self) -> String {
        let shown: Vec<String> = self
            .rules
            .iter()
            .map(|rule| {
                let sign = if rule.allow { '+' } else { '-' };
                match &rule.target {
                    Target::All => format!("{sign}@all"),
                    Target::Category(category) => format!("{sign}@{category}"),
                    Target::Command(name) => format!("{sign}{name}"),
                    Target::Sub(name, sub) => format!("{sign}{name}|{sub}"),
                }
            })
            .collect();
        shown.join(" ")
    }

    fn flags(&self) -> Vec<&'static str> {
        let mut flags = vec![if self.enabled { "on" } else { "off" }];
        if self.nopass {
            flags.push("nopass");
        }
        // Redis always shows this; `RESTORE` payloads are always checked
        // here too.
        flags.push("sanitize-payload");
        flags
    }

    /// The user as one line of `ACL LIST`, which is also valid as a `user`
    /// line in a configuration file.
    fn describe(&self, name: &str) -> String {
        let mut line = format!("user {name} {}", self.flags().join(" "));
        for hash in &self.passwords {
            line.push_str(" #");
            line.push_str(hash);
        }
        // A user limited to some channels, or none, says so first.
        let channels = if self.all_channels {
            String::new()
        } else {
            "resetchannels".to_owned()
        };
        for part in [self.describe_keys(), channels, self.describe_channels()] {
            if !part.is_empty() {
                line.push(' ');
                line.push_str(&part);
            }
        }
        line.push(' ');
        line.push_str(&self.describe_commands());
        line
    }
}

/// The keys a command names. Most follow from the command table; commands
/// that say how many keys follow need their arguments read.
pub(crate) fn command_keys<'a>(spec: &CommandSpec, args: &'a [Bytes]) -> Vec<&'a Bytes> {
    let counted = |count_at: usize| -> Vec<&'a Bytes> {
        let count = args
            .get(count_at)
            .and_then(|arg| rexyra_resp::parse_i64(arg))
            .map_or(0, |count| count.max(0) as usize);
        args.iter().skip(count_at + 1).take(count).collect()
    };
    match spec.name {
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro" | "blmpop"
        | "bzmpop" => counted(2),
        "zunion" | "zinter" | "zdiff" | "sintercard" | "zintercard" | "lmpop" | "zmpop" => {
            counted(1)
        }
        "zunionstore" | "zinterstore" | "zdiffstore" => {
            let mut keys = counted(2);
            keys.extend(args.get(1));
            keys
        }
        "sort" | "sort_ro" => {
            let mut keys: Vec<&Bytes> = args.get(1).into_iter().collect();
            if let Some(at) = args.iter().position(|arg| is(arg, "STORE")) {
                keys.extend(args.get(at + 1));
            }
            keys
        }
        "object" | "memory" => args.get(2).into_iter().collect(),
        _ if spec.first_key == 0 => Vec::new(),
        _ => {
            let last = if spec.last_key < 0 {
                args.len() as i64 + spec.last_key
            } else {
                spec.last_key
            };
            (spec.first_key..=last)
                .step_by(spec.key_step.max(1) as usize)
                .filter_map(|index| args.get(index as usize))
                .collect()
        }
    }
}

/// All users, by name.
#[derive(Debug, Clone)]
pub struct Acl {
    users: BTreeMap<String, User>,
}

impl Default for Acl {
    fn default() -> Self {
        let mut users = BTreeMap::new();
        users.insert(DEFAULT_USER.to_owned(), User::unrestricted());
        Acl { users }
    }
}

impl Acl {
    pub fn user(&self, name: &str) -> Option<&User> {
        self.users.get(name)
    }

    fn default_user(&self) -> &User {
        // The default user cannot be removed.
        &self.users[DEFAULT_USER]
    }

    /// Whether a connection has to authenticate before doing anything.
    pub fn auth_required(&self) -> bool {
        let default = self.default_user();
        !default.nopass || !default.enabled
    }

    /// Whether anyone who can reach the server can use it without a
    /// password.
    pub fn open_to_all(&self) -> bool {
        self.default_user().nopass
    }

    /// Creates or changes a user: `ACL SETUSER name rule ...`, or a `user`
    /// line of a configuration file. Either every rule applies or none.
    pub fn set_user<R: AsRef<[u8]>>(&mut self, name: &str, rules: &[R]) -> Result<(), String> {
        if name.is_empty() || name.contains([' ', '\0']) {
            return Err("ERR Usernames can't contain spaces or null characters".to_owned());
        }
        let mut user = self.users.get(name).cloned().unwrap_or_else(User::new);
        for rule in rules {
            let rule = lossy(rule.as_ref());
            if rule.is_empty() {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{rule}': Syntax error"
                ));
            }
            user.apply(&rule).map_err(|reason| {
                format!("ERR Error in ACL SETUSER modifier '{rule}': {reason}")
            })?;
        }
        self.users.insert(name.to_owned(), user);
        Ok(())
    }

    /// Gives the default user one password, or with an empty one none
    /// (`requirepass`).
    fn set_default_password(&mut self, password: &str) {
        if let Some(default) = self.users.get_mut(DEFAULT_USER) {
            default.passwords.clear();
            default.nopass = password.is_empty();
            if !password.is_empty() {
                default.passwords.push(sha256_hex(password.as_bytes()));
            }
        }
    }
}

impl Server {
    /// Sets `requirepass`: the password of the default user, or none if
    /// empty.
    pub fn set_requirepass(&mut self, password: &str) {
        self.requirepass = password.to_owned();
        self.acl.set_default_password(password);
        self.refresh_protection();
    }

    /// Recomputes whether connections from other machines are refused:
    /// they are while protected mode is on and no password is needed.
    pub fn refresh_protection(&mut self) {
        self.loopback_only.store(
            self.protected_mode && self.acl.open_to_all(),
            Ordering::Relaxed,
        );
    }
}

// --- checks -----------------------------------------------------------------

fn denied_text(user: &str, denied: &Denied, name_key: bool) -> String {
    match denied {
        Denied::Command(command) => {
            format!("User {user} has no permissions to run the '{command}' command")
        }
        Denied::Key(key) if name_key => {
            format!(
                "User {user} has no permissions to access the '{}' key",
                lossy(key)
            )
        }
        Denied::Key(_) => "No permissions to access a key".to_owned(),
        Denied::Channel(channel) if name_key => {
            format!(
                "User {user} has no permissions to access the '{}' channel",
                lossy(channel)
            )
        }
        Denied::Channel(_) => "No permissions to access a channel".to_owned(),
    }
}

/// Sets a connection up as the default user: what a new connection is, and
/// what `RESET` returns it to. It counts as logged in if the default user
/// needs no password at that moment, and stays so if one is set later.
pub fn start_session(server: &Server, client: &mut crate::ClientState) {
    client.user = DEFAULT_USER.to_owned();
    client.authenticated = !server.acl.auth_required();
}

/// Checks that the client may run a command: it has authenticated if it
/// must, and its user has the permissions. Returns the error text if not.
pub(crate) fn check(ctx: &Ctx<'_>, spec: &CommandSpec, args: &[Bytes]) -> Result<(), String> {
    // What is read back from disk was allowed when it first ran.
    if ctx.server.loading {
        return Ok(());
    }
    let acl = &ctx.server.acl;
    let open = spec.flags.contains(&"no_auth");
    if !ctx.client.authenticated && !open {
        return Err("NOAUTH Authentication required.".to_owned());
    }
    let Some(user) = acl.user(&ctx.client.user) else {
        // The user was deleted; its connections are being closed.
        return if open {
            Ok(())
        } else {
            Err("NOAUTH Authentication required.".to_owned())
        };
    };
    user.permits(spec, args)
        .map_err(|denied| format!("NOPERM {}", denied_text(&ctx.client.user, &denied, false)))
}

/// The same check for a command a script calls: scripts run with the
/// permissions of whoever called them.
pub(crate) fn check_in_script(
    ctx: &Ctx<'_>,
    spec: &CommandSpec,
    args: &[Bytes],
) -> Result<(), String> {
    if ctx.server.loading {
        return Ok(());
    }
    match ctx.server.acl.user(&ctx.client.user) {
        Some(user) => user.permits(spec, args).map_err(|denied| {
            format!(
                "ERR ACL failure in script: {}",
                denied_text(&ctx.client.user, &denied, false)
            )
        }),
        None => Ok(()),
    }
}

/// Logs the client in as `name` if the password is right.
pub(crate) fn authenticate(ctx: &mut Ctx<'_>, name: &[u8], password: &[u8]) -> Result<(), Frame> {
    let name = lossy(name);
    match ctx.server.acl.user(&name) {
        Some(user) if user.enabled && user.password_matches(password) => {
            ctx.client.user = name.into_owned();
            ctx.client.authenticated = true;
            Ok(())
        }
        _ => Err(Frame::error(ERR_WRONGPASS)),
    }
}

/// `AUTH [username] password`
pub(crate) fn auth(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (name, password) = match &args[1..] {
        [password] => {
            if ctx.server.acl.default_user().nopass {
                return Frame::error(
                    "ERR AUTH <password> called without any password configured for the \
                     default user. Are you sure your configuration is correct?",
                );
            }
            (DEFAULT_USER.as_bytes(), password)
        }
        [name, password] => (&name[..], password),
        _ => return Frame::error(ERR_SYNTAX),
    };
    match authenticate(ctx, name, password) {
        Ok(()) => Frame::ok(),
        Err(error) => error,
    }
}

// --- ACL --------------------------------------------------------------------

/// Random hexadecimal digits from the operating system's generator.
fn random_hex(digits: usize) -> Result<String, Frame> {
    let mut bytes = vec![0u8; digits.div_ceil(2)];
    getrandom::fill(&mut bytes)
        .map_err(|_| Frame::error("ERR the system random number generator is not available"))?;
    let mut text: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    text.truncate(digits);
    Ok(text)
}

fn getuser(user: &User) -> Frame {
    let bulks = |items: Vec<&str>| {
        items
            .into_iter()
            .map(|item| Frame::bulk(item.to_owned()))
            .collect()
    };
    Frame::Map(vec![
        (Frame::bulk("flags"), Frame::Set(bulks(user.flags()))),
        (
            Frame::bulk("passwords"),
            Frame::Array(bulks(user.passwords.iter().map(String::as_str).collect())),
        ),
        (
            Frame::bulk("commands"),
            Frame::bulk(user.describe_commands()),
        ),
        (Frame::bulk("keys"), Frame::bulk(user.describe_keys())),
        (
            Frame::bulk("channels"),
            Frame::bulk(user.describe_channels()),
        ),
        (Frame::bulk("selectors"), Frame::Array(Vec::new())),
    ])
}

fn category_members(category: &str) -> Vec<Frame> {
    let mut members = Vec::new();
    for spec in COMMANDS {
        match subcommands(spec.name) {
            Some(subs) => {
                for (sub, categories) in subs {
                    if categories.contains(&category) {
                        members.push(Frame::bulk(format!("{}|{sub}", spec.name)));
                    }
                }
            }
            None if in_category(spec, None, category) => members.push(Frame::bulk(spec.name)),
            None => {}
        }
    }
    members
}

fn acl_command(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Result<Frame, Frame> {
    let (sub, rest) = (&args[1], &args[2..]);
    let arity = || wrong_subcommand_arity("acl", sub);
    if is(sub, "WHOAMI") {
        if !rest.is_empty() {
            return Err(arity());
        }
        Ok(Frame::bulk(ctx.client.user.clone()))
    } else if is(sub, "USERS") {
        if !rest.is_empty() {
            return Err(arity());
        }
        let names = ctx.server.acl.users.keys();
        Ok(Frame::Array(
            names.map(|n| Frame::bulk(n.clone())).collect(),
        ))
    } else if is(sub, "LIST") {
        if !rest.is_empty() {
            return Err(arity());
        }
        let users = ctx.server.acl.users.iter();
        Ok(Frame::Array(
            users
                .map(|(name, user)| Frame::bulk(user.describe(name)))
                .collect(),
        ))
    } else if is(sub, "GETUSER") {
        let [name] = rest else {
            return Err(arity());
        };
        Ok(ctx
            .server
            .acl
            .user(&lossy(name))
            .map_or(Frame::Null, getuser))
    } else if is(sub, "SETUSER") {
        let [name, rules @ ..] = rest else {
            return Err(arity());
        };
        let name = lossy(name);
        ctx.server
            .acl
            .set_user(&name, rules)
            .map_err(Frame::error)?;
        ctx.server.refresh_protection();
        // A subscriber whose user may no longer use one of its channels is
        // disconnected, as in Redis.
        if let Some(user) = ctx.server.acl.user(&name) {
            for client in ctx.clients.values() {
                if client.user == name && !user.may_keep(&client.subscriptions) {
                    ctx.server.effects.push(Effect::Kill(client.id));
                }
            }
            if ctx.client.user == name && !user.may_keep(&ctx.client.subscriptions) {
                ctx.client.close_after_reply = true;
            }
        }
        Ok(Frame::ok())
    } else if is(sub, "DELUSER") {
        if rest.is_empty() {
            return Err(arity());
        }
        if rest.iter().any(|name| &name[..] == DEFAULT_USER.as_bytes()) {
            return Err(Frame::error("ERR The 'default' user cannot be removed"));
        }
        let mut removed = 0i64;
        for name in rest {
            let name = lossy(name);
            if ctx.server.acl.users.remove(&*name).is_none() {
                continue;
            }
            removed += 1;
            // Whoever is logged in as the user is disconnected.
            for client in ctx.clients.values() {
                if client.user == name {
                    ctx.server.effects.push(Effect::Kill(client.id));
                }
            }
            if ctx.client.user == name {
                ctx.client.close_after_reply = true;
            }
        }
        Ok(Frame::Integer(removed))
    } else if is(sub, "CAT") {
        match rest {
            [] => Ok(Frame::Array(
                CATEGORIES.iter().map(|c| Frame::bulk(*c)).collect(),
            )),
            [category] => {
                let category = lossy(category).to_ascii_lowercase();
                if !CATEGORIES.contains(&category.as_str()) {
                    return Err(Frame::error(format!("ERR Unknown category '{category}'")));
                }
                Ok(Frame::Array(category_members(&category)))
            }
            _ => Err(arity()),
        }
    } else if is(sub, "GENPASS") {
        let bits = match rest {
            [] => 256,
            [bits] => match int(bits)? {
                bits @ 1..=4096 => bits as usize,
                _ => {
                    return Err(Frame::error(
                        "ERR ACL GENPASS argument must be the number of bits for the output \
                         password, a positive number up to 4096",
                    ))
                }
            },
            _ => return Err(arity()),
        };
        Ok(Frame::bulk(random_hex(bits.div_ceil(4))?))
    } else if is(sub, "DRYRUN") {
        let [name, command @ ..] = rest else {
            return Err(arity());
        };
        if command.is_empty() {
            return Err(arity());
        }
        let name = lossy(name);
        let Some(user) = ctx.server.acl.user(&name) else {
            return Err(Frame::error(format!("ERR User '{name}' not found")));
        };
        let Some(spec) = lookup(&command[0]) else {
            return Err(Frame::error(format!(
                "ERR Command '{}' not found",
                lossy(&command[0])
            )));
        };
        let given = command.len() as i64;
        let wanted = i64::from(spec.arity);
        if (wanted > 0 && given != wanted) || (wanted < 0 && given < -wanted) {
            return Err(wrong_arity(spec.name));
        }
        Ok(match user.permits(spec, command) {
            Ok(()) => Frame::ok(),
            Err(denied) => Frame::bulk(denied_text(&name, &denied, true)),
        })
    } else if is(sub, "SAVE") || is(sub, "LOAD") {
        Err(Frame::error(
            "ERR This Redis instance is not configured to use an ACL file. You may want to \
             specify users via the ACL SETUSER command and then issue a CONFIG REWRITE \
             (assuming you have a Redis configuration file set) in order to store users in \
             the Redis configuration.",
        ))
    } else {
        Err(unknown_subcommand("ACL", sub))
    }
}

/// `ACL <SETUSER | GETUSER | DELUSER | LIST | USERS | WHOAMI | CAT | GENPASS
/// | DRYRUN> ...`
pub(crate) fn acl(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    crate::reply(acl_command(ctx, args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, bulks, err, Harness};

    const NOAUTH: &str = "NOAUTH Authentication required.";

    fn list(h: &mut Harness) -> Vec<String> {
        match h.run("ACL LIST") {
            Frame::Array(lines) => lines
                .into_iter()
                .map(|line| match line {
                    Frame::Bulk(text) => String::from_utf8(text.to_vec()).unwrap(),
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("{other:?}"),
        }
    }

    /// The `ACL LIST` line of alice after applying `rules`.
    fn alice(h: &mut Harness, rules: &str) -> String {
        assert_eq!(h.run(&format!("ACL SETUSER alice {rules}")), Frame::ok());
        list(h).remove(0)
    }

    #[test]
    fn out_of_the_box_everything_is_allowed() {
        let mut h = Harness::new();
        assert_eq!(h.run("ACL WHOAMI"), bulk("default"));
        assert_eq!(h.run("ACL USERS"), Frame::Array(bulks(&["default"])));
        assert_eq!(
            list(&mut h),
            ["user default on nopass sanitize-payload ~* &* +@all"]
        );
        assert_eq!(h.run("SET k v"), Frame::ok());
        // Any password will do for a user without one ...
        assert_eq!(h.run("AUTH default anything"), Frame::ok());
        // ... but giving only a password is taken as a mistake.
        assert_eq!(
            h.run("AUTH anything"),
            err(
                "ERR AUTH <password> called without any password configured for the default \
                 user. Are you sure your configuration is correct?"
            )
        );
        assert_eq!(h.run("AUTH a b c"), err("ERR syntax error"));
        assert_eq!(
            h.run("AUTH nobody pw"),
            err("WRONGPASS invalid username-password pair or user is disabled.")
        );
    }

    #[test]
    fn requirepass_locks_out_whoever_has_not_authenticated() {
        let mut h = Harness::new();
        assert_eq!(h.run("CONFIG SET requirepass hunter2"), Frame::ok());
        assert_eq!(
            list(&mut h),
            ["user default on sanitize-payload \
              #f52fbd32b2b3b86ff88ef6c490628285f482af15ddcb29541f94bcf526a3f6c7 ~* &* +@all"]
        );
        // Whoever was connected before the password was set carries on ...
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
        // ... a new connection has to give it.
        h.run("RESET");
        for command in [
            "GET k",
            "SET k v",
            "PING",
            "CONFIG GET requirepass",
            "ACL LIST",
            "MULTI",
        ] {
            assert_eq!(h.run(command), err(NOAUTH), "{command}");
        }
        assert_eq!(
            h.run("HELLO 3"),
            err(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise \
                 the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the \
                 client and select the RESP protocol version at the same time"
            )
        );
        assert_eq!(
            h.run("AUTH wrong"),
            err("WRONGPASS invalid username-password pair or user is disabled.")
        );
        assert_eq!(
            h.run("HELLO 3 AUTH default wrong"),
            err("WRONGPASS invalid username-password pair or user is disabled.")
        );
        assert_eq!(h.client.proto, rexyra_resp::Protocol::Resp2);
        assert_eq!(h.run("PING"), err(NOAUTH));

        assert_eq!(h.run("AUTH hunter2"), Frame::ok());
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
        assert_eq!(
            h.run("CONFIG GET requirepass"),
            Frame::Map(vec![(bulk("requirepass"), bulk("hunter2"))])
        );
        // RESET logs out again; HELLO can log in.
        assert_eq!(h.run("RESET"), Frame::simple("RESET"));
        assert_eq!(h.run("PING"), err(NOAUTH));
        assert!(matches!(
            h.run("HELLO 3 AUTH default hunter2"),
            Frame::Map(_)
        ));
        assert_eq!(h.run("PING"), Frame::simple("PONG"));

        assert_eq!(h.cmd(&["CONFIG", "SET", "requirepass", ""]), Frame::ok());
        assert_eq!(
            list(&mut h),
            ["user default on nopass sanitize-payload ~* &* +@all"]
        );
        h.run("RESET");
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
    }

    #[test]
    fn setuser_rules_are_shown_the_way_redis_shows_them() {
        let mut h = Harness::new();
        const SECRET: &str = "#2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b";
        assert_eq!(
            alice(&mut h, ""),
            "user alice off sanitize-payload resetchannels -@all"
        );
        assert_eq!(
            alice(&mut h, "on >secret ~cache:* +get +set"),
            format!(
                "user alice on sanitize-payload {SECRET} ~cache:* resetchannels -@all +get +set"
            )
        );
        // A second password; the same one again changes nothing.
        let two = alice(&mut h, ">second >secret +@list -lpush");
        assert!(two.contains(&format!(
            "{SECRET} #16367aacb67a4a017c8da8ab95682ccb390863780f7114dda0a0e0c55644c7c4 ~cache:*"
        )));
        assert!(two.ends_with("-@all +get +set +@list -lpush"));
        assert_eq!(
            h.run("ACL SETUSER alice <notthere"),
            err(
                "ERR Error in ACL SETUSER modifier '<notthere': The password you are trying to \
                 remove from the user does not exist"
            )
        );
        // A later rule about the same thing replaces the earlier one.
        assert!(
            alice(&mut h, "<second -get +get -@list").ends_with(&format!(
                "{SECRET} ~cache:* resetchannels -@all +set -lpush +get -@list"
            ))
        );
        assert!(alice(&mut h, "+@all -@dangerous").ends_with("resetchannels +@all -@dangerous"));
        assert!(alice(&mut h, "-@all +config|get +client|list -client|list")
            .ends_with("-@all +config|get -client|list"));
        assert!(
            alice(&mut h, "nocommands +@read %R~ro:* %W~wo:* %RW~rw:* ~all:*")
                .ends_with("~cache:* %R~ro:* %W~wo:* ~rw:* ~all:* resetchannels -@all +@read")
        );
        assert!(
            alice(&mut h, "allkeys").ends_with(&format!("{SECRET} ~* resetchannels -@all +@read"))
        );
        assert!(alice(&mut h, "resetkeys ~a ~b ~a").contains(" ~a ~b resetchannels "));
        assert!(alice(&mut h, "&chan:*").contains(" ~a ~b resetchannels &chan:* -@all"));
        assert!(alice(&mut h, "allchannels").contains(" ~a ~b &* -@all"));
        assert_eq!(
            alice(&mut h, "nopass"),
            "user alice on nopass sanitize-payload ~a ~b &* -@all +@read"
        );
        assert!(alice(&mut h, ">pw").starts_with("user alice on sanitize-payload #30c952fab1"));
        assert_eq!(
            alice(&mut h, "resetpass"),
            "user alice on sanitize-payload ~a ~b &* -@all +@read"
        );
        assert_eq!(
            alice(&mut h, "reset"),
            "user alice off sanitize-payload resetchannels -@all"
        );
    }

    #[test]
    fn setuser_errors_change_nothing() {
        let mut h = Harness::new();
        h.run("ACL SETUSER alice on >pw");
        let before = list(&mut h);
        for (rules, reason) in [
            (
                "+get +nosuchcommand",
                "'+nosuchcommand': Unknown command or category name in ACL",
            ),
            (
                "+@nosuchcategory",
                "'+@nosuchcategory': Unknown command or category name in ACL",
            ),
            ("~x bogus", "'bogus': Syntax error"),
            (
                "+config|nosuch",
                "'+config|nosuch': Unknown command or category name in ACL",
            ),
            (
                "#abc",
                "'#abc': The password hash must be exactly 64 characters and contain only \
                 lowercase hexadecimal characters",
            ),
            (
                "allkeys ~more",
                "'~more': Adding a pattern after the * pattern",
            ),
            ("%X~k", "'%X~k': Syntax error"),
        ] {
            match h.run(&format!("ACL SETUSER alice {rules}")) {
                Frame::Error(text) => assert!(
                    text.starts_with(&format!("ERR Error in ACL SETUSER modifier {reason}")),
                    "{text}"
                ),
                other => panic!("{rules}: {other:?}"),
            }
            assert_eq!(list(&mut h), before, "{rules}");
        }
        assert_eq!(
            h.cmd(&["ACL", "SETUSER", "bad name", "on"]),
            err("ERR Usernames can't contain spaces or null characters")
        );
        // A password hash can be given and taken away directly.
        let hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8";
        assert_eq!(h.run(&format!("ACL SETUSER alice #{hash}")), Frame::ok());
        assert_eq!(h.run("AUTH alice password"), Frame::ok());
        assert!(matches!(
            h.run(&format!("ACL SETUSER default !{hash}")),
            Frame::Error(_)
        ));
    }

    fn restricted() -> Harness {
        let mut h = Harness::new();
        h.run("MSET cache:1 v other v ro:1 v");
        assert_eq!(
            h.run(
                "ACL SETUSER alice on >secret ~cache:* %R~ro:* -@all +get +set +mget +eval \
                 +multi +exec +lrange +config|get +acl|whoami +zunionstore +sort"
            ),
            Frame::ok()
        );
        h.run("ACL SETUSER bob off >bobpw allkeys +@all");
        h
    }

    #[test]
    fn a_user_is_held_to_its_commands_and_keys() {
        let mut h = restricted();
        let wrongpass = err("WRONGPASS invalid username-password pair or user is disabled.");
        assert_eq!(h.run("AUTH bob bobpw"), wrongpass, "bob is off");
        assert_eq!(h.run("AUTH alice wrong"), wrongpass);
        assert_eq!(h.run("AUTH alice secret"), Frame::ok());
        assert_eq!(h.run("ACL WHOAMI"), bulk("alice"));

        let no_key = err("NOPERM No permissions to access a key");
        let no_command = |name: &str| {
            err(&format!(
                "NOPERM User alice has no permissions to run the '{name}' command"
            ))
        };
        assert_eq!(h.run("GET cache:1"), bulk("v"));
        assert_eq!(h.run("SET cache:2 v"), Frame::ok());
        assert_eq!(h.run("GET other"), no_key);
        assert_eq!(h.run("MGET cache:1 other"), no_key);
        assert_eq!(h.run("DEL cache:1"), no_command("del"));
        // Read-only keys can be read but not written.
        assert_eq!(h.run("GET ro:1"), bulk("v"));
        assert_eq!(h.run("SET ro:1 v"), no_key);
        // Subcommands are allowed one by one.
        assert!(matches!(h.run("CONFIG GET maxmemory"), Frame::Map(_)));
        assert_eq!(h.run("CONFIG SET maxmemory 0"), no_command("config|set"));
        assert_eq!(h.run("ACL LIST"), no_command("acl|list"));
        assert_eq!(h.run("ACL SETUSER alice +@all"), no_command("acl|setuser"));
        // Commands that say how many keys follow.
        assert_eq!(h.run("ZUNIONSTORE cache:z 1 other"), no_key);
        assert_eq!(h.run("ZUNIONSTORE other 1 cache:z"), no_key);
        assert_eq!(h.run("ZUNIONSTORE cache:z 1 cache:y"), Frame::Integer(0));
        assert_eq!(h.run("SORT cache:l STORE other"), no_key);
        // Authenticating as someone else, and leaving, are always possible.
        assert_eq!(h.run("PING"), no_command("ping"));
        assert_eq!(h.run("AUTH default x"), Frame::ok());
        assert_eq!(h.run("GET other"), bulk("v"));
    }

    #[test]
    fn scripts_and_transactions_run_with_the_callers_permissions() {
        let mut h = restricted();
        h.run("AUTH alice secret");
        let eval = |h: &mut Harness, script: &str, key: &str| h.cmd(&["EVAL", script, "1", key]);
        assert_eq!(
            eval(&mut h, "return redis.call('GET', KEYS[1])", "cache:1"),
            bulk("v")
        );
        assert_eq!(
            eval(&mut h, "return redis.call('GET', KEYS[1])", "other"),
            err("NOPERM No permissions to access a key")
        );
        match eval(&mut h, "return redis.call('DEL', KEYS[1])", "cache:1") {
            Frame::Error(text) => assert!(
                text.starts_with(
                    "ERR ACL failure in script: User alice has no permissions to run the 'del' \
                     command script: "
                ),
                "{text}"
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            eval(&mut h, "return redis.pcall('DEL', KEYS[1])", "cache:1"),
            err(
                "ERR ACL failure in script: User alice has no permissions to run the 'del' command"
            )
        );
        // A key the script did not declare is checked when it is used.
        match h.cmd(&["EVAL", "return redis.call('GET', 'other')", "0"]) {
            Frame::Error(text) => assert!(
                text.starts_with("ERR ACL failure in script: No permissions to access a key"),
                "{text}"
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(h.run("GET cache:1"), bulk("v"));

        // A refused command spoils the transaction.
        assert_eq!(h.run("MULTI"), Frame::ok());
        assert_eq!(h.run("SET cache:2 v"), Frame::simple("QUEUED"));
        assert_eq!(
            h.run("DEL cache:2"),
            err("NOPERM User alice has no permissions to run the 'del' command")
        );
        assert_eq!(
            h.run("EXEC"),
            err("EXECABORT Transaction discarded because of previous errors.")
        );
        // Permissions taken away after queueing count when it runs.
        h.run("MULTI");
        h.run("SET cache:3 v");
        h.server.acl.set_user("alice", &["-set"]).unwrap();
        assert_eq!(
            h.run("EXEC"),
            Frame::Array(vec![err(
                "NOPERM User alice has no permissions to run the 'set' command"
            )])
        );
    }

    #[test]
    fn dryrun_names_what_is_missing() {
        let mut h = restricted();
        for (command, verdict) in [
            ("get cache:1", None),
            (
                "get other",
                Some("User alice has no permissions to access the 'other' key"),
            ),
            (
                "del cache:1",
                Some("User alice has no permissions to run the 'del' command"),
            ),
            (
                "set ro:1 v",
                Some("User alice has no permissions to access the 'ro:1' key"),
            ),
            ("get ro:1", None),
            (
                "config set maxmemory 0",
                Some("User alice has no permissions to run the 'config|set' command"),
            ),
            ("config get maxmemory", None),
        ] {
            assert_eq!(
                h.run(&format!("ACL DRYRUN alice {command}")),
                verdict.map_or(Frame::ok(), bulk),
                "{command}"
            );
        }
        assert_eq!(
            h.run("ACL DRYRUN nobody get x"),
            err("ERR User 'nobody' not found")
        );
        assert_eq!(
            h.run("ACL DRYRUN alice nosuch x"),
            err("ERR Command 'nosuch' not found")
        );
        assert_eq!(
            h.run("ACL DRYRUN alice get"),
            err("ERR wrong number of arguments for 'get' command")
        );
        // Nothing was run.
        assert_eq!(h.run("EXISTS cache:1"), Frame::Integer(1));
    }

    #[test]
    fn categories() {
        let mut h = Harness::new();
        let Frame::Array(all) = h.run("ACL CAT") else {
            panic!("not an array")
        };
        assert_eq!(all.len(), 21);
        let members = |h: &mut Harness, category: &str| -> Vec<String> {
            match h.run(&format!("ACL CAT {category}")) {
                Frame::Array(items) => items
                    .into_iter()
                    .map(|item| match item {
                        Frame::Bulk(text) => String::from_utf8(text.to_vec()).unwrap(),
                        other => panic!("{other:?}"),
                    })
                    .collect(),
                other => panic!("{category}: {other:?}"),
            }
        };
        let has = |list: &[String], name: &str| list.iter().any(|item| item == name);
        let dangerous = members(&mut h, "dangerous");
        for name in [
            "flushall",
            "keys",
            "config|set",
            "acl|setuser",
            "client|kill",
            "shutdown",
        ] {
            assert!(has(&dangerous, name), "{name}");
        }
        for name in ["get", "acl|whoami", "client|setname", "ping"] {
            assert!(!has(&dangerous, name), "{name}");
        }
        let transaction = members(&mut h, "transaction");
        assert_eq!(transaction.len(), 5, "{transaction:?}");
        assert!(has(&members(&mut h, "sortedset"), "zadd"));
        assert!(has(&members(&mut h, "scripting"), "function|load"));
        assert!(members(&mut h, "stream").is_empty());
        assert_eq!(
            h.run("ACL CAT nosuch"),
            err("ERR Unknown category 'nosuch'")
        );

        // Categories work as rules.
        h.run("ACL SETUSER carol on nopass allkeys +@all -@dangerous");
        h.run("AUTH carol x");
        assert_eq!(h.run("SET k v"), Frame::ok());
        assert_eq!(h.run("CLIENT SETNAME app"), Frame::ok());
        for (command, name) in [
            ("FLUSHALL", "flushall"),
            ("KEYS *", "keys"),
            ("CONFIG GET port", "config|get"),
            ("CLIENT LIST", "client|list"),
        ] {
            assert_eq!(
                h.run(command),
                err(&format!(
                    "NOPERM User carol has no permissions to run the '{name}' command"
                )),
                "{command}"
            );
        }
    }

    #[test]
    fn deleting_a_user_disconnects_it() {
        let mut h = restricted();
        let mut other = crate::ClientState::new(8, "127.0.0.1:50001".to_owned());
        other.user = "alice".to_owned();
        h.clients.insert(8, other);
        h.clients
            .insert(9, crate::ClientState::new(9, "127.0.0.1:50002".to_owned()));
        assert_eq!(
            h.run("ACL DELUSER default"),
            err("ERR The 'default' user cannot be removed")
        );
        assert_eq!(h.run("ACL DELUSER alice bob nobody"), Frame::Integer(2));
        assert_eq!(h.server.effects, [Effect::Kill(8)]);
        assert_eq!(h.run("ACL USERS"), Frame::Array(bulks(&["default"])));
        assert_eq!(h.run("ACL GETUSER alice"), Frame::Null);
    }

    #[test]
    fn getuser_and_genpass() {
        let mut h = restricted();
        assert_eq!(
            h.run("ACL GETUSER alice"),
            Frame::Map(vec![
                (
                    bulk("flags"),
                    Frame::Set(bulks(&["on", "sanitize-payload"]))
                ),
                (
                    bulk("passwords"),
                    Frame::Array(bulks(&[
                        "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
                    ]))
                ),
                (
                    bulk("commands"),
                    bulk(
                        "-@all +get +set +mget +eval +multi +exec +lrange +config|get \
                         +acl|whoami +zunionstore +sort"
                    )
                ),
                (bulk("keys"), bulk("~cache:* %R~ro:*")),
                (bulk("channels"), bulk("")),
                (bulk("selectors"), Frame::Array(Vec::new())),
            ])
        );
        let length = |h: &mut Harness, command: &str| match h.run(command) {
            Frame::Bulk(text) => {
                assert!(text.iter().all(u8::is_ascii_hexdigit));
                text.len()
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(length(&mut h, "ACL GENPASS"), 64);
        assert_eq!(length(&mut h, "ACL GENPASS 32"), 8);
        assert_eq!(length(&mut h, "ACL GENPASS 5"), 2);
        assert_ne!(h.run("ACL GENPASS"), h.run("ACL GENPASS"));
        for bits in ["0", "5000"] {
            assert!(
                matches!(h.run(&format!("ACL GENPASS {bits}")), Frame::Error(text) if text.contains("up to 4096"))
            );
        }
        assert_eq!(
            h.run("ACL NOPE"),
            err("ERR unknown subcommand 'NOPE'. Try ACL HELP.")
        );
        assert!(matches!(h.run("ACL SAVE"), Frame::Error(text) if text.contains("ACL file")));
    }

    #[test]
    fn passwords_do_not_reach_the_slow_log() {
        let mut h = Harness::new();
        h.run("CONFIG SET slowlog-log-slower-than 0");
        h.run("ACL SETUSER alice on >topsecret ~* +@all");
        h.run("AUTH alice topsecret");
        h.run("HELLO 3 AUTH alice topsecret");
        h.run("CONFIG SET requirepass topsecret");
        h.run("AUTH topsecret");
        h.run("SET k v");
        let log = format!("{:?}", h.run("SLOWLOG GET 100"));
        assert!(log.contains("SET"), "{log}");
        assert!(!log.contains("topsecret"), "{log}");
    }
}
