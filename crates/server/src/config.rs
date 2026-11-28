//! Start-up configuration: a `redis.conf`-style file, command-line flags, or
//! both (flags win).

use std::fmt;

use rexyra_commands::aof::{AofSettings, Fsync};
use rexyra_commands::Policy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub maxclients: usize,
    /// Microseconds; see `slowlog-log-slower-than` in Redis.
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    /// Milliseconds a script may run before other clients are told the
    /// server is busy; see `busy-reply-threshold` in Redis.
    pub busy_reply_threshold: u64,
    /// Directory and file name of the snapshot (`dump.rdb` in the working
    /// directory by default).
    pub dir: String,
    pub dbfilename: String,
    /// The file the log is written to; empty for standard error.
    pub logfile: String,
    /// The size at which the log file is rolled (zero: never), and how many
    /// earlier files are kept; see [`crate::logfile`].
    pub logfile_max_size: u64,
    pub logfile_max_files: u32,
    /// The append-only file; see [`AofSettings`].
    pub aof: AofSettings,
    /// Automatic saves as `(seconds, changes)` pairs; see `save` in Redis.
    pub save_points: Vec<(u64, u64)>,
    /// Whether `save` was set explicitly. The first `save` directive
    /// replaces the default save points; later ones add to it.
    pub save_given: bool,
    /// The memory limit in bytes (zero: none) and what happens when it is
    /// reached; see `maxmemory` and `maxmemory-policy` in Redis.
    pub maxmemory: u64,
    pub maxmemory_policy: Policy,
    pub maxmemory_samples: usize,
    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,
    /// The password of the default user; empty for none.
    pub requirepass: String,
    /// Refuse connections from other machines while no password is set.
    pub protected_mode: bool,
    /// Who may run `DEBUG`: `no`, `yes` or `local`.
    pub enable_debug_command: rexyra_commands::DebugCommand,
    /// The keyspace notifications to publish (`notify-keyspace-events`),
    /// as `rexyra_commands::notify` flags; none by default.
    pub notify_keyspace_events: u32,
    /// The most keys client-side caching remembers before it invalidates
    /// some to make room (`tracking-table-max-keys`); zero for no limit.
    pub tracking_table_max_keys: u64,
    /// `user` lines: a name followed by `ACL SETUSER` rules.
    pub users: Vec<Vec<String>>,
    /// The configuration file this was read from, if any.
    pub config_file: Option<String>,
    /// Directives found in the configuration file that this server does not
    /// support. They are skipped, so an existing `redis.conf` still works;
    /// the caller should tell the user.
    pub ignored: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Loopback only unless asked otherwise, as in Redis.
            bind: "127.0.0.1".to_owned(),
            port: 6379,
            databases: 16,
            maxclients: 10_000,
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            busy_reply_threshold: 5_000,
            dir: ".".to_owned(),
            dbfilename: "dump.rdb".to_owned(),
            logfile: String::new(),
            logfile_max_size: 100 * 1024 * 1024,
            logfile_max_files: 5,
            aof: AofSettings::default(),
            save_points: vec![(3600, 1), (300, 100), (60, 10_000)],
            save_given: false,
            maxmemory: 0,
            maxmemory_policy: Policy::NoEviction,
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            requirepass: String::new(),
            protected_mode: true,
            enable_debug_command: rexyra_commands::DebugCommand::No,
            notify_keyspace_events: 0,
            tracking_table_max_keys: rexyra_commands::tracking::DEFAULT_MAX_KEYS,
            users: Vec::new(),
            config_file: None,
            ignored: Vec::new(),
        }
    }
}

// Built once at start-up, so the size of `Run` does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq)]
pub enum ArgsOutcome {
    Run(Config),
    /// `--create-config PATH`: write the configuration to that file and exit.
    CreateConfig(String, Config),
    Help,
    Version,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ArgsError(String);

impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ArgsError {}

pub const USAGE: &str = "\
Usage: rexyra-server [CONFIG_FILE] [OPTIONS]

CONFIG_FILE is a redis.conf-style file. Options override it.

Options:
  --bind <ADDR>                     Address to listen on          [default: 127.0.0.1]
  --port <PORT>                     TCP port to listen on         [default: 6379]
  --databases <N>                   Number of logical databases   [default: 16]
  --maxclients <N>                  Maximum simultaneous clients  [default: 10000]
  --slowlog-log-slower-than <USEC>  Slow log threshold, -1 = off  [default: 10000]
  --slowlog-max-len <N>             Slow log capacity             [default: 128]
  --busy-reply-threshold <MSEC>     Script run time before others
                                    get BUSY, 0 = never           [default: 5000]
  --save '<SEC> <CHANGES> ...'      Save automatically after SEC
                                    seconds if CHANGES keys changed;
                                    '' = never   [default: '3600 1 300 100 60 10000']
  --appendonly <yes|no>             Log every change to an
                                    append-only file              [default: no]
  --appendfsync <always|everysec|no>
                                    When that log is forced to disk
                                                                  [default: everysec]
  --requirepass <PASSWORD>          Password clients must give with
                                    AUTH                          [default: none]
  --protected-mode <yes|no>         Without a password, accept only
                                    connections from this machine [default: yes]
  --enable-debug-command <no|yes|local>
                                    Who may run DEBUG; local = clients
                                    on this machine               [default: no]
  --maxmemory <BYTES>               Memory limit, e.g. 512mb;
                                    0 = none                      [default: 0]
  --maxmemory-policy <POLICY>       What to evict at the limit: noeviction,
                                    allkeys-lru, allkeys-lfu, allkeys-random,
                                    volatile-lru, volatile-lfu, volatile-random,
                                    volatile-ttl                  [default: noeviction]
  --dir <PATH>                      Directory of the snapshot     [default: .]
  --dbfilename <NAME>               File name of the snapshot     [default: dump.rdb]
  --logfile <PATH>                  File the log is written to    [default: standard error]
  --logfile-max-size <BYTES>        Size at which the log file is
                                    started afresh, the old one kept
                                    as NAME.1.log; 0 = never      [default: 100mb]
  --logfile-max-files <N>           Earlier log files kept        [default: 5]
  --service                         Run under the Windows service manager. For
                                    a service's command line, not a terminal
  --create-config <PATH>            Write a configuration file holding the
                                    other options to PATH, unless that file
                                    exists, and exit
  -h, --help                        Print this help
  -v, --version                     Print the version
";

/// Splits one configuration line into words. Double quotes allow spaces and
/// backslash escapes; single quotes are literal.
fn split_line(line: &str) -> Result<Vec<String>, &'static str> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else {
            return Ok(words);
        };
        let mut word = String::new();
        if first == '"' || first == '\'' {
            chars.next();
            loop {
                match chars.next() {
                    None => return Err("unbalanced quotes"),
                    Some(c) if c == first => break,
                    Some('\\') if first == '"' => word.push(match chars.next() {
                        Some('n') => '\n',
                        Some('r') => '\r',
                        Some('t') => '\t',
                        Some(other) => other,
                        None => return Err("unbalanced quotes"),
                    }),
                    Some(c) => word.push(c),
                }
            }
            if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                return Err("closing quote must be followed by a space");
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                word.push(c);
            }
        }
        words.push(word);
    }
}

impl Config {
    /// Applies one directive. `Ok(false)` means this server does not
    /// support it.
    fn apply(&mut self, name: &str, values: &[String]) -> Result<bool, String> {
        let single = || match values {
            [value] => Ok(value.as_str()),
            _ => Err(format!("'{name}' takes exactly one value")),
        };
        let invalid = |value: &str| format!("invalid value '{value}' for '{name}'");
        match name {
            "bind" => {
                // Redis accepts several addresses; this server listens on
                // one. A leading '-' ("ignore if unavailable") is dropped.
                let first = values
                    .first()
                    .ok_or_else(|| format!("'{name}' needs a value"))?;
                self.bind = first.trim_start_matches('-').to_owned();
            }
            "port" => {
                let value = single()?;
                self.port = value.parse().map_err(|_| invalid(value))?;
            }
            "databases" => {
                let value = single()?;
                self.databases = value
                    .parse()
                    .ok()
                    .filter(|&n| n >= 1)
                    .ok_or_else(|| invalid(value))?;
            }
            "maxclients" => {
                let value = single()?;
                self.maxclients = value
                    .parse()
                    .ok()
                    .filter(|&n| n >= 1)
                    .ok_or_else(|| invalid(value))?;
            }
            "slowlog-log-slower-than" => {
                let value = single()?;
                self.slowlog_log_slower_than = value.parse().map_err(|_| invalid(value))?;
            }
            "slowlog-max-len" => {
                let value = single()?;
                self.slowlog_max_len = value.parse().map_err(|_| invalid(value))?;
            }
            "tracking-table-max-keys" => {
                let value = single()?;
                self.tracking_table_max_keys = value
                    .parse()
                    .ok()
                    .filter(|&n| i64::try_from(n).is_ok())
                    .ok_or_else(|| invalid(value))?;
            }
            // `save 900 1` adds a save point; the first one replaces the
            // defaults, and `save ""` turns automatic saving off.
            "save" => {
                let text = values.join(" ");
                let points = rexyra_commands::persistence::parse_save_points(&text)
                    .ok_or_else(|| invalid(&text))?;
                if !std::mem::replace(&mut self.save_given, true) || points.is_empty() {
                    self.save_points.clear();
                }
                self.save_points.extend(points);
            }
            "requirepass" => self.requirepass = single()?.to_owned(),
            "user" => {
                if values.is_empty() {
                    return Err(format!("'{name}' needs a user name"));
                }
                self.users.push(values.to_vec());
            }
            "appendonly" | "aof-load-truncated" | "protected-mode" => {
                let value = single()?;
                let flag = match value.to_ascii_lowercase().as_str() {
                    "yes" => true,
                    "no" => false,
                    _ => return Err(invalid(value)),
                };
                match name {
                    "appendonly" => self.aof.enabled = flag,
                    "protected-mode" => self.protected_mode = flag,
                    _ => self.aof.load_truncated = flag,
                }
            }
            "appendfsync" => {
                let value = single()?;
                self.aof.fsync = Fsync::parse(value).ok_or_else(|| invalid(value))?;
            }
            "enable-debug-command" => {
                let value = single()?;
                self.enable_debug_command =
                    rexyra_commands::DebugCommand::parse(value).ok_or_else(|| invalid(value))?;
            }
            // Case matters: `K` and `k` are not the same.
            "notify-keyspace-events" => {
                let value = single()?;
                self.notify_keyspace_events = rexyra_commands::notify::parse(value.as_bytes())
                    .ok_or_else(|| {
                        format!(
                            "invalid value '{value}' for '{name}': {}",
                            rexyra_commands::notify::ERR_INVALID
                        )
                    })?;
            }
            "appenddirname" | "appendfilename" => {
                let value = single()?;
                if value.is_empty() || value.contains(['/', '\\']) {
                    return Err(format!("'{name}' must be a name, not a path"));
                }
                if name == "appenddirname" {
                    self.aof.dirname = value.to_owned();
                } else {
                    self.aof.filename = value.to_owned();
                }
            }
            "auto-aof-rewrite-percentage" => {
                let value = single()?;
                self.aof.rewrite_percentage = value.parse().map_err(|_| invalid(value))?;
            }
            "auto-aof-rewrite-min-size" => {
                let value = single()?;
                self.aof.rewrite_min_size =
                    rexyra_commands::aof::parse_size(value).ok_or_else(|| invalid(value))?;
            }
            "maxmemory" => {
                let value = single()?;
                self.maxmemory =
                    rexyra_commands::aof::parse_size(value).ok_or_else(|| invalid(value))?;
            }
            "maxmemory-policy" => {
                let value = single()?;
                self.maxmemory_policy = Policy::parse(value).ok_or_else(|| invalid(value))?;
            }
            "maxmemory-samples" => {
                let value = single()?;
                self.maxmemory_samples = value
                    .parse()
                    .ok()
                    .filter(|samples| (1..=64).contains(samples))
                    .ok_or_else(|| invalid(value))?;
            }
            "lfu-log-factor" | "lfu-decay-time" => {
                let value = single()?;
                let number = value.parse().map_err(|_| invalid(value))?;
                if name == "lfu-log-factor" {
                    self.lfu_log_factor = number;
                } else {
                    self.lfu_decay_time = number;
                }
            }
            "dir" => self.dir = single()?.to_owned(),
            "logfile" => self.logfile = single()?.to_owned(),
            "logfile-max-size" => {
                let value = single()?;
                self.logfile_max_size =
                    rexyra_commands::aof::parse_size(value).ok_or_else(|| invalid(value))?;
            }
            "logfile-max-files" => {
                let value = single()?;
                self.logfile_max_files = value
                    .parse()
                    .ok()
                    .filter(|&n| n <= 1000)
                    .ok_or_else(|| invalid(value))?;
            }
            "dbfilename" => {
                let value = single()?;
                if value.contains(['/', '\\']) {
                    return Err(format!("'{name}' must be a file name, not a path"));
                }
                self.dbfilename = value.to_owned();
            }
            // `lua-time-limit` is the older name of the same setting.
            "busy-reply-threshold" | "lua-time-limit" => {
                let value = single()?;
                self.busy_reply_threshold = value.parse().map_err(|_| invalid(value))?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Applies the contents of a configuration file: one directive per line,
    /// `#` starts a comment line. Unsupported directives are collected in
    /// [`Config::ignored`]; a supported directive with a bad value is an
    /// error.
    pub fn apply_file(&mut self, text: &str) -> Result<(), ArgsError> {
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fail = |why: &str| ArgsError(format!("config file line {}: {why}", index + 1));
            let words = split_line(line).map_err(fail)?;
            let Some((name, values)) = words.split_first() else {
                continue;
            };
            let name = name.to_ascii_lowercase();
            if !self.apply(&name, values).map_err(|why| fail(&why))? {
                self.ignored.push(name);
            }
        }
        Ok(())
    }

    /// Parses `[config-file] [--name value ...]`, the same style
    /// `redis-server` accepts.
    pub fn from_args<I, S>(args: I) -> Result<ArgsOutcome, ArgsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut config = Config::default();
        // `--service` says how the process is run, not how the server is
        // set up; `main` looks for it. `--create-config` takes the path of
        // the file to write. Either may come before the configuration file.
        let mut create = None;
        let mut rest = Vec::new();
        let mut given = args.into_iter();
        while let Some(arg) = given.next() {
            match arg.as_ref() {
                "--service" => {}
                "--create-config" => {
                    let path = given.next().ok_or_else(|| {
                        ArgsError("option '--create-config' needs a value".to_owned())
                    })?;
                    create = Some(path.as_ref().to_owned());
                }
                other => rest.push(other.to_owned()),
            }
        }
        let mut args = rest.into_iter().peekable();

        if let Some(path) = args.next_if(|arg| !arg.starts_with('-')) {
            let path = path.as_str();
            let text = std::fs::read_to_string(path)
                .map_err(|e| ArgsError(format!("cannot read config file '{path}': {e}")))?;
            config.apply_file(&text)?;
            config.config_file = Some(path.to_owned());
        }

        while let Some(flag) = args.next() {
            let flag = flag.as_str();
            match flag {
                "-h" | "--help" => return Ok(ArgsOutcome::Help),
                "-v" | "--version" => return Ok(ArgsOutcome::Version),
                _ => {}
            }
            let name = flag
                .strip_prefix("--")
                .ok_or_else(|| ArgsError(format!("unknown option '{flag}'")))?;
            let value = args
                .next()
                .ok_or_else(|| ArgsError(format!("option '{flag}' needs a value")))?;
            match config.apply(name, &[value]) {
                Ok(true) => {}
                Ok(false) => return Err(ArgsError(format!("unknown option '{flag}'"))),
                Err(why) => {
                    return Err(ArgsError(
                        why.replace(&format!("'{name}'"), &format!("'{flag}'")),
                    ))
                }
            }
        }
        Ok(match create {
            Some(path) => ArgsOutcome::CreateConfig(path, config),
            None => ArgsOutcome::Run(config),
        })
    }

    /// The text of a configuration file that starts a server set up like
    /// this one. It lists every setting there is ([`DIRECTIVES`]): the
    /// address, port and data folder always, the others written out where
    /// they differ from the default and otherwise as a comment showing the
    /// default, so that whoever edits the file has all of them in front of
    /// them.
    pub fn starter_file(&self) -> String {
        let defaults = Config::default();
        let yes_no = |flag: bool| if flag { "yes" } else { "no" }.to_owned();
        let mut file = Starter {
            text: String::from(
                "# Rexyra configuration.\n\
                 #\n\
                 # Every setting is listed here. A line that starts with # is a comment:\n\
                 # a setting left commented has the value shown, which is its default.\n\
                 # To change one, remove the # and edit the value, then restart the server\n\
                 # (or the Rexyra service). Settings changed while the server runs, with\n\
                 # CONFIG SET, are written back to this file by CONFIG REWRITE.\n\
                 #\n\
                 # Sizes take a unit: 1kb is 1024 bytes, 1mb 1024 kb, 1gb 1024 mb.\n\
                 # A path with spaces goes in single quotes: dir 'C:\\My Data\\rexyra'\n\
                 # Inside double quotes a backslash starts an escape, as in redis.conf.\n\
                 #\n\
                 # More about each setting: https://github.com/moznhub/rexyra#configuration\n",
            ),
        };

        file.section("Network");
        file.about(
            "The address and port to listen on. 127.0.0.1 accepts connections from\n\
             this machine only; 0.0.0.0 accepts them on every network interface.",
        );
        file.active("bind", &quoted(&self.bind));
        file.active("port", &self.port.to_string());
        file.about(
            "While there is no password, refuse connections from other machines\n\
             whatever 'bind' says.",
        );
        file.setting(
            "protected-mode",
            &yes_no(self.protected_mode),
            self.protected_mode == defaults.protected_mode,
        );
        file.about("The most clients connected at one time.");
        file.setting(
            "maxclients",
            &self.maxclients.to_string(),
            self.maxclients == defaults.maxclients,
        );

        file.section("Security");
        file.about("A password clients must give with AUTH. Without one, anyone who can connect\nmay run any command.");
        if self.requirepass.is_empty() {
            file.example("requirepass", "a-long-random-password");
        } else {
            file.active("requirepass", &quoted(&self.requirepass));
        }
        file.about(
            "Further users, each with a name and the rules ACL SETUSER takes. This one\n\
             may only use the list commands, on keys that start with jobs:",
        );
        if self.users.is_empty() {
            file.example("user", "worker on >another-long-password ~jobs:* +@list");
        }
        for user in &self.users {
            let words: Vec<String> = user.iter().map(|word| quoted(word)).collect();
            file.active("user", &words.join(" "));
        }
        file.about(
            "Who may run the DEBUG command: no, yes, or local for clients on this\n\
             machine only.",
        );
        file.setting(
            "enable-debug-command",
            self.enable_debug_command.name(),
            self.enable_debug_command == defaults.enable_debug_command,
        );

        file.section("General");
        file.about("The number of databases, which SELECT chooses between.");
        file.setting(
            "databases",
            &self.databases.to_string(),
            self.databases == defaults.databases,
        );
        file.about("The file the log is written to. Without one it goes to standard error.");
        if self.logfile.is_empty() {
            file.example("logfile", "rexyra.log");
        } else {
            file.active("logfile", &quoted(&self.logfile));
        }
        file.about(
            "When the log file reaches this size it is renamed rexyra.1.log (the one\n\
             before becomes rexyra.2.log, and so on) and a new one is started. This\n\
             many earlier files are kept. A size of 0 lets the file grow for ever.",
        );
        file.setting(
            "logfile-max-size",
            &size(self.logfile_max_size),
            self.logfile_max_size == defaults.logfile_max_size,
        );
        file.setting(
            "logfile-max-files",
            &self.logfile_max_files.to_string(),
            self.logfile_max_files == defaults.logfile_max_files,
        );

        file.section("Snapshots");
        file.about(
            "The folder the data files are kept in, which must exist, and the name of\n\
             the snapshot file in it.",
        );
        file.active("dir", &quoted(&self.dir));
        file.setting(
            "dbfilename",
            &quoted(&self.dbfilename),
            self.dbfilename == defaults.dbfilename,
        );
        file.about(
            "Save a snapshot in the background after this many seconds if at least\n\
             this many keys changed: pairs of seconds and changes. save \"\" never\n\
             saves on its own.",
        );
        let save = if self.save_points.is_empty() {
            "\"\"".to_owned()
        } else {
            rexyra_commands::persistence::format_save_points(&self.save_points)
        };
        file.setting("save", &save, self.save_points == defaults.save_points);

        file.section("Append-only file");
        file.about(
            "Besides the snapshots, log every change to an append-only file, so that\n\
             a crash or a power cut loses at most about a second of changes.",
        );
        file.setting(
            "appendonly",
            &yes_no(self.aof.enabled),
            self.aof.enabled == defaults.aof.enabled,
        );
        file.about(
            "When the log is forced to disk: always (before each reply), everysec, or\n\
             no (when Windows gets round to it).",
        );
        file.setting(
            "appendfsync",
            self.aof.fsync.name(),
            self.aof.fsync == defaults.aof.fsync,
        );
        file.about("The folder, inside 'dir', and the file name the log is kept under.");
        file.setting(
            "appenddirname",
            &quoted(&self.aof.dirname),
            self.aof.dirname == defaults.aof.dirname,
        );
        file.setting(
            "appendfilename",
            &quoted(&self.aof.filename),
            self.aof.filename == defaults.aof.filename,
        );
        file.about(
            "Rewrite the log, to make it short again, once it has grown by this\n\
             percentage since the last rewrite and is at least this large. A\n\
             percentage of 0 never rewrites on its own.",
        );
        file.setting(
            "auto-aof-rewrite-percentage",
            &self.aof.rewrite_percentage.to_string(),
            self.aof.rewrite_percentage == defaults.aof.rewrite_percentage,
        );
        file.setting(
            "auto-aof-rewrite-min-size",
            &size(self.aof.rewrite_min_size),
            self.aof.rewrite_min_size == defaults.aof.rewrite_min_size,
        );
        file.about(
            "A log that ends in the middle of a command, as after a crash, is cut back\n\
             to the last complete command at start-up. With no, the server refuses to\n\
             start instead.",
        );
        file.setting(
            "aof-load-truncated",
            &yes_no(self.aof.load_truncated),
            self.aof.load_truncated == defaults.aof.load_truncated,
        );

        file.section("Memory");
        file.about(
            "A limit on the memory used, for example 2gb, and what to remove when it\n\
             is reached: noeviction (refuse writes), allkeys-lru, allkeys-lfu,\n\
             allkeys-random, volatile-lru, volatile-lfu, volatile-random or\n\
             volatile-ttl. 0 is no limit: the server takes what it is given.",
        );
        file.setting(
            "maxmemory",
            &size(self.maxmemory),
            self.maxmemory == defaults.maxmemory,
        );
        file.setting(
            "maxmemory-policy",
            self.maxmemory_policy.name(),
            self.maxmemory_policy == defaults.maxmemory_policy,
        );
        file.about(
            "How many keys are looked at to pick one to remove (1 to 64): more is\n\
             more exact and slower.",
        );
        file.setting(
            "maxmemory-samples",
            &self.maxmemory_samples.to_string(),
            self.maxmemory_samples == defaults.maxmemory_samples,
        );
        file.about(
            "For the lfu policies: how slowly a key's use counter grows, and after\n\
             how many minutes without use it is lowered.",
        );
        file.setting(
            "lfu-log-factor",
            &self.lfu_log_factor.to_string(),
            self.lfu_log_factor == defaults.lfu_log_factor,
        );
        file.setting(
            "lfu-decay-time",
            &self.lfu_decay_time.to_string(),
            self.lfu_decay_time == defaults.lfu_decay_time,
        );

        file.section("Scripts and the slow log");
        file.about(
            "Milliseconds a script may run before other clients are answered BUSY and\n\
             SCRIPT KILL can stop it; 0 is never. (Also read as lua-time-limit.)",
        );
        file.setting(
            "busy-reply-threshold",
            &self.busy_reply_threshold.to_string(),
            self.busy_reply_threshold == defaults.busy_reply_threshold,
        );
        file.about(
            "Commands that take longer than this many microseconds are recorded in the\n\
             slow log (-1 records none, 0 all of them), which keeps this many.",
        );
        file.setting(
            "slowlog-log-slower-than",
            &self.slowlog_log_slower_than.to_string(),
            self.slowlog_log_slower_than == defaults.slowlog_log_slower_than,
        );
        file.setting(
            "slowlog-max-len",
            &self.slowlog_max_len.to_string(),
            self.slowlog_max_len == defaults.slowlog_max_len,
        );

        file.section("Notifications and client-side caching");
        file.about(
            "Which changes to keys are published to Pub/Sub channels, in Redis's\n\
             letters: KEA for all of them, \"\" for none.",
        );
        let events = rexyra_commands::notify::format(self.notify_keyspace_events);
        let events = if events.is_empty() {
            "\"\"".to_owned()
        } else {
            events
        };
        file.setting(
            "notify-keyspace-events",
            &events,
            self.notify_keyspace_events == defaults.notify_keyspace_events,
        );
        file.about(
            "The most keys remembered for clients that cache (CLIENT TRACKING) before\n\
             some are invalidated early; 0 is no limit.",
        );
        file.setting(
            "tracking-table-max-keys",
            &self.tracking_table_max_keys.to_string(),
            self.tracking_table_max_keys == defaults.tracking_table_max_keys,
        );
        file.text
    }
}

/// Every setting a configuration file may hold, and so every one the
/// starter file lists.
pub const DIRECTIVES: &[&str] = &[
    "bind",
    "port",
    "protected-mode",
    "maxclients",
    "requirepass",
    "user",
    "enable-debug-command",
    "databases",
    "logfile",
    "logfile-max-size",
    "logfile-max-files",
    "dir",
    "dbfilename",
    "save",
    "appendonly",
    "appendfsync",
    "appenddirname",
    "appendfilename",
    "auto-aof-rewrite-percentage",
    "auto-aof-rewrite-min-size",
    "aof-load-truncated",
    "maxmemory",
    "maxmemory-policy",
    "maxmemory-samples",
    "lfu-log-factor",
    "lfu-decay-time",
    "busy-reply-threshold",
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "notify-keyspace-events",
    "tracking-table-max-keys",
];

/// The starter file as it is put together.
struct Starter {
    text: String,
}

impl Starter {
    /// A heading for a group of settings.
    fn section(&mut self, title: &str) {
        self.text.push_str(&format!(
            "\n\n# ---- {title} {}\n",
            "-".repeat(66usize.saturating_sub(title.len()))
        ));
    }

    /// A comment describing the settings that follow it.
    fn about(&mut self, comment: &str) {
        self.text.push('\n');
        for line in comment.lines() {
            self.text.push_str(&format!("# {line}\n"));
        }
    }

    /// A setting that is always written out.
    fn active(&mut self, name: &str, value: &str) {
        self.text.push_str(&format!("{name} {value}\n"));
    }

    /// A setting: written out if it was changed, and as a comment showing
    /// its value, the default, if not.
    fn setting(&mut self, name: &str, value: &str, is_default: bool) {
        if is_default {
            self.example(name, value);
        } else {
            self.active(name, value);
        }
    }

    /// A setting that is not set, with a value to show how it is written.
    fn example(&mut self, name: &str, value: &str) {
        self.text.push_str(&format!("# {name} {value}\n"));
    }
}

/// A number of bytes with the largest unit that divides it, as the
/// configuration file reads it back: `104857600` is `100mb`.
fn size(bytes: u64) -> String {
    const UNITS: [(u64, &str); 3] = [(1 << 30, "gb"), (1 << 20, "mb"), (1 << 10, "kb")];
    for (unit, name) in UNITS {
        if bytes != 0 && bytes.is_multiple_of(unit) {
            return format!("{}{name}", bytes / unit);
        }
    }
    bytes.to_string()
}

/// One value as a configuration file takes it, written the same way
/// `CONFIG REWRITE` writes it.
fn quoted(value: &str) -> String {
    rexyra_commands::rewrite::quote(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_and_create_config_are_not_settings() {
        // `--service` is for `main`; it may come before the file or after.
        assert_eq!(
            Config::from_args(["--service", "--port", "7000"]),
            Config::from_args(["--port", "7000", "--service"]),
        );
        let Ok(ArgsOutcome::Run(config)) = Config::from_args(["--service", "--port", "7000"])
        else {
            panic!("expected a configuration");
        };
        assert_eq!(config.port, 7000);

        let outcome =
            Config::from_args(["--create-config", "C:\\x\\rexyra.conf", "--port", "7000"]);
        let Ok(ArgsOutcome::CreateConfig(path, config)) = outcome else {
            panic!("expected a file to create: {outcome:?}");
        };
        assert_eq!(path, "C:\\x\\rexyra.conf");
        assert_eq!(config.port, 7000);
        assert_eq!(
            Config::from_args(["--create-config"])
                .unwrap_err()
                .to_string(),
            "option '--create-config' needs a value"
        );
    }

    #[test]
    fn a_starter_file_reads_back_as_the_same_settings() {
        let cases = [
            Config::default(),
            Config {
                bind: "0.0.0.0".to_owned(),
                port: 6400,
                dir: "C:\\ProgramData\\Rexyra\\data".to_owned(),
                logfile: "C:\\ProgramData\\Rexyra\\logs\\rexyra.log".to_owned(),
                ..Config::default()
            },
            // Spaces, a quote of each kind, and a value that looks like a comment.
            Config {
                dir: "D:\\My Data\\rexyra".to_owned(),
                logfile: "D:\\Bob's \"logs\"\\rexyra.log".to_owned(),
                logfile_max_size: 0,
                logfile_max_files: 20,
                requirepass: "#not a comment".to_owned(),
                maxmemory: 2 << 30,
                maxmemory_policy: Policy::AllKeysLru,
                aof: AofSettings {
                    enabled: true,
                    ..AofSettings::default()
                },
                ..Config::default()
            },
        ];
        for config in cases {
            let text = config.starter_file();
            let mut read = Config::default();
            read.apply_file(&text)
                .unwrap_or_else(|e| panic!("{e}\n{text}"));
            assert_eq!(read, config, "{text}");
        }
        // Paths without spaces stay as they are typed.
        let text = cases_text();
        assert!(
            text.contains("\ndir C:\\ProgramData\\Rexyra\\data\n"),
            "{text}"
        );
        assert!(text.contains("\n# appendonly no\n"), "{text}");
    }

    /// Every setting besides those the three cases above change.
    #[test]
    fn a_starter_file_keeps_every_setting_it_was_given() {
        let config = Config {
            databases: 4,
            maxclients: 500,
            slowlog_log_slower_than: -1,
            slowlog_max_len: 64,
            busy_reply_threshold: 0,
            dbfilename: "data.rdb".to_owned(),
            logfile_max_size: 1500,
            save_points: Vec::new(),
            save_given: true,
            aof: AofSettings {
                enabled: true,
                dirname: "log dir".to_owned(),
                filename: "changes.aof".to_owned(),
                fsync: Fsync::Always,
                rewrite_percentage: 0,
                rewrite_min_size: 3 << 20,
                load_truncated: false,
            },
            maxmemory: 1000,
            maxmemory_policy: Policy::VolatileTtl,
            maxmemory_samples: 10,
            lfu_log_factor: 3,
            lfu_decay_time: 0,
            protected_mode: false,
            enable_debug_command: rexyra_commands::DebugCommand::Local,
            notify_keyspace_events: rexyra_commands::notify::parse(b"Kx").unwrap(),
            tracking_table_max_keys: 0,
            users: vec![
                ["worker", "on", ">pass word", "~jobs:*", "+@list"]
                    .map(str::to_owned)
                    .to_vec(),
                ["reader", "on", "nopass", "~*", "+@read"]
                    .map(str::to_owned)
                    .to_vec(),
            ],
            ..Config::default()
        };
        let text = config.starter_file();
        let mut read = Config::default();
        read.apply_file(&text)
            .unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(read, config, "{text}");
        // Sizes keep a unit where one divides them, and "never save" is kept.
        for line in [
            "save \"\"",
            "logfile-max-size 1500",
            "auto-aof-rewrite-min-size 3mb",
            "maxmemory 1000",
        ] {
            assert!(
                text.lines().any(|found| found == line),
                "{line}
{text}"
            );
        }

        let other = Config {
            save_points: vec![(900, 1), (30, 5)],
            save_given: true,
            ..Config::default()
        };
        let mut read = Config::default();
        read.apply_file(&other.starter_file()).unwrap();
        assert_eq!(read, other);
    }

    /// The settings of a starter file, by name: whether each is written out
    /// or commented, and its value.
    fn listed(text: &str) -> Vec<(String, bool, String)> {
        text.lines()
            .filter_map(|line| {
                let (active, rest) = match line.strip_prefix("# ") {
                    Some(rest) => (false, rest),
                    None => (true, line),
                };
                let (name, value) = rest.split_once(' ')?;
                DIRECTIVES
                    .contains(&name)
                    .then(|| (name.to_owned(), active, value.to_owned()))
            })
            .collect()
    }

    #[test]
    fn a_starter_file_lists_every_setting() {
        let text = Config::default().starter_file();
        let listed = listed(&text);
        let names: Vec<&str> = listed.iter().map(|(name, ..)| name.as_str()).collect();
        // Each one once, in the order of the list, and nothing else that
        // would read as a setting once uncommented.
        assert_eq!(names, DIRECTIVES, "{text}");
        // By default only where to listen and where the data goes are set.
        let active: Vec<&str> = listed
            .iter()
            .filter(|(_, active, _)| *active)
            .map(|(name, ..)| name.as_str())
            .collect();
        assert_eq!(active, ["bind", "port", "dir"], "{text}");
        // No line is long, and every line is a comment, a setting or empty.
        for line in text.lines() {
            assert!(line.len() <= 80, "{line}");
        }

        // The server knows every setting on the list...
        for name in DIRECTIVES {
            let known = Config::default().apply(name, &["1".to_owned()]);
            assert_ne!(known, Ok(false), "{name}");
        }
        // ...and each commented line gives the default: with every one of
        // them uncommented, the file reads back as the defaults, apart from
        // the three that have no default to show and show an example.
        let uncommented: String = text
            .lines()
            .map(|line| match line.strip_prefix("# ") {
                Some(rest)
                    if rest
                        .split_once(' ')
                        .is_some_and(|(name, _)| DIRECTIVES.contains(&name)) =>
                {
                    format!("{rest}\n")
                }
                _ => format!("{line}\n"),
            })
            .collect();
        let mut read = Config::default();
        read.apply_file(&uncommented)
            .unwrap_or_else(|e| panic!("{e}\n{uncommented}"));
        assert_eq!(
            read,
            Config {
                requirepass: "a-long-random-password".to_owned(),
                logfile: "rexyra.log".to_owned(),
                users: vec![[
                    "worker",
                    "on",
                    ">another-long-password",
                    "~jobs:*",
                    "+@list"
                ]
                .map(str::to_owned)
                .to_vec()],
                save_given: true,
                ..Config::default()
            },
            "{uncommented}"
        );
    }

    fn cases_text() -> String {
        Config {
            dir: "C:\\ProgramData\\Rexyra\\data".to_owned(),
            ..Config::default()
        }
        .starter_file()
    }

    #[test]
    fn defaults() {
        assert_eq!(
            Config::from_args(Vec::<String>::new()),
            Ok(ArgsOutcome::Run(Config::default()))
        );
    }

    #[test]
    fn overrides() {
        let outcome = Config::from_args([
            "--port",
            "7000",
            "--bind",
            "0.0.0.0",
            "--databases",
            "4",
            "--slowlog-log-slower-than",
            "-1",
        ]);
        assert_eq!(
            outcome,
            Ok(ArgsOutcome::Run(Config {
                bind: "0.0.0.0".to_owned(),
                port: 7000,
                databases: 4,
                slowlog_log_slower_than: -1,
                ..Config::default()
            }))
        );
    }

    #[test]
    fn help_and_version() {
        assert_eq!(Config::from_args(["--help"]), Ok(ArgsOutcome::Help));
        assert_eq!(Config::from_args(["-v"]), Ok(ArgsOutcome::Version));
    }

    #[test]
    fn errors() {
        assert!(Config::from_args(["--nope", "1"]).is_err());
        assert!(Config::from_args(["--port"]).is_err());
        assert!(Config::from_args(["--port", "99999"]).is_err());
        assert!(Config::from_args(["--databases", "0"]).is_err());
        assert!(Config::from_args(["--slowlog-max-len", "-1"]).is_err());
        assert!(Config::from_args(["no-such-file.conf"]).is_err());
        assert!(Config::from_args(["--logfile-max-size", "big"]).is_err());
        assert!(Config::from_args(["--logfile-max-files", "-1"]).is_err());
        assert!(Config::from_args(["--logfile-max-files", "1001"]).is_err());
    }

    #[test]
    fn notify_keyspace_events_is_checked_and_case_sensitive() {
        let mut config = Config::default();
        config.apply_file("notify-keyspace-events KEA\n").unwrap();
        assert_eq!(
            rexyra_commands::notify::format(config.notify_keyspace_events),
            "AKE"
        );
        config.apply_file("notify-keyspace-events \"\"\n").unwrap();
        assert_eq!(config.notify_keyspace_events, 0);
        let error = config
            .apply_file("notify-keyspace-events kea\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("Invalid event class character"), "{error}");
    }

    #[test]
    fn file_directives() {
        let mut config = Config::default();
        config
            .apply_file(
                "# A comment\n\
                 \n\
                 PORT 7001\n\
                 bind -10.0.0.1 -::1\n\
                 databases \"8\"\n\
                 maxclients '200'\n\
                 slowlog-log-slower-than 0\n\
                 slowlog-max-len 64\n\
                 lua-time-limit 250\n\
                 dir \"data dir\"\n\
                 dbfilename snap.rdb\n\
                 logfile C:\\logs\\rexyra.log\n\
                 logfile-max-size 10mb\n\
                 logfile-max-files 3\n\
                 save 900 1\n\
                 save 30 5\n\
                 enable-debug-command local\n\
                 appendonly yes\n\
                 appendfsync always\n\
                 auto-aof-rewrite-min-size 1mb\n\
                 maxmemory 2gb\n\
                 maxmemory-policy allkeys-lfu\n\
                 maxmemory-samples 10\n\
                 lfu-decay-time 3\n\
                 tcp-keepalive 300\n\
                 requirepass \"two words\"\n\
                 protected-mode no\n\
                 notify-keyspace-events \"Elx\"\n\
                 tracking-table-max-keys 5000\n\
                 user worker on >pw ~jobs:* +@list\n",
            )
            .unwrap();
        assert_eq!(
            config,
            Config {
                bind: "10.0.0.1".to_owned(),
                port: 7001,
                databases: 8,
                maxclients: 200,
                slowlog_log_slower_than: 0,
                slowlog_max_len: 64,
                busy_reply_threshold: 250,
                dir: "data dir".to_owned(),
                dbfilename: "snap.rdb".to_owned(),
                logfile: r"C:\logs\rexyra.log".to_owned(),
                logfile_max_size: 10 * 1024 * 1024,
                logfile_max_files: 3,
                aof: AofSettings {
                    enabled: true,
                    fsync: Fsync::Always,
                    rewrite_min_size: 1024 * 1024,
                    ..AofSettings::default()
                },
                save_points: vec![(900, 1), (30, 5)],
                save_given: true,
                maxmemory: 2 * 1024 * 1024 * 1024,
                maxmemory_policy: Policy::AllKeysLfu,
                maxmemory_samples: 10,
                lfu_log_factor: 10,
                lfu_decay_time: 3,
                requirepass: "two words".to_owned(),
                protected_mode: false,
                enable_debug_command: rexyra_commands::DebugCommand::Local,
                notify_keyspace_events: rexyra_commands::notify::KEYEVENT
                    | rexyra_commands::notify::LIST
                    | rexyra_commands::notify::EXPIRED,
                tracking_table_max_keys: 5000,
                users: vec![["worker", "on", ">pw", "~jobs:*", "+@list"]
                    .map(str::to_owned)
                    .to_vec()],
                config_file: None,
                ignored: vec!["tcp-keepalive".to_owned()],
            }
        );
    }

    #[test]
    fn file_errors_name_the_line() {
        for (text, expected) in [
            (
                "port 1\nport x",
                "config file line 2: invalid value 'x' for 'port'",
            ),
            (
                "port 1 2",
                "config file line 1: 'port' takes exactly one value",
            ),
            (
                "databases 0",
                "config file line 1: invalid value '0' for 'databases'",
            ),
            ("bind \"127.0.0.1", "config file line 1: unbalanced quotes"),
            (
                "bind \"127.0.0.1\"x",
                "config file line 1: closing quote must be followed by a space",
            ),
        ] {
            let error = Config::default().apply_file(text).unwrap_err();
            assert_eq!(error.to_string(), expected, "{text}");
        }
    }

    #[test]
    fn quoting() {
        assert_eq!(
            split_line(r#"  a "b c" 'd "e"' "f\"g\n" h"#).unwrap(),
            ["a", "b c", "d \"e\"", "f\"g\n", "h"]
        );
        assert_eq!(split_line("   ").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn flags_override_the_file() {
        let path =
            std::env::temp_dir().join(format!("rexyra-server-test-{}.conf", std::process::id()));
        std::fs::write(&path, "port 7005\ndatabases 3\ntcp-keepalive 300\n").unwrap();
        let path_text = path.to_str().unwrap().to_owned();
        let outcome = Config::from_args([path_text.as_str(), "--port", "7006"]);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            outcome,
            Ok(ArgsOutcome::Run(Config {
                port: 7006,
                databases: 3,
                config_file: Some(path_text),
                ignored: vec!["tcp-keepalive".to_owned()],
                ..Config::default()
            }))
        );
    }
}
