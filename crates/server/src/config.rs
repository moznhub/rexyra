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
  --maxmemory <BYTES>               Memory limit, e.g. 512mb;
                                    0 = none                      [default: 0]
  --maxmemory-policy <POLICY>       What to evict at the limit: noeviction,
                                    allkeys-lru, allkeys-lfu, allkeys-random,
                                    volatile-lru, volatile-lfu, volatile-random,
                                    volatile-ttl                  [default: noeviction]
  --dir <PATH>                      Directory of the snapshot     [default: .]
  --dbfilename <NAME>               File name of the snapshot     [default: dump.rdb]
  --logfile <PATH>                  File the log is written to    [default: standard error]
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
    /// this one: the settings an installation chooses, written out, and the
    /// ones worth knowing about as comments.
    pub fn starter_file(&self) -> String {
        let mut text = String::from(
            "# Rexyra configuration.\n\
             #\n\
             # Restart the server (or the Rexyra service) after changing this file.\n\
             # Every setting is described at https://github.com/moznhub/rexyra#configuration\n\
             #\n\
             # A path with spaces goes in single quotes: dir 'C:\\My Data\\rexyra'\n\
             # Inside double quotes a backslash starts an escape, as in redis.conf.\n\
             \n\
             # The address and port to listen on. 127.0.0.1 accepts connections from\n\
             # this machine only; 0.0.0.0 accepts them on every network interface.\n",
        );
        text.push_str(&format!(
            "bind {}\nport {}\n",
            quoted(&self.bind),
            self.port
        ));
        text.push_str(
            "\n\
             # A password clients must give with AUTH. Without one, connections from\n\
             # other machines are refused whatever 'bind' says.\n",
        );
        if self.requirepass.is_empty() {
            text.push_str("# requirepass a-long-random-password\n");
        } else {
            text.push_str(&format!("requirepass {}\n", quoted(&self.requirepass)));
        }
        text.push_str("\n# The folder the data files are kept in.\n");
        text.push_str(&format!("dir {}\n", quoted(&self.dir)));
        text.push_str(
            "\n# The file the log is written to. Without one it goes to standard error.\n",
        );
        if self.logfile.is_empty() {
            text.push_str("# logfile rexyra.log\n");
        } else {
            text.push_str(&format!("logfile {}\n", quoted(&self.logfile)));
        }
        text.push_str(
            "\n\
             # Besides the snapshots, log every change to an append-only file, so that\n\
             # a crash or a power cut loses at most about a second of changes.\n",
        );
        text.push_str(if self.aof.enabled {
            "appendonly yes\n"
        } else {
            "# appendonly yes\n"
        });
        text.push_str(
            "\n\
             # A limit on the memory used for data, and what to remove when it is\n\
             # reached. Without a limit the server takes what it is given.\n",
        );
        if self.maxmemory == 0 {
            text.push_str("# maxmemory 2gb\n# maxmemory-policy allkeys-lru\n");
        } else {
            text.push_str(&format!(
                "maxmemory {}\nmaxmemory-policy {}\n",
                self.maxmemory,
                self.maxmemory_policy.name()
            ));
        }
        text
    }
}

/// One value as a configuration file takes it: bare where that is safe,
/// otherwise in single quotes, which are literal, or in double quotes with
/// escapes when the value itself holds a single quote.
fn quoted(value: &str) -> String {
    let bare = !value.is_empty()
        && !value.starts_with('#')
        && !value.contains(|c: char| c.is_whitespace() || c == '"' || c == '\'');
    if bare {
        value.to_owned()
    } else if !value.contains('\'') {
        format!("'{value}'")
    } else {
        let mut out = String::from("\"");
        for c in value.chars() {
            match c {
                '\\' | '"' => out.extend(['\\', c]),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c => out.push(c),
            }
        }
        out.push('"');
        out
    }
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
        assert!(text.contains("\n# appendonly yes\n"), "{text}");
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
                 save 900 1\n\
                 save 30 5\n\
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
