//! rexyra-keyevents: records the keyspace notifications a script of commands
//! produces, so that rexyra-server can be compared with Redis.
//!
//! One connection subscribes to every notification (`PSUBSCRIBE
//! __key*@*__:*`) and to a marker channel. The script's commands run on
//! other connections; after each step a control connection publishes a
//! marker, and everything the subscriber received before that marker is
//! the step's notifications. The transcript (each step, its reply and its
//! notifications, in the order they arrived) goes to standard output, so
//! two runs can be compared with `diff`:
//!
//! ```text
//! rexyra-keyevents --port 6387 --script tools/rexyra-keyevents/scripts/all.txt > redis.txt
//! rexyra-keyevents --port 6399 --script tools/rexyra-keyevents/scripts/all.txt > rexyra.txt
//! diff redis.txt rexyra.txt
//! ```
//!
//! # Script
//!
//! One step per line; `#` starts a comment line.
//!
//! * `SET k v` runs a command on the connection `main` and waits for the
//!   reply. Words are split on spaces; `"..."` (with `\n`, `\"`, `\\` and
//!   `\xHH` escapes) and `'...'` quote.
//! * `[name] BLPOP q 0 &` runs it on the connection `name` (opened on first
//!   use); a trailing `&` sends it without waiting for the reply.
//! * `~SPOP s` hides the reply (for commands whose reply is random).
//! * `!wait name` reads the reply `name` is owed.
//! * `!sleep 250` waits that many milliseconds.
//! * `!flags KEA` sets `notify-keyspace-events`.
//! * `!maxmemory +100000` sets `maxmemory` to the memory in use plus that
//!   many bytes; `!maxmemory 0` removes the limit.
//!
//! The run starts with `FLUSHALL` and `notify-keyspace-events` set to the
//! value of `--flags` (default `KEA`).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "\
usage: rexyra-keyevents --port PORT --script FILE [--host 127.0.0.1] [--flags KEA]";

const MARKER: &str = "rexyra-keyevents:marker";

#[derive(Debug)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
    Map(Vec<(Reply, Reply)>),
    Set(Vec<Reply>),
    Push(Vec<Reply>),
    Null,
    Double(String),
    Bool(bool),
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn open(host: &str, port: u16) -> io::Result<Conn> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        Ok(Conn {
            reader: BufReader::new(stream.try_clone()?),
            writer: stream,
        })
    }

    fn send(&mut self, args: &[Vec<u8>]) -> io::Result<()> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf)
    }

    fn call(&mut self, args: &[&str]) -> io::Result<Reply> {
        let args: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
        self.send(&args)?;
        self.read()
    }

    fn line(&mut self) -> io::Result<String> {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line)?;
        if !line.ends_with(b"\r\n") {
            return Err(io::Error::other("connection closed"));
        }
        line.truncate(line.len() - 2);
        Ok(String::from_utf8_lossy(&line).into_owned())
    }

    fn read(&mut self) -> io::Result<Reply> {
        let line = self.line()?;
        let (kind, rest) = line.split_at(1);
        let number = || {
            rest.parse::<i64>()
                .map_err(|_| io::Error::other(format!("bad length {rest:?}")))
        };
        Ok(match kind {
            "+" => Reply::Simple(rest.to_owned()),
            "-" => Reply::Error(rest.to_owned()),
            ":" => Reply::Int(number()?),
            "," => Reply::Double(rest.to_owned()),
            "#" => Reply::Bool(rest == "t"),
            "_" => Reply::Null,
            "(" => Reply::Simple(rest.to_owned()),
            "$" | "=" | "!" => {
                let len = number()?;
                if len < 0 {
                    Reply::Bulk(None)
                } else {
                    let mut data = vec![0; len as usize + 2];
                    self.reader.read_exact(&mut data)?;
                    data.truncate(len as usize);
                    if kind == "!" {
                        Reply::Error(String::from_utf8_lossy(&data).into_owned())
                    } else {
                        Reply::Bulk(Some(data))
                    }
                }
            }
            "*" | "~" | ">" => {
                let len = number()?;
                if len < 0 {
                    Reply::Array(None)
                } else {
                    let items = (0..len).map(|_| self.read()).collect::<io::Result<_>>()?;
                    match kind {
                        "*" => Reply::Array(Some(items)),
                        "~" => Reply::Set(items),
                        _ => Reply::Push(items),
                    }
                }
            }
            "%" | "|" => {
                let len = number()?;
                let mut pairs = Vec::new();
                for _ in 0..len {
                    pairs.push((self.read()?, self.read()?));
                }
                if kind == "|" {
                    // An attribute comes before the reply it describes.
                    return self.read();
                }
                Reply::Map(pairs)
            }
            _ => return Err(io::Error::other(format!("unexpected reply {line:?}"))),
        })
    }
}

fn escape(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            0x20..=0x7e => out.push(b as char),
            _ => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
    }
    out
}

fn show(reply: &Reply) -> String {
    let list = |items: &[Reply]| items.iter().map(show).collect::<Vec<_>>().join(", ");
    match reply {
        Reply::Simple(text) => text.clone(),
        Reply::Error(text) => format!("(error) {text}"),
        Reply::Int(n) => n.to_string(),
        Reply::Bulk(Some(data)) => format!("\"{}\"", escape(data)),
        Reply::Bulk(None) | Reply::Array(None) | Reply::Null => "(nil)".to_owned(),
        Reply::Array(Some(items)) => format!("[{}]", list(items)),
        Reply::Set(items) => format!("~[{}]", list(items)),
        Reply::Push(items) => format!(">[{}]", list(items)),
        Reply::Map(pairs) => format!(
            "{{{}}}",
            pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", show(k), show(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Reply::Double(text) => format!("(double) {text}"),
        Reply::Bool(flag) => format!("(bool) {flag}"),
    }
}

/// Splits a script line into arguments.
fn split_words(line: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&first) = chars.peek() else {
            return Ok(words);
        };
        let mut word = Vec::new();
        if first == '"' || first == '\'' {
            chars.next();
            loop {
                match chars.next() {
                    None => return Err(format!("unbalanced quotes in {line:?}")),
                    Some(c) if c == first => break,
                    Some('\\') if first == '"' => match chars.next() {
                        Some('n') => word.push(b'\n'),
                        Some('r') => word.push(b'\r'),
                        Some('t') => word.push(b'\t'),
                        Some('x') => {
                            let hex: String = chars.by_ref().take(2).collect();
                            let byte = u8::from_str_radix(&hex, 16)
                                .map_err(|_| format!("bad \\x escape in {line:?}"))?;
                            word.push(byte);
                        }
                        Some(c) => {
                            let mut buf = [0; 4];
                            word.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                        }
                        None => return Err(format!("unbalanced quotes in {line:?}")),
                    },
                    Some(c) => {
                        let mut buf = [0; 4];
                        word.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                let mut buf = [0; 4];
                word.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                chars.next();
            }
        }
        words.push(word);
    }
}

struct Options {
    host: String,
    port: u16,
    script: String,
    flags: String,
}

fn parse() -> Result<Options, String> {
    let mut options = Options {
        host: "127.0.0.1".to_owned(),
        port: 0,
        script: String::new(),
        flags: "KEA".to_owned(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it.next().ok_or(format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--host" => options.host = value.clone(),
            "--port" => options.port = value.parse().map_err(|_| "bad --port".to_owned())?,
            "--script" => options.script = value.clone(),
            "--flags" => options.flags = value.clone(),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    if options.port == 0 || options.script.is_empty() {
        return Err(USAGE.to_owned());
    }
    Ok(options)
}

/// The notifications received before the marker for `step`, one per line.
fn events_until(sub: &mut Conn, step: usize) -> io::Result<Vec<String>> {
    let mut events = Vec::new();
    loop {
        let reply = sub.read()?;
        let items = match reply {
            Reply::Array(Some(items)) | Reply::Push(items) => items,
            other => return Err(io::Error::other(format!("unexpected {}", show(&other)))),
        };
        let text: Vec<Vec<u8>> = items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(Some(data)) => data,
                other => show(&other).into_bytes(),
            })
            .collect();
        match text.first().map(Vec::as_slice) {
            Some(b"message") if text.len() == 3 && text[1] == MARKER.as_bytes() => {
                if text[2] == step.to_string().as_bytes() {
                    return Ok(events);
                }
            }
            Some(b"pmessage") if text.len() == 4 => {
                events.push(format!("  {} {}", escape(&text[2]), escape(&text[3])));
            }
            _ => events.push(format!(
                "  ?? {}",
                text.iter().map(|t| escape(t)).collect::<Vec<_>>().join(" ")
            )),
        }
    }
}

fn used_memory(control: &mut Conn) -> io::Result<u64> {
    let info = match control.call(&["INFO", "memory"])? {
        Reply::Bulk(Some(data)) => String::from_utf8_lossy(&data).into_owned(),
        other => return Err(io::Error::other(format!("INFO: {}", show(&other)))),
    };
    info.lines()
        .find_map(|line| line.strip_prefix("used_memory:"))
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| io::Error::other("no used_memory in INFO"))
}

fn run(options: &Options) -> io::Result<()> {
    let script = std::fs::read_to_string(&options.script)?;
    let open = || Conn::open(&options.host, options.port);
    let mut control = open()?;
    control.call(&["FLUSHALL"])?;
    let set = control.call(&["CONFIG", "SET", "notify-keyspace-events", &options.flags])?;
    println!("flags {} -> {}", options.flags, show(&set));
    let mut sub = open()?;
    sub.call(&["PSUBSCRIBE", "__key*@*__:*"])?;
    sub.call(&["SUBSCRIBE", MARKER])?;

    let mut conns: HashMap<String, Conn> = HashMap::new();
    let mut step = 0;
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for raw in script.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        step += 1;
        let heading;
        if let Some(directive) = line.strip_prefix('!') {
            let words: Vec<&str> = directive.split_whitespace().collect();
            match words.as_slice() {
                ["sleep", ms] => {
                    let ms: u64 = ms.parse().map_err(|_| io::Error::other("bad sleep"))?;
                    std::thread::sleep(Duration::from_millis(ms));
                    heading = line.to_owned();
                }
                ["flags", value] => {
                    let value = value.trim_matches('"');
                    let reply =
                        control.call(&["CONFIG", "SET", "notify-keyspace-events", value])?;
                    heading = format!("{line} -> {}", show(&reply));
                }
                ["flags"] => {
                    let reply = control.call(&["CONFIG", "SET", "notify-keyspace-events", ""])?;
                    heading = format!("{line} -> {}", show(&reply));
                }
                ["wait", name] => {
                    let conn = conns
                        .get_mut(*name)
                        .ok_or_else(|| io::Error::other(format!("no connection {name}")))?;
                    heading = format!("{line} -> {}", show(&conn.read()?));
                }
                ["maxmemory", amount] => {
                    let limit = match amount.strip_prefix('+') {
                        Some(extra) => {
                            used_memory(&mut control)?
                                + extra
                                    .parse::<u64>()
                                    .map_err(|_| io::Error::other("bad +N"))?
                        }
                        None => amount
                            .parse()
                            .map_err(|_| io::Error::other("bad maxmemory"))?,
                    };
                    let reply =
                        control.call(&["CONFIG", "SET", "maxmemory", &limit.to_string()])?;
                    heading = format!("{line} -> {}", show(&reply));
                }
                _ => return Err(io::Error::other(format!("unknown directive {line:?}"))),
            }
        } else {
            let (name, rest) = match line.strip_prefix('[') {
                Some(rest) => {
                    let (name, rest) = rest
                        .split_once(']')
                        .ok_or_else(|| io::Error::other(format!("bad line {line:?}")))?;
                    (name.to_owned(), rest.trim())
                }
                None => ("main".to_owned(), line),
            };
            let (rest, background) = match rest.strip_suffix('&') {
                Some(rest) => (rest.trim_end(), true),
                None => (rest, false),
            };
            let (rest, hidden) = match rest.strip_prefix('~') {
                Some(rest) => (rest, true),
                None => (rest, false),
            };
            let words = split_words(rest).map_err(io::Error::other)?;
            if !conns.contains_key(&name) {
                conns.insert(name.clone(), open()?);
            }
            let conn = conns.get_mut(&name).expect("inserted above");
            conn.send(&words)?;
            let prefix = if name == "main" {
                String::new()
            } else {
                format!("[{name}] ")
            };
            if background {
                heading = format!("{prefix}{rest} &");
            } else {
                // Pushes the connection receives (in RESP3) before its reply
                // are shown in order, ahead of it.
                let mut shown = Vec::new();
                loop {
                    let reply = conn.read()?;
                    let push = matches!(reply, Reply::Push(_));
                    shown.push(if hidden && !push {
                        "(hidden)".to_owned()
                    } else {
                        show(&reply)
                    });
                    if !push {
                        break;
                    }
                }
                heading = format!("{prefix}{rest} -> {}", shown.join(" ; "));
            }
        }
        control.call(&["PUBLISH", MARKER, &step.to_string()])?;
        let events = events_until(&mut sub, step)?;
        writeln!(out, "{heading}")?;
        for event in events {
            writeln!(out, "{event}")?;
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let options = match parse() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    match run(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("rexyra-keyevents: {error}");
            ExitCode::FAILURE
        }
    }
}
