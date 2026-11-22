//! rexyra-tracking: records everything several connections receive while a
//! script of commands runs, so that client-side caching (`CLIENT TRACKING`)
//! in rexyra-server can be compared with Redis.
//!
//! Each connection's replies and pushed messages (invalidations, Pub/Sub
//! messages, keyspace notifications) are printed in the order they
//! arrive, together with where they arrived: before a command was sent,
//! right after its reply, or when the script asked to look. The
//! transcript goes to standard output, so two runs can be compared with
//! `diff`:
//!
//! ```text
//! rexyra-tracking --port 6392 --script tools/rexyra-tracking/scripts/basic.txt > redis.txt
//! rexyra-tracking --port 6399 --script tools/rexyra-tracking/scripts/basic.txt > rexyra.txt
//! diff redis.txt rexyra.txt
//! ```
//!
//! Run each script against a freshly started server: scripts assume an
//! empty keyspace and default settings.
//!
//! # Script
//!
//! One step per line. Words are split as a POSIX shell would (quotes and
//! backslashes).
//!
//! * `name: COMMAND args` sends the command on the connection `name`
//!   (opened on first use) and reads its reply. What the connection had
//!   received before is shown first; what arrives right after the reply
//!   is shown after it, marked `(after reply)`.
//! * `name! COMMAND args` sends it without reading the reply, which then
//!   shows up later as something received.
//! * `name| A ;; B ;; C` sends the commands as one pipeline.
//! * `~sleep ms` waits.
//! * `~drain` shows what every connection has received, connection by
//!   connection in the order they were opened.
//! * A line starting with `##` is printed as a heading; other lines
//!   starting with `#`, and blank lines, are ignored.
//!
//! A word `$name` stands for the id of the connection `name` (opened if
//! need be), so that scripts work whatever ids a server hands out. Ids are
//! printed as `#name` wherever they appear: a `HELLO` or `CLIENT
//! TRACKINGINFO` field `id` or `redirect`, the reply to `CLIENT ID` and
//! `CLIENT GETREDIR`, a `tracking-redir-broken` message, and the `id=` and
//! `redir=` fields of `CLIENT LIST` and `CLIENT INFO`, whose lines are put
//! in id order.
//!
//! A command may end with `@@ field ...` to show only those fields of its
//! reply: the keys of a map (or of a flat RESP2 list of pairs), the
//! `field:value` lines of `INFO`, or the `field=value` words of `CLIENT
//! LIST` and `CLIENT INFO`. That leaves out what differs between servers
//! for reasons that have nothing to do with tracking (versions, counters,
//! addresses).
//!
//! Waiting for pushed messages works as in the Python probe this tool
//! replaces: something "has arrived" if it is there within 150 ms.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: rexyra-tracking --port PORT --script FILE [--host 127.0.0.1]";

/// How long to wait for more of something already arriving.
const PATIENCE: Duration = Duration::from_secs(5);
/// How long to wait for something that may not come.
const QUIET: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Simple(String),
    Error(String),
    Int(i64),
    Double(String),
    Bool(bool),
    Null,
    Bulk(Vec<u8>),
    Array(Vec<Value>),
    Set(Vec<Value>),
    Push(Vec<Value>),
    Map(Vec<(Value, Value)>),
    /// A client id, shown by the name of its connection.
    Id(String),
    /// The server closed the connection.
    Closed,
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    closed: bool,
}

impl Conn {
    fn open(host: &str, port: u16) -> io::Result<Conn> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        Ok(Conn {
            stream,
            buf: Vec::new(),
            closed: false,
        })
    }

    fn send(&mut self, commands: &[Vec<Vec<u8>>]) -> io::Result<()> {
        let mut out = Vec::new();
        for args in commands {
            out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for arg in args {
                out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
                out.extend_from_slice(arg);
                out.extend_from_slice(b"\r\n");
            }
        }
        self.stream.write_all(&out)
    }

    /// Reads whatever arrives within `timeout`. `Ok(false)` if nothing did.
    fn fill(&mut self, timeout: Duration) -> io::Result<bool> {
        self.stream.set_read_timeout(Some(timeout))?;
        let mut chunk = [0u8; 65536];
        match self.stream.read(&mut chunk) {
            Ok(0) => Err(io::Error::new(ErrorKind::UnexpectedEof, "closed")),
            Ok(n) => {
                self.buf.extend_from_slice(&chunk[..n]);
                Ok(true)
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn line(&mut self) -> io::Result<Vec<u8>> {
        loop {
            if let Some(at) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.buf[..at].to_vec();
                self.buf.drain(..at + 2);
                return Ok(line);
            }
            if !self.fill(PATIENCE)? {
                return Err(io::Error::new(ErrorKind::TimedOut, "no reply"));
            }
        }
    }

    fn exact(&mut self, n: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < n + 2 {
            if !self.fill(PATIENCE)? {
                return Err(io::Error::new(ErrorKind::TimedOut, "no reply"));
            }
        }
        let data = self.buf[..n].to_vec();
        self.buf.drain(..n + 2);
        Ok(data)
    }

    fn frame(&mut self) -> io::Result<Value> {
        let line = self.line()?;
        let Some((&kind, rest)) = line.split_first() else {
            return Err(io::Error::other("empty line"));
        };
        let rest = String::from_utf8_lossy(rest).into_owned();
        let number = || {
            rest.parse::<i64>()
                .map_err(|_| io::Error::other(format!("bad number {rest:?}")))
        };
        Ok(match kind {
            b'+' => Value::Simple(rest),
            b'-' => Value::Error(rest),
            b':' => Value::Int(number()?),
            b',' => Value::Double(rest),
            b'#' => Value::Bool(rest == "t"),
            b'_' => Value::Null,
            b'(' => Value::Simple(rest),
            b'$' | b'=' | b'!' => match usize::try_from(number()?) {
                Err(_) => Value::Null,
                Ok(n) => {
                    let data = self.exact(n)?;
                    if kind == b'!' {
                        Value::Error(String::from_utf8_lossy(&data).into_owned())
                    } else {
                        Value::Bulk(data)
                    }
                }
            },
            b'*' | b'~' | b'>' => match usize::try_from(number()?) {
                Err(_) => Value::Null,
                Ok(n) => {
                    let items = (0..n)
                        .map(|_| self.frame())
                        .collect::<io::Result<Vec<_>>>()?;
                    match kind {
                        b'*' => Value::Array(items),
                        b'~' => Value::Set(items),
                        _ => Value::Push(items),
                    }
                }
            },
            b'%' | b'|' => {
                let n = number()?;
                let mut pairs = Vec::new();
                for _ in 0..n {
                    pairs.push((self.frame()?, self.frame()?));
                }
                if kind == b'|' {
                    // Attributes describe the reply that follows.
                    return self.frame();
                }
                Value::Map(pairs)
            }
            _ => return Err(io::Error::other(format!("unexpected {line:?}"))),
        })
    }

    /// What the connection has been sent, without waiting long for it.
    fn pending(&mut self) -> io::Result<Vec<Value>> {
        let mut frames = Vec::new();
        if self.closed {
            return Ok(frames);
        }
        loop {
            if self.buf.is_empty() {
                match self.fill(QUIET) {
                    Ok(true) => {}
                    Ok(false) => return Ok(frames),
                    Err(_) => {
                        self.closed = true;
                        frames.push(Value::Closed);
                        return Ok(frames);
                    }
                }
            }
            frames.push(self.frame()?);
        }
    }
}

/// Splits a command the way Python's `shlex.split` does.
fn split_words(text: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => word.push(c),
                        None => return Err("unterminated '".to_owned()),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(c @ ('"' | '\\' | '$' | '`')) => word.push(c),
                            Some(c) => {
                                word.push('\\');
                                word.push(c);
                            }
                            None => return Err("unterminated \"".to_owned()),
                        },
                        Some(c) => word.push(c),
                        None => return Err("unterminated \"".to_owned()),
                    }
                }
            }
            '\\' => {
                in_word = true;
                if let Some(c) = chars.next() {
                    word.push(c);
                }
            }
            c => {
                in_word = true;
                word.push(c);
            }
        }
    }
    if in_word {
        words.push(word);
    }
    Ok(words)
}

/// Python's `repr` of a string.
fn quote(text: &str) -> String {
    let delimiter = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
    out.push(delimiter);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == delimiter => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push(delimiter);
    out
}

/// The transcript form of a value, close to what the Python probe printed.
fn show(value: &Value) -> String {
    let list = |items: &[Value]| items.iter().map(show).collect::<Vec<_>>().join(", ");
    match value {
        Value::Simple(text) => quote(text),
        Value::Error(text) => quote(&format!("(error) {text}")),
        Value::Int(n) => n.to_string(),
        Value::Double(text) => quote(&format!("(double) {text}")),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Null => "None".to_owned(),
        Value::Bulk(data) => quote(&String::from_utf8_lossy(data)),
        Value::Array(items) => format!("[{}]", list(items)),
        Value::Set(items) => format!("('SET', [{}])", list(items)),
        Value::Push(items) => format!("('PUSH', [{}])", list(items)),
        Value::Map(pairs) => format!(
            "{{{}}}",
            pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", quote(&text_of(k)), show(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Id(name) => format!("#{name}"),
        Value::Closed => quote("(connection closed)"),
    }
}

fn text_of(value: &Value) -> String {
    match value {
        Value::Simple(text) => text.clone(),
        Value::Bulk(data) => String::from_utf8_lossy(data).into_owned(),
        other => show(other),
    }
}

fn is_text(value: &Value, wanted: &str) -> bool {
    matches!(value, Value::Simple(_) | Value::Bulk(_)) && text_of(value) == wanted
}

struct Normaliser<'a> {
    /// Connection name by client id.
    names: &'a HashMap<u64, String>,
}

impl Normaliser<'_> {
    fn id(&self, value: Value) -> Value {
        match value {
            Value::Int(n) => match u64::try_from(n).ok().and_then(|id| self.names.get(&id)) {
                Some(name) => Value::Id(name.clone()),
                None => Value::Int(n),
            },
            other => other,
        }
    }

    fn word(&self, word: &str) -> String {
        for field in ["id=", "redir="] {
            if let Some(number) = word.strip_prefix(field) {
                if let Some(name) = number
                    .parse::<u64>()
                    .ok()
                    .and_then(|id| self.names.get(&id))
                {
                    return format!("{field}#{name}");
                }
            }
        }
        word.to_owned()
    }

    /// Lines of `CLIENT LIST`, in id order, with ids named.
    fn client_list(&self, text: &str) -> String {
        let mut lines: Vec<&str> = text.lines().collect();
        let id_of = |line: &str| {
            line.split(' ')
                .find_map(|word| word.strip_prefix("id="))
                .and_then(|id| id.parse::<u64>().ok())
                .unwrap_or(0)
        };
        lines.sort_by_key(|line| id_of(line));
        let mut out = String::new();
        for line in lines {
            let words: Vec<String> = line.split(' ').map(|word| self.word(word)).collect();
            out.push_str(&words.join(" "));
            out.push('\n');
        }
        out
    }

    fn value(&self, value: Value, id_reply: bool) -> Value {
        match value {
            Value::Int(_) if id_reply => self.id(value),
            Value::Bulk(data) => {
                let text = String::from_utf8_lossy(&data).into_owned();
                let (prefix, body) = match text.strip_prefix("txt:") {
                    Some(body) => ("txt:", body),
                    None => ("", text.as_str()),
                };
                if body.starts_with("id=") {
                    Value::Bulk(format!("{prefix}{}", self.client_list(body)).into_bytes())
                } else {
                    Value::Bulk(data)
                }
            }
            Value::Push(items) => {
                let broken = items
                    .first()
                    .is_some_and(|first| is_text(first, "tracking-redir-broken"));
                Value::Push(
                    items
                        .into_iter()
                        .map(|item| {
                            if broken {
                                self.id(item)
                            } else {
                                self.value(item, false)
                            }
                        })
                        .collect(),
                )
            }
            Value::Map(pairs) => Value::Map(
                pairs
                    .into_iter()
                    .map(|(k, v)| {
                        let named = is_text(&k, "id") || is_text(&k, "redirect");
                        let v = if named {
                            self.id(v)
                        } else {
                            self.value(v, false)
                        };
                        (k, v)
                    })
                    .collect(),
            ),
            Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                let mut named = false;
                for item in items {
                    let item = if named {
                        self.id(item)
                    } else {
                        self.value(item, false)
                    };
                    named = is_text(&item, "id") || is_text(&item, "redirect");
                    out.push(item);
                }
                Value::Array(out)
            }
            Value::Set(items) => {
                Value::Set(items.into_iter().map(|i| self.value(i, false)).collect())
            }
            other => other,
        }
    }
}

/// Keeps only the named fields of a reply; see `@@` in the module docs.
fn keep_fields(value: Value, fields: &[String]) -> Value {
    let wanted = |name: &str| fields.iter().any(|field| field == name);
    match value {
        Value::Map(pairs) => Value::Map(
            pairs
                .into_iter()
                .filter(|(k, _)| wanted(&text_of(k)))
                .collect(),
        ),
        Value::Array(items) if items.len() % 2 == 0 => {
            let mut out = Vec::new();
            let mut iter = items.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                if wanted(&text_of(&k)) {
                    out.push(k);
                    out.push(v);
                }
            }
            Value::Array(out)
        }
        Value::Bulk(data) => {
            let text = String::from_utf8_lossy(&data).into_owned();
            let (prefix, body) = match text.strip_prefix("txt:") {
                Some(body) => ("txt:", body),
                None => ("", text.as_str()),
            };
            let mut out = String::from(prefix);
            if body.contains("\r\n") {
                // `INFO`: `field:value` lines.
                for line in body.split("\r\n").filter(|line| !line.is_empty()) {
                    let name = line.split(':').next().unwrap_or_default();
                    if line.starts_with('#') || wanted(name) {
                        out.push_str(line);
                        out.push_str("\r\n");
                    }
                }
            } else {
                // `CLIENT LIST`: lines of `field=value` words.
                for line in body.lines() {
                    let words: Vec<&str> = line
                        .split(' ')
                        .filter(|word| wanted(word.split('=').next().unwrap_or_default()))
                        .collect();
                    out.push_str(&words.join(" "));
                    out.push('\n');
                }
            }
            Value::Bulk(out.into_bytes())
        }
        other => other,
    }
}

struct Run {
    host: String,
    port: u16,
    conns: HashMap<String, Conn>,
    /// Connection names in the order they were opened.
    order: Vec<String>,
    /// Connection name by client id, for every connection ever opened.
    names: HashMap<u64, String>,
    ids: HashMap<String, u64>,
}

impl Run {
    fn conn(&mut self, name: &str) -> io::Result<&mut Conn> {
        if !self.conns.contains_key(name) {
            let mut conn = Conn::open(&self.host, self.port)?;
            conn.send(&[vec![b"CLIENT".to_vec(), b"ID".to_vec()]])?;
            let id = match conn.frame()? {
                Value::Int(id) => id as u64,
                other => return Err(io::Error::other(format!("CLIENT ID: {other:?}"))),
            };
            self.names.insert(id, name.to_owned());
            self.ids.insert(name.to_owned(), id);
            self.conns.insert(name.to_owned(), conn);
            self.order.push(name.to_owned());
        }
        Ok(self.conns.get_mut(name).expect("opened above"))
    }

    /// Splits a command into words, expanding `$name`, and the fields
    /// after `@@`.
    fn parse(&mut self, text: &str) -> Result<(Vec<Vec<u8>>, Vec<String>), String> {
        let (command, fields) = match text.split_once(" @@ ") {
            Some((command, fields)) => (
                command,
                fields.split_whitespace().map(str::to_owned).collect(),
            ),
            None => (text, Vec::new()),
        };
        let mut args = Vec::new();
        for word in split_words(command)? {
            match word.strip_prefix('$') {
                Some(name) if !name.is_empty() => {
                    self.conn(name).map_err(|e| format!("{name}: {e}"))?;
                    args.push(self.ids[name].to_string().into_bytes());
                }
                _ => args.push(word.into_bytes()),
            }
        }
        if args.is_empty() {
            return Err("empty command".to_owned());
        }
        Ok((args, fields))
    }

    fn show_reply(&self, reply: Value, args: &[Vec<u8>], fields: &[String]) -> String {
        let id_reply = args.len() == 2
            && args[0].eq_ignore_ascii_case(b"CLIENT")
            && (args[1].eq_ignore_ascii_case(b"ID") || args[1].eq_ignore_ascii_case(b"GETREDIR"));
        let reply = if fields.is_empty() {
            reply
        } else {
            keep_fields(reply, fields)
        };
        let normaliser = Normaliser { names: &self.names };
        show(&normaliser.value(reply, id_reply))
    }

    fn show_pending(&mut self, name: &str, suffix: &str) -> io::Result<()> {
        let frames = match self.conns.get_mut(name) {
            Some(conn) => conn.pending()?,
            None => return Ok(()),
        };
        let normaliser = Normaliser { names: &self.names };
        for frame in frames {
            println!(
                "    [{name}] gets {}{suffix}",
                show(&normaliser.value(frame, false))
            );
        }
        Ok(())
    }

    fn step(&mut self, line: &str) -> Result<(), String> {
        let fail = |e: io::Error| e.to_string();
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            if line.starts_with("##") {
                println!("{line}");
            }
            return Ok(());
        }
        if let Some(rest) = line.strip_prefix("~sleep") {
            let millis: u64 = rest.trim().parse().map_err(|_| "bad ~sleep")?;
            std::thread::sleep(Duration::from_millis(millis));
            return Ok(());
        }
        if line.starts_with("~drain") {
            for name in self.order.clone() {
                self.show_pending(&name, "").map_err(fail)?;
            }
            return Ok(());
        }
        let head = line.split_whitespace().next().unwrap_or_default();
        if let Some(name) = head.strip_suffix('|') {
            // A pipeline: commands separated by `;;`, sent at once.
            self.conn(name).map_err(fail)?;
            self.show_pending(name, "").map_err(fail)?;
            let texts: Vec<&str> = line[head.len()..].split(";;").map(str::trim).collect();
            let mut parsed = Vec::new();
            for text in &texts {
                parsed.push(self.parse(text)?);
            }
            let commands: Vec<Vec<Vec<u8>>> = parsed.iter().map(|(a, _)| a.clone()).collect();
            self.conns
                .get_mut(name)
                .expect("opened")
                .send(&commands)
                .map_err(fail)?;
            for (text, (args, fields)) in texts.iter().zip(&parsed) {
                let reply = self
                    .conns
                    .get_mut(name)
                    .expect("opened")
                    .frame()
                    .map_err(fail)?;
                println!("{name}| {text} -> {}", self.show_reply(reply, args, fields));
            }
            self.show_pending(name, " (after pipeline)").map_err(fail)?;
            return Ok(());
        }
        let (name, wait, text) = if head.contains(':') {
            let (name, text) = line.split_once(':').expect("contains ':'");
            (name.trim(), true, text.trim())
        } else if let Some((name, text)) = line.split_once('!') {
            (name.trim(), false, text.trim())
        } else {
            return Err(format!("cannot read step {line:?}"));
        };
        self.conn(name).map_err(fail)?;
        let (args, fields) = self.parse(text)?;
        // Anything that arrived before this command is shown first.
        self.show_pending(name, "").map_err(fail)?;
        let conn = self.conns.get_mut(name).expect("opened");
        conn.send(std::slice::from_ref(&args)).map_err(fail)?;
        if wait {
            let reply = conn.frame().map_err(fail)?;
            println!(
                "{name}: {text} -> {}",
                self.show_reply(reply, &args, &fields)
            );
            self.show_pending(name, " (after reply)").map_err(fail)?;
        } else {
            println!("{name}! {text}");
        }
        Ok(())
    }
}

fn main() -> ExitCode {
    let mut host = "127.0.0.1".to_owned();
    let mut port = None;
    let mut script = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match (arg.as_str(), args.next()) {
            ("--host", Some(value)) => host = value,
            ("--port", Some(value)) => port = value.parse::<u16>().ok(),
            ("--script", Some(value)) => script = Some(value),
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let (Some(port), Some(script)) = (port, script) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let text = match std::fs::read_to_string(&script) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{script}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut run = Run {
        host,
        port,
        conns: HashMap::new(),
        order: Vec::new(),
        names: HashMap::new(),
        ids: HashMap::new(),
    };
    for (number, line) in text.lines().enumerate() {
        if let Err(e) = run.step(line) {
            eprintln!("{script}:{}: {e}", number + 1);
            return ExitCode::FAILURE;
        }
    }
    std::thread::sleep(Duration::from_millis(200));
    for name in run.order.clone() {
        if let Err(e) = run.show_pending(&name, " (end)") {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_split_like_shlex() {
        assert_eq!(
            split_words(r#"EVAL "return redis.call('GET', KEYS[1])" 1 k"#).unwrap(),
            ["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]
        );
        assert_eq!(
            split_words("a 'b c' d\\ e \"\"").unwrap(),
            ["a", "b c", "d e", ""]
        );
        assert!(split_words("'open").is_err());
    }

    #[test]
    fn values_print_like_python() {
        assert_eq!(show(&Value::Bulk(b"it's".to_vec())), "\"it's\"");
        assert_eq!(show(&Value::Bulk(b"a\r\n".to_vec())), "'a\\r\\n'");
        assert_eq!(
            show(&Value::Push(vec![
                Value::Bulk(b"invalidate".to_vec()),
                Value::Array(vec![Value::Bulk(b"k".to_vec())])
            ])),
            "('PUSH', ['invalidate', ['k']])"
        );
        assert_eq!(
            show(&Value::Map(vec![(
                Value::Bulk(b"flags".to_vec()),
                Value::Set(vec![Value::Bulk(b"on".to_vec())])
            )])),
            "{'flags': ('SET', ['on'])}"
        );
    }

    #[test]
    fn ids_are_named() {
        let names = HashMap::from([(3, "r".to_owned()), (10, "t".to_owned())]);
        let n = Normaliser { names: &names };
        assert_eq!(n.value(Value::Int(3), true), Value::Id("r".to_owned()));
        assert_eq!(n.value(Value::Int(3), false), Value::Int(3));
        let list = b"id=10 flags=t redir=3\nid=3 flags=P redir=-1\n".to_vec();
        assert_eq!(
            n.value(Value::Bulk(list), false),
            Value::Bulk(b"id=#r flags=P redir=-1\nid=#t flags=t redir=#r\n".to_vec())
        );
        let flat = vec![Value::Bulk(b"redirect".to_vec()), Value::Int(10)];
        assert_eq!(
            show(&n.value(Value::Array(flat), false)),
            "['redirect', #t]"
        );
    }

    #[test]
    fn fields_are_kept() {
        let info = Value::Bulk(b"# Clients\r\na:1\r\nb:2\r\n".to_vec());
        assert_eq!(
            keep_fields(info, &["b".to_owned()]),
            Value::Bulk(b"# Clients\r\nb:2\r\n".to_vec())
        );
        let list = Value::Bulk(b"txt:id=1 addr=x flags=t\n".to_vec());
        assert_eq!(
            keep_fields(list, &["id".to_owned(), "flags".to_owned()]),
            Value::Bulk(b"txt:id=1 flags=t\n".to_vec())
        );
    }
}
