//! A minimal blocking RESP2 client: one request at a time or a pipeline,
//! replies parsed fully before returning.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

pub fn bad(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

impl Reply {
    pub fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }

    pub fn int(&self) -> io::Result<i64> {
        match self {
            Reply::Int(n) => Ok(*n),
            other => Err(bad(format!(
                "expected an integer reply, got {}",
                other.short()
            ))),
        }
    }

    pub fn text(&self) -> io::Result<String> {
        match self {
            Reply::Simple(s) => Ok(s.clone()),
            Reply::Bulk(Some(b)) => Ok(String::from_utf8_lossy(b).into_owned()),
            other => Err(bad(format!("expected a text reply, got {}", other.short()))),
        }
    }

    /// The reply for messages, cut to 200 characters.
    pub fn short(&self) -> String {
        format!("{self:?}").chars().take(200).collect()
    }
}

/// Appends a RESP array-of-bulk-strings header line (`*n` or `$n`).
pub fn write_header(buf: &mut Vec<u8>, kind: u8, n: usize) {
    buf.push(kind);
    buf.extend_from_slice(n.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Appends one command, encoded as RESP, to `buf`.
pub fn encode(buf: &mut Vec<u8>, args: &[&[u8]]) {
    write_header(buf, b'*', args.len());
    for arg in args {
        write_header(buf, b'$', arg.len());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
}

pub struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    line: Vec<u8>,
}

impl Conn {
    pub fn connect(port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_nodelay(true)?;
        let writer = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::with_capacity(256 * 1024, stream),
            writer,
            line: Vec::with_capacity(64),
        })
    }

    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.writer.set_read_timeout(timeout)?;
        self.writer.set_write_timeout(timeout)
    }

    pub fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)
    }

    /// Reads one complete reply.
    pub fn read(&mut self) -> io::Result<Reply> {
        self.line.clear();
        let n = self.reader.read_until(b'\n', &mut self.line)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the server closed the connection",
            ));
        }
        if self.line.len() < 3 || !self.line.ends_with(b"\r\n") {
            return Err(bad(format!(
                "malformed reply line {:?}",
                String::from_utf8_lossy(&self.line)
            )));
        }
        let kind = self.line[0];
        let body = String::from_utf8_lossy(&self.line[1..self.line.len() - 2]).into_owned();
        let number = |body: &str| -> io::Result<i64> {
            body.parse()
                .map_err(|_| bad(format!("bad number in reply: {body:?}")))
        };
        match kind {
            b'+' => Ok(Reply::Simple(body)),
            b'-' => Ok(Reply::Error(body)),
            b':' => Ok(Reply::Int(number(&body)?)),
            b'$' => {
                let n = number(&body)?;
                if n < 0 {
                    return Ok(Reply::Bulk(None));
                }
                let n = usize::try_from(n).map_err(|_| bad("bulk length"))?;
                let mut data = vec![0u8; n + 2];
                self.reader.read_exact(&mut data)?;
                if &data[n..] != b"\r\n" {
                    return Err(bad("bulk string not followed by CRLF"));
                }
                data.truncate(n);
                Ok(Reply::Bulk(Some(data)))
            }
            b'*' => {
                let n = number(&body)?;
                if n < 0 {
                    return Ok(Reply::Array(None));
                }
                let n = usize::try_from(n).map_err(|_| bad("array length"))?;
                let mut items = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    items.push(self.read()?);
                }
                Ok(Reply::Array(Some(items)))
            }
            other => Err(bad(format!("unknown reply type byte {other:#x}"))),
        }
    }

    pub fn call(&mut self, args: &[&[u8]]) -> io::Result<Reply> {
        let mut buf = Vec::new();
        encode(&mut buf, args);
        self.send(&buf)?;
        self.read()
    }

    pub fn call_str(&mut self, args: &[&str]) -> io::Result<Reply> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.call(&args)
    }
}

/// Finds `key:value` in an `INFO` reply.
pub fn info_field<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.trim())
}
