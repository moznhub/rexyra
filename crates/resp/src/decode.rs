use bytes::{Buf, Bytes, BytesMut};

use crate::util::{read_line, Line};
use crate::{parse_i64, Frame, ProtocolError, MAX_AGGREGATE_LEN, MAX_BULK_LEN, MAX_DEPTH};

/// Decodes one RESP2/RESP3 value from the front of `buf`.
///
/// Returns `Ok(None)` and leaves `buf` untouched when the value is not yet
/// complete. On success the consumed bytes are removed from `buf`.
pub fn decode(buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
    let mut pos = 0;
    match parse(&buf[..], &mut pos, 0) {
        Ok(frame) => {
            buf.advance(pos);
            Ok(Some(frame))
        }
        Err(Stop::Incomplete) => Ok(None),
        Err(Stop::Invalid(e)) => Err(e),
    }
}

enum Stop {
    Incomplete,
    Invalid(ProtocolError),
}

impl From<ProtocolError> for Stop {
    fn from(e: ProtocolError) -> Self {
        Stop::Invalid(e)
    }
}

fn invalid(message: impl Into<String>) -> Stop {
    Stop::Invalid(ProtocolError::new(message))
}

fn line<'a>(src: &'a [u8], pos: &mut usize) -> Result<&'a [u8], Stop> {
    match read_line(src, pos, "Protocol error: line too long")? {
        Line::Complete(body) => Ok(body),
        Line::Incomplete => Err(Stop::Incomplete),
    }
}

fn text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

/// The text of a simple string or error. Neither may contain a line break:
/// a line ends at the first CRLF, so the only one that could get this far
/// is a carriage return on its own, and that is refused rather than passed
/// on as part of the text.
fn line_text(body: &[u8]) -> Result<String, Stop> {
    if body.contains(&b'\r') {
        return Err(invalid(
            "Protocol error: carriage return inside a simple string",
        ));
    }
    Ok(text(body))
}

/// Parses a length header. `None` means the RESP2 null marker (`-1`).
fn length(body: &[u8], max: usize, what: &str) -> Result<Option<usize>, Stop> {
    match parse_i64(body) {
        Some(-1) => Ok(None),
        Some(n) if n >= 0 && (n as u64) <= max as u64 => Ok(Some(n as usize)),
        _ => Err(invalid(format!("Protocol error: invalid {what} length"))),
    }
}

/// Reads `len` payload bytes followed by CRLF.
fn payload<'a>(src: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8], Stop> {
    let end = *pos + len;
    if src.len() < end + 2 {
        return Err(Stop::Incomplete);
    }
    if &src[end..end + 2] != b"\r\n" {
        return Err(invalid("Protocol error: expected CRLF after bulk payload"));
    }
    let data = &src[*pos..end];
    *pos = end + 2;
    Ok(data)
}

fn items(src: &[u8], pos: &mut usize, count: usize, depth: usize) -> Result<Vec<Frame>, Stop> {
    // The count is untrusted: never pre-allocate more than a small amount.
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        out.push(parse(src, pos, depth + 1)?);
    }
    Ok(out)
}

fn parse(src: &[u8], pos: &mut usize, depth: usize) -> Result<Frame, Stop> {
    if depth > MAX_DEPTH {
        return Err(invalid("Protocol error: nesting too deep"));
    }
    let Some(&tag) = src.get(*pos) else {
        return Err(Stop::Incomplete);
    };
    *pos += 1;
    let body = line(src, pos)?;

    match tag {
        b'+' => Ok(Frame::Simple(line_text(body)?)),
        b'-' => Ok(Frame::Error(line_text(body)?)),
        b':' => parse_i64(body)
            .map(Frame::Integer)
            .ok_or_else(|| invalid("Protocol error: invalid integer")),
        b'$' => match length(body, MAX_BULK_LEN, "bulk")? {
            None => Ok(Frame::Null),
            Some(len) => Ok(Frame::Bulk(Bytes::copy_from_slice(payload(src, pos, len)?))),
        },
        b'*' => match length(body, MAX_AGGREGATE_LEN, "multibulk")? {
            None => Ok(Frame::Null),
            Some(count) => Ok(Frame::Array(items(src, pos, count, depth)?)),
        },
        b'_' => {
            if body.is_empty() {
                Ok(Frame::Null)
            } else {
                Err(invalid("Protocol error: invalid null"))
            }
        }
        b'#' => match body {
            b"t" => Ok(Frame::Boolean(true)),
            b"f" => Ok(Frame::Boolean(false)),
            _ => Err(invalid("Protocol error: invalid boolean")),
        },
        b',' => std::str::from_utf8(body)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .map(Frame::Double)
            .ok_or_else(|| invalid("Protocol error: invalid double")),
        b'(' => {
            let digits = body.strip_prefix(b"-").unwrap_or(body);
            if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
                return Err(invalid("Protocol error: invalid big number"));
            }
            Ok(Frame::BigNumber(text(body)))
        }
        b'=' => {
            let len = length(body, MAX_BULK_LEN, "verbatim string")?
                .filter(|&len| len >= 4)
                .ok_or_else(|| invalid("Protocol error: invalid verbatim string length"))?;
            let data = payload(src, pos, len)?;
            if data[3] != b':' {
                return Err(invalid("Protocol error: invalid verbatim string"));
            }
            Ok(Frame::Verbatim {
                format: [data[0], data[1], data[2]],
                data: Bytes::copy_from_slice(&data[4..]),
            })
        }
        b'%' => {
            let count = length(body, MAX_AGGREGATE_LEN, "map")?
                .ok_or_else(|| invalid("Protocol error: invalid map length"))?;
            let mut pairs = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let key = parse(src, pos, depth + 1)?;
                let value = parse(src, pos, depth + 1)?;
                pairs.push((key, value));
            }
            Ok(Frame::Map(pairs))
        }
        b'~' => {
            let count = length(body, MAX_AGGREGATE_LEN, "set")?
                .ok_or_else(|| invalid("Protocol error: invalid set length"))?;
            Ok(Frame::Set(items(src, pos, count, depth)?))
        }
        b'>' => {
            let count = length(body, MAX_AGGREGATE_LEN, "push")?
                .ok_or_else(|| invalid("Protocol error: invalid push length"))?;
            Ok(Frame::Push(items(src, pos, count, depth)?))
        }
        other => Err(invalid(format!(
            "Protocol error: unknown type byte 0x{other:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode, Protocol};
    use proptest::prelude::*;

    fn dec(input: &[u8]) -> Result<Option<Frame>, ProtocolError> {
        decode(&mut BytesMut::from(input))
    }

    #[test]
    fn decodes_resp2_values() {
        assert_eq!(dec(b"+OK\r\n").unwrap(), Some(Frame::ok()));
        assert_eq!(dec(b"-ERR x\r\n").unwrap(), Some(Frame::error("ERR x")));
        assert_eq!(dec(b":12\r\n").unwrap(), Some(Frame::Integer(12)));
        assert_eq!(dec(b"$3\r\nfoo\r\n").unwrap(), Some(Frame::bulk("foo")));
        assert_eq!(dec(b"$-1\r\n").unwrap(), Some(Frame::Null));
        assert_eq!(dec(b"*-1\r\n").unwrap(), Some(Frame::Null));
        assert_eq!(
            dec(b"*2\r\n:1\r\n$1\r\na\r\n").unwrap(),
            Some(Frame::Array(vec![Frame::Integer(1), Frame::bulk("a")]))
        );
    }

    #[test]
    fn a_bare_carriage_return_in_a_line_is_refused() {
        // Found by fuzzing: these were accepted, with the carriage return as
        // part of the text, and could not be written back as they were read.
        for input in [
            &b"+a\rb\r\n"[..],
            b"-~,\r\r\n",
            b"+\r\r\n",
            b"*1\r\n-ERR\rx\r\n",
        ] {
            let error = dec(input).unwrap_err();
            assert!(
                error.to_string().contains("carriage return"),
                "{:?}: {error}",
                input.escape_ascii().to_string()
            );
        }
        // Without one, the same lines are fine, other control bytes included.
        assert_eq!(dec(b"+a b\r\n").unwrap(), Some(Frame::Simple("a b".into())));
        assert_eq!(
            dec(b"+a\tb\0\r\n").unwrap(),
            Some(Frame::Simple("a\tb\0".into()))
        );
        assert_eq!(dec(b"-\r\n").unwrap(), Some(Frame::error("")));
        // A bulk string may hold anything.
        assert_eq!(dec(b"$3\r\na\rb\r\n").unwrap(), Some(Frame::bulk("a\rb")));
    }

    #[test]
    fn decodes_resp3_values() {
        assert_eq!(dec(b"_\r\n").unwrap(), Some(Frame::Null));
        assert_eq!(dec(b"#t\r\n").unwrap(), Some(Frame::Boolean(true)));
        assert_eq!(dec(b",2.5\r\n").unwrap(), Some(Frame::Double(2.5)));
        assert_eq!(
            dec(b",-inf\r\n").unwrap(),
            Some(Frame::Double(f64::NEG_INFINITY))
        );
        assert_eq!(
            dec(b"%1\r\n+a\r\n:1\r\n").unwrap(),
            Some(Frame::Map(vec![(Frame::simple("a"), Frame::Integer(1))]))
        );
        assert_eq!(
            dec(b"=7\r\ntxt:hey\r\n").unwrap(),
            Some(Frame::Verbatim {
                format: *b"txt",
                data: Bytes::from_static(b"hey")
            })
        );
    }

    #[test]
    fn incomplete_input_consumes_nothing() {
        for partial in [
            &b""[..],
            b"+OK",
            b"+OK\r",
            b"$3\r\nfo",
            b"$3\r\nfoo\r",
            b"*2\r\n:1\r\n",
        ] {
            let mut buf = BytesMut::from(partial);
            assert_eq!(decode(&mut buf).unwrap(), None);
            assert_eq!(&buf[..], partial);
        }
    }

    #[test]
    fn leaves_following_bytes_in_buffer() {
        let mut buf = BytesMut::from(&b":1\r\n:2\r\n"[..]);
        assert_eq!(decode(&mut buf).unwrap(), Some(Frame::Integer(1)));
        assert_eq!(&buf[..], b":2\r\n");
    }

    #[test]
    fn rejects_malformed_input() {
        for bad in [
            &b"?\r\n"[..],
            b":abc\r\n",
            b"$-2\r\n",
            b"$3\r\nfooXX",
            b"*99999999999\r\n",
            b"#x\r\n",
            b"=2\r\nab\r\n",
            b"+OK\n",
        ] {
            assert!(dec(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn rejects_excessive_nesting() {
        let input = "*1\r\n".repeat(MAX_DEPTH + 2);
        assert!(dec(input.as_bytes()).is_err());
    }

    fn line_text() -> impl Strategy<Value = String> {
        "[ -~]{0,24}"
    }

    fn frame() -> impl Strategy<Value = Frame> {
        let leaf = prop_oneof![
            line_text().prop_map(Frame::Simple),
            line_text().prop_map(Frame::Error),
            any::<i64>().prop_map(Frame::Integer),
            prop::collection::vec(any::<u8>(), 0..48).prop_map(|v| Frame::Bulk(Bytes::from(v))),
            Just(Frame::Null),
            any::<f64>()
                .prop_filter("NaN is not equal to itself", |d| !d.is_nan())
                .prop_map(Frame::Double),
            any::<bool>().prop_map(Frame::Boolean),
            "-?[0-9]{1,40}".prop_map(Frame::BigNumber),
            (any::<[u8; 3]>(), prop::collection::vec(any::<u8>(), 0..24)).prop_map(
                |(format, data)| Frame::Verbatim {
                    format,
                    data: Bytes::from(data)
                }
            ),
        ];
        leaf.prop_recursive(4, 48, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Frame::Array),
                prop::collection::vec(inner.clone(), 0..6).prop_map(Frame::Set),
                prop::collection::vec(inner.clone(), 0..6).prop_map(Frame::Push),
                prop::collection::vec((inner.clone(), inner), 0..4).prop_map(Frame::Map),
            ]
        })
    }

    /// A small deterministic source of bytes: enough to explore the grammar
    /// in an ordinary test run, without a fuzzer.
    struct Noise(u64);

    impl Noise {
        fn next(&mut self) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 >> 16) as usize
        }

        /// Bytes that look enough like RESP for the decoder to accept a fair
        /// share of them.
        fn resp_like(&mut self) -> Vec<u8> {
            const PIECES: &[&[u8]] = &[
                b"+",
                b"-",
                b":",
                b"$",
                b"*",
                b"_",
                b"#",
                b",",
                b"(",
                b"=",
                b"%",
                b"~",
                b">",
                b"\r\n",
                b"\r\n",
                b"\r\n",
                b"\r",
                b"\n",
                b"0",
                b"1",
                b"2",
                b"3",
                b"4",
                b"7",
                b"-1",
                b"t",
                b"f",
                b"txt:",
                b"a",
                b"OK",
                b"inf",
                b"nan",
                b".",
                b"e",
                b"E",
                b" ",
                b"\xff",
                b"\0",
                b"$5\r\nhello\r\n",
                b":12\r\n",
                b"+x\r\n",
            ];
            let mut out = Vec::new();
            for _ in 0..1 + self.next() % 10 {
                out.extend_from_slice(PIECES[self.next() % PIECES.len()]);
            }
            out
        }
    }

    fn has_nan(frame: &Frame) -> bool {
        match frame {
            Frame::Double(d) => d.is_nan(),
            Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => {
                items.iter().any(has_nan)
            }
            Frame::Map(pairs) => pairs.iter().any(|(k, v)| has_nan(k) || has_nan(v)),
            _ => false,
        }
    }

    /// The property the fuzz target checks, on generated input: whatever
    /// the decoder accepts, the encoder writes back in a form that decodes
    /// to the same value.
    #[test]
    fn whatever_decodes_survives_reencoding() {
        let mut noise = Noise(0x9E37_79B9_7F4A_7C15);
        let mut accepted = 0;
        for _ in 0..300_000 {
            let input = noise.resp_like();
            let mut buf = BytesMut::from(&input[..]);
            let Ok(Some(frame)) = decode(&mut buf) else {
                continue;
            };
            // NaN is not equal to itself, so it cannot be compared.
            if has_nan(&frame) {
                continue;
            }
            accepted += 1;
            let mut wire = BytesMut::new();
            encode(&frame, Protocol::Resp3, &mut wire);
            let again = decode(&mut wire);
            assert_eq!(
                again.ok().flatten().as_ref(),
                Some(&frame),
                "input {:?}",
                input.escape_ascii().to_string()
            );
            assert!(
                wire.is_empty(),
                "input {:?}",
                input.escape_ascii().to_string()
            );
        }
        assert!(accepted > 20_000, "only {accepted} inputs were accepted");
    }

    proptest! {
        #[test]
        fn resp3_round_trip(frame in frame()) {
            let mut buf = BytesMut::new();
            encode(&frame, Protocol::Resp3, &mut buf);
            let decoded = decode(&mut buf).unwrap();
            prop_assert_eq!(decoded, Some(frame));
            prop_assert!(buf.is_empty());
        }

        #[test]
        fn truncated_frames_are_incomplete(frame in frame(), cut in any::<prop::sample::Index>()) {
            let mut full = BytesMut::new();
            encode(&frame, Protocol::Resp3, &mut full);
            let cut = cut.index(full.len());
            let mut partial = BytesMut::from(&full[..cut]);
            prop_assert_eq!(decode(&mut partial).unwrap(), None);
            prop_assert_eq!(partial.len(), cut);
        }

        #[test]
        fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..256)) {
            let mut buf = BytesMut::from(&data[..]);
            let before = buf.len();
            match decode(&mut buf) {
                Ok(Some(_)) => prop_assert!(buf.len() < before),
                Ok(None) | Err(_) => prop_assert_eq!(buf.len(), before),
            }
        }
    }
}
