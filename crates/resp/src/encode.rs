use bytes::{BufMut, BytesMut};

use crate::{Frame, Protocol};

/// Appends the wire form of `frame` to `out`.
///
/// RESP3-only types are downgraded when `proto` is [`Protocol::Resp2`], the
/// same way Redis does it, so handlers never branch on protocol version.
pub fn encode(frame: &Frame, proto: Protocol, out: &mut BytesMut) {
    let resp3 = proto == Protocol::Resp3;
    match frame {
        Frame::Simple(s) => line(out, b'+', s.as_bytes()),
        Frame::Error(s) => line(out, b'-', s.as_bytes()),
        Frame::Integer(n) => {
            out.put_u8(b':');
            out.put_slice(itoa::Buffer::new().format(*n).as_bytes());
            out.put_slice(b"\r\n");
        }
        Frame::Bulk(b) => bulk(out, b),
        Frame::Null => out.put_slice(if resp3 { b"_\r\n" } else { b"$-1\r\n" }),
        Frame::NullArray => out.put_slice(if resp3 { b"_\r\n" } else { b"*-1\r\n" }),
        Frame::Array(items) => {
            header(out, b'*', items.len());
            encode_all(items, proto, out);
        }
        Frame::Double(d) => {
            let text = format_double(*d);
            if resp3 {
                line(out, b',', text.as_bytes());
            } else {
                bulk(out, text.as_bytes());
            }
        }
        Frame::Boolean(b) => out.put_slice(match (resp3, *b) {
            (true, true) => b"#t\r\n",
            (true, false) => b"#f\r\n",
            (false, true) => b":1\r\n",
            (false, false) => b":0\r\n",
        }),
        Frame::BigNumber(s) => {
            if resp3 {
                line(out, b'(', s.as_bytes());
            } else {
                bulk(out, s.as_bytes());
            }
        }
        Frame::Verbatim { format, data } => {
            if resp3 {
                header(out, b'=', data.len() + 4);
                out.put_slice(format);
                out.put_u8(b':');
                out.put_slice(data);
                out.put_slice(b"\r\n");
            } else {
                bulk(out, data);
            }
        }
        Frame::Map(pairs) => {
            if resp3 {
                header(out, b'%', pairs.len());
            } else {
                header(out, b'*', pairs.len() * 2);
            }
            for (key, value) in pairs {
                encode(key, proto, out);
                encode(value, proto, out);
            }
        }
        Frame::Set(items) => {
            header(out, if resp3 { b'~' } else { b'*' }, items.len());
            encode_all(items, proto, out);
        }
        Frame::Push(items) => {
            header(out, if resp3 { b'>' } else { b'*' }, items.len());
            encode_all(items, proto, out);
        }
        Frame::Sequence(items) => encode_all(items, proto, out),
    }
}

fn encode_all(items: &[Frame], proto: Protocol, out: &mut BytesMut) {
    for item in items {
        encode(item, proto, out);
    }
}

fn header(out: &mut BytesMut, tag: u8, len: usize) {
    out.put_u8(tag);
    out.put_slice(itoa::Buffer::new().format(len).as_bytes());
    out.put_slice(b"\r\n");
}

fn bulk(out: &mut BytesMut, data: &[u8]) {
    header(out, b'$', data.len());
    out.put_slice(data);
    out.put_slice(b"\r\n");
}

/// Writes a line-type value. A CR or LF inside the text would desynchronise
/// the stream, so they are replaced with spaces.
fn line(out: &mut BytesMut, tag: u8, text: &[u8]) {
    out.put_u8(tag);
    if text.iter().any(|&b| b == b'\r' || b == b'\n') {
        out.extend(
            text.iter()
                .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
        );
    } else {
        out.put_slice(text);
    }
    out.put_slice(b"\r\n");
}

/// Formats a double the way Redis prints scores and other double replies:
/// the shortest digits that parse back to the same value, in plain notation
/// for ordinary magnitudes and scientific notation (`1e+20`, `1.5e-7`) for
/// very large or very small ones.
pub fn format_double(d: f64) -> String {
    if d.is_nan() {
        return "nan".to_owned();
    }
    if d.is_infinite() {
        return if d > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }

    // Whole numbers that a double represents exactly print as integers.
    // Whole numbers up to half the 64-bit integer range print as integers.
    const INTEGER_LIMIT: f64 = (i64::MAX / 2) as f64;
    if d.fract() == 0.0 && d.abs() <= INTEGER_LIMIT {
        return (d as i64).to_string();
    }

    // Rust's `{:e}` already yields the shortest round-tripping digits, as
    // `d[.ddd]e<exp>`; only the layout needs to follow Redis.
    let scientific = format!("{:e}", d.abs());
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always contains an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent.parse().expect("`{:e}` exponent is an integer");
    let count = digits.len() as i32;
    // The value is `digits × 10^shift`.
    let shift = exponent - (count - 1);
    let negative = d < 0.0;

    let mut out = String::with_capacity(24);
    if negative {
        out.push('-');
    }
    let max_trailing_zeros = if negative { 6 } else { 7 };
    if shift >= 0 && exponent < count + max_trailing_zeros {
        // An integer: the digits, then zeros.
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', shift as usize));
    } else if shift < 0 && (shift > -7 || exponent.abs() < 4) {
        let integer_digits = count + shift;
        if integer_digits <= 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-integer_digits) as usize));
            out.push_str(&digits);
        } else {
            let (whole, fraction) = digits.split_at(integer_digits as usize);
            out.push_str(whole);
            out.push('.');
            out.push_str(fraction);
        }
    } else {
        let (first, rest) = digits.split_at(1);
        out.push_str(first);
        if !rest.is_empty() {
            out.push('.');
            out.push_str(rest);
        }
        out.push('e');
        out.push(if exponent < 0 { '-' } else { '+' });
        out.push_str(&exponent.abs().to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn enc(frame: &Frame, proto: Protocol) -> String {
        let mut out = BytesMut::new();
        encode(frame, proto, &mut out);
        String::from_utf8(out.to_vec()).unwrap()
    }

    #[test]
    fn scalars() {
        assert_eq!(enc(&Frame::ok(), Protocol::Resp2), "+OK\r\n");
        assert_eq!(enc(&Frame::error("ERR x"), Protocol::Resp2), "-ERR x\r\n");
        assert_eq!(enc(&Frame::Integer(-7), Protocol::Resp2), ":-7\r\n");
        assert_eq!(enc(&Frame::bulk("hi"), Protocol::Resp2), "$2\r\nhi\r\n");
        assert_eq!(enc(&Frame::bulk(""), Protocol::Resp2), "$0\r\n\r\n");
    }

    #[test]
    fn null_differs_by_protocol() {
        assert_eq!(enc(&Frame::Null, Protocol::Resp2), "$-1\r\n");
        assert_eq!(enc(&Frame::Null, Protocol::Resp3), "_\r\n");
        assert_eq!(enc(&Frame::NullArray, Protocol::Resp2), "*-1\r\n");
        assert_eq!(enc(&Frame::NullArray, Protocol::Resp3), "_\r\n");
    }

    #[test]
    fn map_is_flattened_for_resp2() {
        let map = Frame::Map(vec![(Frame::bulk("a"), Frame::Integer(1))]);
        assert_eq!(enc(&map, Protocol::Resp3), "%1\r\n$1\r\na\r\n:1\r\n");
        assert_eq!(enc(&map, Protocol::Resp2), "*2\r\n$1\r\na\r\n:1\r\n");
    }

    #[test]
    fn resp3_types_downgrade() {
        assert_eq!(enc(&Frame::Boolean(true), Protocol::Resp3), "#t\r\n");
        assert_eq!(enc(&Frame::Boolean(true), Protocol::Resp2), ":1\r\n");
        assert_eq!(enc(&Frame::Double(1.5), Protocol::Resp3), ",1.5\r\n");
        assert_eq!(enc(&Frame::Double(1.5), Protocol::Resp2), "$3\r\n1.5\r\n");
        assert_eq!(enc(&Frame::Double(3.0), Protocol::Resp3), ",3\r\n");
        assert_eq!(
            enc(&Frame::Double(f64::NEG_INFINITY), Protocol::Resp3),
            ",-inf\r\n"
        );
        let verbatim = Frame::Verbatim {
            format: *b"txt",
            data: Bytes::from_static(b"hey"),
        };
        assert_eq!(enc(&verbatim, Protocol::Resp3), "=7\r\ntxt:hey\r\n");
        assert_eq!(enc(&verbatim, Protocol::Resp2), "$3\r\nhey\r\n");
        let set = Frame::Set(vec![Frame::Integer(1)]);
        assert_eq!(enc(&set, Protocol::Resp3), "~1\r\n:1\r\n");
        assert_eq!(enc(&set, Protocol::Resp2), "*1\r\n:1\r\n");
    }

    #[test]
    fn doubles_are_formatted_like_redis() {
        for (value, expected) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (3.25, "3.25"),
            (0.1, "0.1"),
            (0.5, "0.5"),
            (100.0, "100"),
            (1e7, "10000000"),
            (12345678.0, "12345678"),
            (1e8, "100000000"),
            (4503599627370495.0, "4503599627370495"),
            (4503599627370496.0, "4503599627370496"),
            (1e17, "100000000000000000"),
            (4e18, "4000000000000000000"),
            (5e18, "5e+18"),
            (1.5e10, "15000000000"),
            (1e20, "1e+20"),
            (1.2345e21, "1.2345e+21"),
            (0.0001, "0.0001"),
            (0.000001, "0.000001"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (123.456, "123.456"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            assert_eq!(format_double(value), expected);
        }
    }

    #[test]
    fn formatted_doubles_parse_back_exactly() {
        for value in [
            0.1 + 0.2,
            1.0 / 3.0,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            -123456789.125,
            9007199254740993.0,
        ] {
            let text = format_double(value);
            assert_eq!(text.parse::<f64>().unwrap(), value, "{text}");
        }
    }

    #[test]
    fn line_breaks_in_errors_are_neutralised() {
        assert_eq!(
            enc(&Frame::error("ERR a\r\nb"), Protocol::Resp2),
            "-ERR a  b\r\n"
        );
    }
}
