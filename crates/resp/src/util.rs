use crate::{ProtocolError, MAX_LINE_LEN};

/// Strict decimal `i64` parser with Redis's rules: optional leading `-`,
/// digits only, no whitespace, no leading `+`, no leading zeros, no `-0`, no
/// overflow. A value accepted here always formats back to the same bytes.
pub fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some(_) => (false, bytes),
        None => return None,
    };
    if digits.is_empty() || digits.len() > 19 {
        return None;
    }
    if digits[0] == b'0' && (negative || digits.len() > 1) {
        return None;
    }
    // Accumulate as a negative number so that i64::MIN is representable.
    let mut value: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_sub(i64::from(b - b'0'))?;
    }
    if negative {
        Some(value)
    } else {
        value.checked_neg()
    }
}

/// Outcome of looking for a CRLF-terminated line.
pub(crate) enum Line<'a> {
    /// The line body (without CRLF); `pos` has been moved past the terminator.
    Complete(&'a [u8]),
    /// No terminator yet; more bytes are needed.
    Incomplete,
}

/// Reads a CRLF-terminated line starting at `*pos`.
///
/// `too_long` is the error reported when more than [`MAX_LINE_LEN`] bytes
/// arrive without a terminator.
pub(crate) fn read_line<'a>(
    src: &'a [u8],
    pos: &mut usize,
    too_long: &str,
) -> Result<Line<'a>, ProtocolError> {
    let rest = &src[*pos..];
    match rest.iter().position(|&b| b == b'\n') {
        None => {
            if rest.len() > MAX_LINE_LEN {
                Err(ProtocolError::new(too_long))
            } else {
                Ok(Line::Incomplete)
            }
        }
        Some(nl) => {
            if nl == 0 || rest[nl - 1] != b'\r' {
                return Err(ProtocolError::new("Protocol error: expected CRLF"));
            }
            *pos += nl + 1;
            Ok(Line::Complete(&rest[..nl - 1]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_i64;

    #[test]
    fn parses_valid_integers() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-1"), Some(-1));
        assert_eq!(parse_i64(b"42"), Some(42));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn rejects_invalid_integers() {
        for bad in [
            &b""[..],
            b"-",
            b"+1",
            b" 1",
            b"1 ",
            b"1a",
            b"01",
            b"00",
            b"-0",
            b"-01",
            b"9223372036854775808",
            b"-9223372036854775809",
            b"99999999999999999999",
        ] {
            assert_eq!(parse_i64(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
