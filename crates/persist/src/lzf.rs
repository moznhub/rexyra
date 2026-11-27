//! LZF decompression. Redis compresses long strings in RDB files with LZF
//! by default, so reading its files needs this. This server writes strings
//! uncompressed.

/// The most one byte of input can expand to: a back-reference of three
/// bytes copies up to 264.
const MAX_EXPANSION: usize = 264;

/// Decompresses `input`, which must expand to exactly `expected` bytes.
///
/// `expected` comes from the file or payload being read, so it is checked
/// against what `input` could possibly expand to before any memory is
/// reserved for it.
pub(crate) fn decompress(input: &[u8], expected: usize) -> Option<Vec<u8>> {
    if expected > input.len().saturating_mul(MAX_EXPANSION) {
        return None;
    }
    let mut out: Vec<u8> = Vec::with_capacity(expected);
    let mut at = 0;
    while at < input.len() {
        let control = usize::from(input[at]);
        at += 1;
        if control < 32 {
            // A run of literal bytes.
            let run = control + 1;
            out.extend_from_slice(input.get(at..at + run)?);
            at += run;
        } else {
            // A copy of bytes already written.
            let mut length = control >> 5;
            if length == 7 {
                length += usize::from(*input.get(at)?);
                at += 1;
            }
            length += 2;
            let distance = ((control & 0x1F) << 8) + usize::from(*input.get(at)?) + 1;
            at += 1;
            let start = out.len().checked_sub(distance)?;
            // Byte by byte: the source may overlap what is being written.
            for from in start..start + length {
                out.push(out[from]);
            }
        }
        if out.len() > expected {
            return None;
        }
    }
    (out.len() == expected).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::decompress;

    #[test]
    fn literals_and_back_references() {
        // "abc" literally.
        assert_eq!(decompress(&[2, b'a', b'b', b'c'], 3).unwrap(), b"abc");
        // "a", then copy 9 bytes from one byte back: ten a's.
        assert_eq!(
            decompress(&[0, b'a', 0xE0, 0, 0], 10).unwrap(),
            b"aaaaaaaaaa"
        );
        // "ab", then copy 4 bytes from two back: "ababab".
        assert_eq!(decompress(&[1, b'a', b'b', 0x40, 1], 6).unwrap(), b"ababab");
    }

    #[test]
    fn a_length_the_input_cannot_reach_is_refused_without_reserving_it() {
        // The expected length comes from the file or the RESTORE payload.
        assert!(decompress(&[0, b'a'], usize::MAX).is_none());
        assert!(decompress(&[0, b'a'], 1 << 40).is_none());
        assert!(decompress(&[0, b'a', 0xE0, 0, 0], 1 << 30).is_none());
    }

    #[test]
    fn rejects_bad_input() {
        assert!(decompress(&[2, b'a'], 3).is_none()); // literal run cut short
        assert!(decompress(&[0x40, 5], 4).is_none()); // reference before the start
        assert!(decompress(&[2, b'a', b'b', b'c'], 2).is_none()); // longer than promised
        assert!(decompress(&[0, b'a'], 5).is_none()); // shorter than promised
    }
}
