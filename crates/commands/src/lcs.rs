//! `LCS` — longest common subsequence of two strings.

use bytes::Bytes;
use rexyra_resp::{Frame, MAX_BULK_LEN};

use crate::strings::read;
use crate::{int, is, reply, Ctx, ERR_SYNTAX};

/// One run of consecutive matching bytes: inclusive ranges in each string.
struct Match {
    a: (usize, usize),
    b: (usize, usize),
}

impl Match {
    fn len(&self) -> usize {
        self.a.1 - self.a.0 + 1
    }
}

/// Computes the LCS of `a` and `b` and the matching runs that make it up,
/// ordered from the end of the strings towards the start (the order Redis
/// reports them in).
fn compute(a: &[u8], b: &[u8]) -> (Vec<u8>, Vec<Match>) {
    let width = b.len() + 1;
    // table[i * width + j] = LCS length of a[..i] and b[..j].
    let mut table = vec![0u32; (a.len() + 1) * width];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            table[i * width + j] = if a[i - 1] == b[j - 1] {
                table[(i - 1) * width + (j - 1)] + 1
            } else {
                table[(i - 1) * width + j].max(table[i * width + (j - 1)])
            };
        }
    }

    // Walk back from the bottom-right corner, collecting the subsequence
    // (backwards) and coalescing adjacent matches into runs.
    let mut lcs = vec![0u8; table[a.len() * width + b.len()] as usize];
    let mut remaining = lcs.len();
    let mut matches = Vec::new();
    let mut run: Option<Match> = None;
    let (mut i, mut j) = (a.len(), b.len());

    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            remaining -= 1;
            lcs[remaining] = a[i - 1];
            match &mut run {
                // Extends the current run one byte to the left.
                Some(current) if current.a.0 == i && current.b.0 == j => {
                    current.a.0 -= 1;
                    current.b.0 -= 1;
                }
                _ => {
                    matches.extend(run.take());
                    run = Some(Match {
                        a: (i - 1, i - 1),
                        b: (j - 1, j - 1),
                    });
                }
            }
            i -= 1;
            j -= 1;
        } else {
            matches.extend(run.take());
            if table[(i - 1) * width + j] > table[i * width + (j - 1)] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
    }
    matches.extend(run);
    (lcs, matches)
}

/// `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]`
pub(crate) fn lcs(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (mut want_len, mut want_idx, mut with_match_len) = (false, false, false);
        let mut min_match_len = 0usize;
        let mut options = &args[3..];
        loop {
            match options {
                [] => break,
                [opt, rest @ ..] if is(opt, "LEN") => {
                    want_len = true;
                    options = rest;
                }
                [opt, rest @ ..] if is(opt, "IDX") => {
                    want_idx = true;
                    options = rest;
                }
                [opt, rest @ ..] if is(opt, "WITHMATCHLEN") => {
                    with_match_len = true;
                    options = rest;
                }
                [opt, value, rest @ ..] if is(opt, "MINMATCHLEN") => {
                    min_match_len = int(value)?.max(0) as usize;
                    options = rest;
                }
                _ => return Err(Frame::error(ERR_SYNTAX)),
            }
        }
        if want_len && want_idx {
            return Err(Frame::error(
                "ERR If you want both the length and indexes, please just use IDX.",
            ));
        }

        // Unlike other string commands, LCS does not answer WRONGTYPE.
        let not_strings = |_| Frame::error("ERR The specified keys must contain string values");
        let a = read(ctx, &args[1])
            .map_err(not_strings)?
            .unwrap_or_default();
        let b = read(ctx, &args[2])
            .map_err(not_strings)?
            .unwrap_or_default();
        // The table holds one u32 per pair of prefixes; refuse inputs whose
        // table would exceed the largest value the server allows.
        let cells = (a.len() as u64 + 1) * (b.len() as u64 + 1);
        if cells > (MAX_BULK_LEN / 4) as u64 {
            return Err(Frame::error(
                "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
            ));
        }
        let (lcs, matches) = compute(&a, &b);

        if want_len {
            return Ok(Frame::Integer(lcs.len() as i64));
        }
        if !want_idx {
            return Ok(Frame::bulk(lcs));
        }
        let range = |(start, end): (usize, usize)| {
            Frame::Array(vec![
                Frame::Integer(start as i64),
                Frame::Integer(end as i64),
            ])
        };
        let matches = matches
            .iter()
            .filter(|m| m.len() >= min_match_len)
            .map(|m| {
                let mut entry = vec![range(m.a), range(m.b)];
                if with_match_len {
                    entry.push(Frame::Integer(m.len() as i64));
                }
                Frame::Array(entry)
            })
            .collect();
        Ok(Frame::Map(vec![
            (Frame::bulk("matches"), Frame::Array(matches)),
            (Frame::bulk("len"), Frame::Integer(lcs.len() as i64)),
        ]))
    })())
}

#[cfg(test)]
mod tests {
    use super::compute;
    use crate::testing::{bulk, err, Harness};
    use proptest::prelude::*;
    use rexyra_resp::Frame;

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn pair(a: i64, b: i64) -> Frame {
        Frame::Array(vec![int(a), int(b)])
    }

    fn setup() -> Harness {
        let mut h = Harness::new();
        h.run("MSET key1 ohmytext key2 mynewtext");
        h
    }

    #[test]
    fn string_and_length() {
        let mut h = setup();
        assert_eq!(h.run("LCS key1 key2"), bulk("mytext"));
        assert_eq!(h.run("LCS key1 key2 LEN"), int(6));
        assert_eq!(h.run("LCS key1 missing"), bulk(""));
        assert_eq!(h.run("LCS missing1 missing2 LEN"), int(0));
    }

    #[test]
    fn idx() {
        let mut h = setup();
        assert_eq!(
            h.run("LCS key1 key2 IDX"),
            Frame::Map(vec![
                (
                    bulk("matches"),
                    Frame::Array(vec![
                        Frame::Array(vec![pair(4, 7), pair(5, 8)]),
                        Frame::Array(vec![pair(2, 3), pair(0, 1)]),
                    ])
                ),
                (bulk("len"), int(6)),
            ])
        );
    }

    #[test]
    fn idx_with_filters() {
        let mut h = setup();
        assert_eq!(
            h.run("LCS key1 key2 IDX MINMATCHLEN 4 WITHMATCHLEN"),
            Frame::Map(vec![
                (
                    bulk("matches"),
                    Frame::Array(vec![Frame::Array(vec![pair(4, 7), pair(5, 8), int(4)])])
                ),
                (bulk("len"), int(6),),
            ])
        );
    }

    #[test]
    fn errors() {
        let mut h = setup();
        assert_eq!(
            h.run("LCS key1 key2 LEN IDX"),
            err("ERR If you want both the length and indexes, please just use IDX.")
        );
        assert_eq!(h.run("LCS key1 key2 BOGUS"), err("ERR syntax error"));
        h.run("RPUSH list a");
        assert_eq!(
            h.run("LCS key1 list"),
            err("ERR The specified keys must contain string values")
        );
        assert_eq!(h.run("LCS key1 key2 MINMATCHLEN"), err("ERR syntax error"));
        assert_eq!(
            h.run("LCS key1 key2 MINMATCHLEN x"),
            err("ERR value is not an integer or out of range")
        );
    }

    /// Whether `needle` can be obtained from `haystack` by deleting bytes.
    fn is_subsequence(needle: &[u8], haystack: &[u8]) -> bool {
        let mut rest = haystack.iter();
        needle.iter().all(|byte| rest.any(|other| other == byte))
    }

    /// Exponential reference implementation, for small inputs only.
    fn naive_len(a: &[u8], b: &[u8]) -> usize {
        match (a.split_first(), b.split_first()) {
            (Some((x, a_rest)), Some((y, b_rest))) => {
                if x == y {
                    1 + naive_len(a_rest, b_rest)
                } else {
                    naive_len(a_rest, b).max(naive_len(a, b_rest))
                }
            }
            _ => 0,
        }
    }

    proptest! {
        #[test]
        fn result_is_a_longest_common_subsequence(
            a in prop::collection::vec(0u8..3, 0..10),
            b in prop::collection::vec(0u8..3, 0..10),
        ) {
            let (lcs, matches) = compute(&a, &b);
            prop_assert!(is_subsequence(&lcs, &a));
            prop_assert!(is_subsequence(&lcs, &b));
            prop_assert_eq!(lcs.len(), naive_len(&a, &b));

            // The runs cover exactly the subsequence, point at equal bytes,
            // and move strictly towards the start of both strings.
            prop_assert_eq!(matches.iter().map(|m| m.len()).sum::<usize>(), lcs.len());
            let mut bound = (a.len(), b.len());
            for m in &matches {
                prop_assert_eq!(&a[m.a.0..=m.a.1], &b[m.b.0..=m.b.1]);
                prop_assert!(m.a.1 < bound.0 && m.b.1 < bound.1);
                bound = (m.a.0, m.b.0);
            }
        }
    }
}
