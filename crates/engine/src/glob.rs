/// Redis glob matching, as used by `KEYS` and `SCAN MATCH`.
///
/// * `*` matches any sequence of bytes, including none
/// * `?` matches exactly one byte
/// * `[abc]`, `[a-z]`, `[^x]` match one byte from (or not from) a class
/// * `\x` matches `x` literally
///
/// Runs in O(pattern × input) time with no recursion, so a hostile pattern
/// such as `a*a*a*a*b` cannot stall the server.
pub fn glob_match(pattern: &[u8], input: &[u8]) -> bool {
    let (mut p, mut s) = (0, 0);
    // Where to resume after the most recent `*`: (pattern index after the
    // star, input index the star has consumed up to).
    let mut star: Option<(usize, usize)> = None;

    loop {
        if pattern.get(p) == Some(&b'*') {
            while pattern.get(p) == Some(&b'*') {
                p += 1;
            }
            star = Some((p, s));
            continue;
        }
        let Some(&byte) = input.get(s) else {
            // Input exhausted: only a fully consumed pattern matches.
            return p == pattern.len();
        };
        if let Some(next) = match_one(pattern, p, byte) {
            p = next;
            s += 1;
        } else if let Some((after_star, consumed)) = star {
            // Let the last star swallow one more byte and retry.
            p = after_star;
            s = consumed + 1;
            star = Some((after_star, consumed + 1));
        } else {
            return false;
        }
    }
}

/// Matches the single-byte pattern element at `p` against `byte`, returning
/// the index of the next pattern element on success.
fn match_one(pattern: &[u8], p: usize, byte: u8) -> Option<usize> {
    match *pattern.get(p)? {
        b'?' => Some(p + 1),
        b'[' => {
            let mut i = p + 1;
            let negate = pattern.get(i) == Some(&b'^');
            if negate {
                i += 1;
            }
            let mut matched = false;
            loop {
                match pattern.get(i) {
                    // Unterminated class: treat what we have as the class.
                    None => break,
                    Some(b']') => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') if i + 1 < pattern.len() => {
                        matched |= pattern[i + 1] == byte;
                        i += 2;
                    }
                    Some(&lo) if i + 2 < pattern.len() && pattern[i + 1] == b'-' => {
                        let hi = pattern[i + 2];
                        let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                        matched |= (lo..=hi).contains(&byte);
                        i += 3;
                    }
                    Some(&c) => {
                        matched |= c == byte;
                        i += 1;
                    }
                }
            }
            (matched != negate).then_some(i)
        }
        b'\\' if p + 1 < pattern.len() => (pattern[p + 1] == byte).then_some(p + 2),
        c => (c == byte).then_some(p + 1),
    }
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    fn m(pattern: &str, input: &str) -> bool {
        glob_match(pattern.as_bytes(), input.as_bytes())
    }

    #[test]
    fn literals() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hellO"));
        assert!(!m("hello", "hell"));
        assert!(!m("hell", "hello"));
        assert!(m("", ""));
        assert!(!m("", "a"));
    }

    #[test]
    fn star() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("h*o", "ho"));
        assert!(m("h*o", "hello"));
        assert!(!m("h*o", "hell"));
        assert!(m("user:*:name", "user:42:name"));
        assert!(m("*a*b*", "xxaxxbxx"));
        assert!(!m("*a*b*", "xxbxxaxx"));
        assert!(m("a**b", "ab"));
        assert!(m("*abc", "ababc"));
    }

    #[test]
    fn question_mark() {
        assert!(m("h?llo", "hello"));
        assert!(m("h?llo", "hallo"));
        assert!(!m("h?llo", "hllo"));
        assert!(!m("?", ""));
    }

    #[test]
    fn classes() {
        assert!(m("h[ae]llo", "hello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-c]llo", "hbllo"));
        assert!(!m("h[a-c]llo", "hdllo"));
        assert!(m("h[c-a]llo", "hbllo"));
        assert!(m("[a-z][0-9]", "x7"));
        assert!(m("h[\\]]llo", "h]llo"));
        // An unterminated class still matches one byte from what was given.
        assert!(m("key[ab", "keya"));
        assert!(!m("key[ab", "keyc"));
        assert!(!m("key[", "key"));
    }

    #[test]
    fn escapes() {
        assert!(m("h\\*llo", "h*llo"));
        assert!(!m("h\\*llo", "hello"));
        assert!(m("h\\?llo", "h?llo"));
        assert!(m("a\\", "a\\"));
    }

    #[test]
    fn star_backtracks_over_classes() {
        assert!(m("*[0-9]", "abc5"));
        assert!(!m("*[0-9]", "abc"));
        assert!(m("*?", "a"));
        assert!(!m("*?", ""));
    }

    #[test]
    fn pathological_pattern_is_fast() {
        let input = "a".repeat(5_000);
        let pattern = format!("{}b", "a*".repeat(50));
        assert!(!m(&pattern, &input));
    }

    #[test]
    fn binary_safe() {
        assert!(glob_match(b"\x00*\xff", b"\x00\x01\x02\xff"));
    }
}
