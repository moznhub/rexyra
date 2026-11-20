use bytes::Bytes;

use crate::dict::Dict;
use crate::{random_u64, COMPACT_MAX_ENTRIES, COMPACT_MAX_VALUE};

/// Largest set kept as a sorted array of integers.
pub const INTSET_MAX_ENTRIES: usize = 512;

/// A set of byte strings.
///
/// Mirrors Redis's three encodings, because they are observable: iteration
/// order (`SMEMBERS`) differs between them, and `OBJECT ENCODING` names them.
///
/// * [`Set::Ints`] — every member is a canonical integer; kept sorted.
/// * [`Set::Compact`] — a small flat list in insertion order.
/// * [`Set::Table`] — a hash table, for everything else.
///
/// Conversions only ever go towards `Table`.
#[derive(Debug, Clone, PartialEq)]
pub enum Set {
    Ints(Vec<i64>),
    Compact(Vec<Bytes>),
    Table(Dict<()>),
}

impl Default for Set {
    fn default() -> Self {
        Set::Ints(Vec::new())
    }
}

/// Parses a member as an integer only if it is in canonical form, so that
/// converting back to text reproduces the member exactly (`"7"` yes; `"07"`,
/// `"+7"`, `"-0"` no).
fn as_int(member: &[u8]) -> Option<i64> {
    let digits = member.strip_prefix(b"-").unwrap_or(member);
    let canonical = match digits {
        [] => false,
        [b'0'] => digits.len() == member.len(),
        [b'0', ..] => false,
        _ => digits.len() <= 19 && digits.iter().all(u8::is_ascii_digit),
    };
    if !canonical {
        return None;
    }
    std::str::from_utf8(member).ok()?.parse().ok()
}

fn int_bytes(value: i64) -> Bytes {
    Bytes::from(value.to_string())
}

impl Set {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        match self {
            Set::Ints(ints) => ints.len(),
            Set::Compact(members) => members.len(),
            Set::Table(dict) => dict.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The name `OBJECT ENCODING` reports.
    pub fn encoding(&self) -> &'static str {
        match self {
            Set::Ints(_) => "intset",
            Set::Compact(_) => "listpack",
            Set::Table(_) => "hashtable",
        }
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            Set::Ints(ints) => as_int(member).is_some_and(|n| ints.binary_search(&n).is_ok()),
            Set::Compact(members) => members.iter().any(|m| m[..] == *member),
            Set::Table(dict) => dict.contains(member),
        }
    }

    /// Adds `member` (copied into its own allocation), returning `true` if it
    /// was not already present.
    pub fn insert(&mut self, member: &[u8]) -> bool {
        match self {
            Set::Ints(ints) => match as_int(member) {
                Some(n) => match ints.binary_search(&n) {
                    Ok(_) => false,
                    Err(index) => {
                        ints.insert(index, n);
                        if ints.len() > INTSET_MAX_ENTRIES {
                            self.upgrade_to_table();
                        }
                        true
                    }
                },
                None => {
                    // A non-integer member ends the integer encoding.
                    let small =
                        ints.len() < COMPACT_MAX_ENTRIES && member.len() <= COMPACT_MAX_VALUE;
                    let members: Vec<Bytes> = ints.iter().copied().map(int_bytes).collect();
                    *self = Set::Compact(members);
                    if !small {
                        self.upgrade_to_table();
                    }
                    self.insert(member)
                }
            },
            Set::Compact(members) => {
                if members.iter().any(|m| m[..] == *member) {
                    return false;
                }
                members.push(Bytes::copy_from_slice(member));
                if members.len() > COMPACT_MAX_ENTRIES || member.len() > COMPACT_MAX_VALUE {
                    self.upgrade_to_table();
                }
                true
            }
            Set::Table(dict) => {
                if dict.contains(member) {
                    return false;
                }
                dict.insert(Bytes::copy_from_slice(member), ());
                true
            }
        }
    }

    fn upgrade_to_table(&mut self) {
        let members: Vec<Bytes> = match self {
            Set::Ints(ints) => ints.iter().copied().map(int_bytes).collect(),
            Set::Compact(members) => std::mem::take(members),
            Set::Table(_) => return,
        };
        let mut dict = Dict::new();
        for member in members {
            dict.insert(member, ());
        }
        *self = Set::Table(dict);
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            Set::Ints(ints) => match as_int(member).map(|n| ints.binary_search(&n)) {
                Some(Ok(index)) => {
                    ints.remove(index);
                    true
                }
                _ => false,
            },
            Set::Compact(members) => match members.iter().position(|m| m[..] == *member) {
                Some(index) => {
                    members.remove(index);
                    true
                }
                None => false,
            },
            Set::Table(dict) => dict.remove(member).is_some(),
        }
    }

    /// Every member: ascending for integer sets, insertion order for compact
    /// sets, unspecified otherwise.
    pub fn iter(&self) -> Box<dyn Iterator<Item = Bytes> + '_> {
        match self {
            Set::Ints(ints) => Box::new(ints.iter().copied().map(int_bytes)),
            Set::Compact(members) => Box::new(members.iter().cloned()),
            Set::Table(dict) => Box::new(dict.iter().map(|(member, _)| member.clone())),
        }
    }

    /// Calls `visit` with every member, without making the set share its
    /// members with anyone (which [`Set::iter`], handing out reference-
    /// counted copies, does).
    pub fn for_each(&self, mut visit: impl FnMut(&[u8])) {
        match self {
            Set::Ints(ints) => ints.iter().for_each(|&int| visit(&int_bytes(int))),
            Set::Compact(members) => members.iter().for_each(|member| visit(member)),
            Set::Table(dict) => dict.iter().for_each(|(member, _)| visit(member)),
        }
    }

    /// Visits the next part of a set that does not change between calls,
    /// lending each member; see [`Dict::visit_from`]. Integer and compact
    /// sets are visited whole.
    pub fn visit_from(
        &self,
        position: u64,
        buckets: usize,
        mut visit: impl FnMut(&[u8]),
    ) -> Option<u64> {
        match self {
            Set::Table(dict) => dict.visit_from(position, buckets, |member, _| visit(member)),
            _ => {
                self.for_each(visit);
                None
            }
        }
    }

    /// Cursor iteration for `SSCAN`. Integer and compact sets are returned
    /// whole in one call (cursor `0`), as Redis does.
    pub fn scan(&self, cursor: u64, mut visit: impl FnMut(Bytes)) -> u64 {
        match self {
            Set::Table(dict) => dict.scan(cursor, |member, _| visit(member.clone())),
            _ => {
                self.iter().for_each(visit);
                0
            }
        }
    }

    /// A pseudo-randomly chosen member.
    pub fn random(&self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let pick = random_u64() as usize % self.len();
        match self {
            Set::Ints(ints) => Some(int_bytes(ints[pick])),
            Set::Compact(members) => Some(members[pick].clone()),
            Set::Table(dict) => dict
                .random_entry(random_u64(), random_u64())
                .map(|(member, _)| member.clone()),
        }
    }
}

impl<'a> FromIterator<&'a [u8]> for Set {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(members: I) -> Self {
        let mut set = Set::new();
        for member in members {
            set.insert(member);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn members(set: &Set) -> Vec<String> {
        set.iter()
            .map(|m| String::from_utf8(m.to_vec()).unwrap())
            .collect()
    }

    fn of(items: &[&str]) -> Set {
        items.iter().map(|m| m.as_bytes()).collect()
    }

    #[test]
    fn canonical_integers_only() {
        for yes in [
            "0",
            "7",
            "-7",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            assert!(as_int(yes.as_bytes()).is_some(), "{yes}");
        }
        for no in [
            "",
            "-",
            "07",
            "+7",
            "-0",
            "00",
            "1.0",
            " 1",
            "a",
            "9223372036854775808",
        ] {
            assert!(as_int(no.as_bytes()).is_none(), "{no}");
        }
    }

    #[test]
    fn integer_sets_are_sorted_numerically() {
        let set = of(&["10", "2", "-5", "33", "2"]);
        assert_eq!(set.encoding(), "intset");
        assert_eq!(members(&set), ["-5", "2", "10", "33"]);
        assert!(set.contains(b"10"));
        assert!(!set.contains(b"010"));
        assert!(!set.contains(b"x"));
    }

    #[test]
    fn a_string_member_converts_to_compact_keeping_integers_first() {
        let mut set = of(&["3", "1", "2"]);
        assert!(set.insert(b"hello"));
        assert_eq!(set.encoding(), "listpack");
        assert_eq!(members(&set), ["1", "2", "3", "hello"]);
        assert!(set.contains(b"2"));
        assert!(!set.insert(b"2"));
        assert!(
            set.insert(b"07"),
            "non-canonical integers are plain strings"
        );
    }

    #[test]
    fn compact_sets_keep_insertion_order() {
        let mut set = of(&["c", "a", "b"]);
        assert_eq!(members(&set), ["c", "a", "b"]);
        assert!(set.remove(b"a"));
        assert!(!set.remove(b"a"));
        assert_eq!(members(&set), ["c", "b"]);
    }

    #[test]
    fn upgrades_to_table() {
        let mut ints = Set::new();
        for i in 0..=INTSET_MAX_ENTRIES {
            ints.insert(i.to_string().as_bytes());
        }
        assert_eq!(ints.encoding(), "hashtable");
        assert_eq!(ints.len(), INTSET_MAX_ENTRIES + 1);
        assert!(ints.contains(b"300"));

        let mut strings = Set::new();
        for i in 0..=COMPACT_MAX_ENTRIES {
            strings.insert(format!("m{i}").as_bytes());
        }
        assert_eq!(strings.encoding(), "hashtable");

        let mut long = of(&["a"]);
        long.insert("x".repeat(COMPACT_MAX_VALUE + 1).as_bytes());
        assert_eq!(long.encoding(), "hashtable");

        // Many integers plus one string skips the compact form entirely.
        let mut mixed = Set::new();
        for i in 0..COMPACT_MAX_ENTRIES {
            mixed.insert(i.to_string().as_bytes());
        }
        mixed.insert(b"s");
        assert_eq!(mixed.encoding(), "hashtable");
        assert_eq!(mixed.len(), COMPACT_MAX_ENTRIES + 1);
    }

    #[test]
    fn random_returns_members() {
        assert_eq!(Set::new().random(), None);
        for set in [of(&["1", "2", "3"]), of(&["a", "b"])] {
            for _ in 0..50 {
                assert!(set.contains(&set.random().unwrap()));
            }
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(String),
        Remove(String),
    }

    fn member() -> impl Strategy<Value = String> {
        prop_oneof![
            (-20i64..700).prop_map(|n| n.to_string()),
            "[a-c]{1,2}",
            Just("x".repeat(COMPACT_MAX_VALUE + 1)),
            Just("007".to_owned()),
        ]
    }

    proptest! {
        /// Whatever encoding it ends up in, the set holds exactly the members
        /// a plain set would, and a full scan reports each exactly once.
        #[test]
        fn behaves_like_a_set(ops in prop::collection::vec(
            prop_oneof![3 => member().prop_map(Op::Insert), 1 => member().prop_map(Op::Remove)],
            0..900,
        )) {
            let mut set = Set::new();
            let mut model = BTreeSet::new();
            for op in ops {
                match op {
                    Op::Insert(m) => prop_assert_eq!(set.insert(m.as_bytes()), model.insert(m)),
                    Op::Remove(m) => prop_assert_eq!(set.remove(m.as_bytes()), model.remove(&m)),
                }
                prop_assert_eq!(set.len(), model.len());
            }
            let mut scanned = Vec::new();
            let mut cursor = 0;
            loop {
                cursor = set.scan(cursor, |m| scanned.push(String::from_utf8(m.to_vec()).unwrap()));
                if cursor == 0 {
                    break;
                }
            }
            let unique: BTreeSet<String> = scanned.iter().cloned().collect();
            if !matches!(set, Set::Table(_)) {
                prop_assert_eq!(scanned.len(), unique.len());
            }
            prop_assert_eq!(&unique, &model);
            for m in &model {
                prop_assert!(set.contains(m.as_bytes()));
            }
        }
    }
}
