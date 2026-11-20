use bytes::Bytes;

use crate::dict::Dict;
use crate::{random_u64, COMPACT_MAX_VALUE, HASH_COMPACT_MAX_ENTRIES};

/// A hash: field → value.
///
/// Like Redis, small hashes use a compact representation — a flat list that
/// is searched linearly and **keeps insertion order** — and switch to a real
/// hash table once they outgrow it. The switch is one-way.
#[derive(Debug, Clone, PartialEq)]
pub enum Hash {
    /// At most [`HASH_COMPACT_MAX_ENTRIES`] pairs, each side at most
    /// [`COMPACT_MAX_VALUE`] bytes.
    Compact(Vec<(Bytes, Bytes)>),
    Table(Dict<Bytes>),
}

impl Default for Hash {
    fn default() -> Self {
        Hash::Compact(Vec::new())
    }
}

impl Hash {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        match self {
            Hash::Compact(pairs) => pairs.len(),
            Hash::Table(dict) => dict.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the compact representation is in use (`OBJECT ENCODING`).
    pub fn is_compact(&self) -> bool {
        matches!(self, Hash::Compact(_))
    }

    pub fn get(&self, field: &[u8]) -> Option<&Bytes> {
        match self {
            Hash::Compact(pairs) => pairs
                .iter()
                .find(|(name, _)| name[..] == *field)
                .map(|(_, value)| value),
            Hash::Table(dict) => dict.get(field),
        }
    }

    pub fn contains(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    /// Sets `field`, returning `true` if it did not exist before. The field
    /// name is copied into its own allocation when it is new.
    pub fn insert(&mut self, field: &[u8], value: Bytes) -> bool {
        match self {
            Hash::Compact(pairs) => {
                let fits = field.len() <= COMPACT_MAX_VALUE && value.len() <= COMPACT_MAX_VALUE;
                if let Some((_, slot)) = pairs.iter_mut().find(|(name, _)| name[..] == *field) {
                    *slot = value;
                    if !fits {
                        self.upgrade();
                    }
                    return false;
                }
                pairs.push((Bytes::copy_from_slice(field), value));
                if !fits || pairs.len() > HASH_COMPACT_MAX_ENTRIES {
                    self.upgrade();
                }
                true
            }
            Hash::Table(dict) => match dict.get_mut(field) {
                Some(slot) => {
                    *slot = value;
                    false
                }
                None => {
                    dict.insert(Bytes::copy_from_slice(field), value);
                    true
                }
            },
        }
    }

    fn upgrade(&mut self) {
        if let Hash::Compact(pairs) = self {
            let mut dict = Dict::new();
            for (field, value) in pairs.drain(..) {
                dict.insert(field, value);
            }
            *self = Hash::Table(dict);
        }
    }

    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self {
            Hash::Compact(pairs) => match pairs.iter().position(|(name, _)| name[..] == *field) {
                Some(index) => {
                    // Order-preserving removal: later fields keep their place.
                    pairs.remove(index);
                    true
                }
                None => false,
            },
            Hash::Table(dict) => dict.remove(field).is_some(),
        }
    }

    /// Every pair. Compact hashes yield insertion order.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (&Bytes, &Bytes)> + '_> {
        match self {
            Hash::Compact(pairs) => Box::new(pairs.iter().map(|(field, value)| (field, value))),
            Hash::Table(dict) => Box::new(dict.iter()),
        }
    }

    /// Visits the next part of a hash that does not change between calls;
    /// see [`Dict::visit_from`]. A compact hash is visited whole.
    pub fn visit_from(
        &self,
        position: u64,
        buckets: usize,
        mut visit: impl FnMut(&Bytes, &Bytes),
    ) -> Option<u64> {
        match self {
            Hash::Compact(pairs) => {
                for (field, value) in pairs {
                    visit(field, value);
                }
                None
            }
            Hash::Table(dict) => dict.visit_from(position, buckets, visit),
        }
    }

    /// Cursor iteration for `HSCAN`. A compact hash is returned whole in one
    /// call (cursor `0`), as Redis does.
    pub fn scan(&self, cursor: u64, mut visit: impl FnMut(&Bytes, &Bytes)) -> u64 {
        match self {
            Hash::Compact(pairs) => {
                for (field, value) in pairs {
                    visit(field, value);
                }
                0
            }
            Hash::Table(dict) => dict.scan(cursor, visit),
        }
    }

    /// A pseudo-randomly chosen pair.
    pub fn random(&self) -> Option<(&Bytes, &Bytes)> {
        match self {
            Hash::Compact(pairs) if pairs.is_empty() => None,
            Hash::Compact(pairs) => {
                let (field, value) = &pairs[random_u64() as usize % pairs.len()];
                Some((field, value))
            }
            Hash::Table(dict) => dict.random_entry(random_u64(), random_u64()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(text: &str) -> Bytes {
        Bytes::copy_from_slice(text.as_bytes())
    }

    fn fields(hash: &Hash) -> Vec<String> {
        hash.iter()
            .map(|(field, _)| String::from_utf8(field.to_vec()).unwrap())
            .collect()
    }

    #[test]
    fn insert_get_remove() {
        let mut h = Hash::new();
        assert!(h.insert(b"a", b("1")));
        assert!(!h.insert(b"a", b("2")));
        assert_eq!(h.get(b"a"), Some(&b("2")));
        assert!(h.contains(b"a"));
        assert_eq!(h.len(), 1);
        assert!(h.remove(b"a"));
        assert!(!h.remove(b"a"));
        assert!(h.is_empty());
    }

    #[test]
    fn compact_keeps_insertion_order_across_updates_and_removals() {
        let mut h = Hash::new();
        for field in ["c", "a", "d", "b"] {
            h.insert(field.as_bytes(), b("v"));
        }
        h.insert(b"a", b("updated"));
        assert_eq!(fields(&h), ["c", "a", "d", "b"]);
        h.remove(b"a");
        assert_eq!(fields(&h), ["c", "d", "b"]);
        assert!(h.is_compact());
    }

    #[test]
    fn upgrades_when_too_many_entries() {
        let mut h = Hash::new();
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            h.insert(format!("f{i}").as_bytes(), b("v"));
        }
        assert!(h.is_compact());
        h.insert(b"one-more", b("v"));
        assert!(!h.is_compact());
        assert_eq!(h.len(), HASH_COMPACT_MAX_ENTRIES + 1);
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            assert_eq!(h.get(format!("f{i}").as_bytes()), Some(&b("v")));
        }
        // The upgrade is one-way.
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            h.remove(format!("f{i}").as_bytes());
        }
        assert!(!h.is_compact());
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn upgrades_when_a_field_or_value_is_too_long() {
        let long = "x".repeat(COMPACT_MAX_VALUE + 1);
        let edge = "x".repeat(COMPACT_MAX_VALUE);

        let mut h = Hash::new();
        h.insert(b"f", b(&edge));
        assert!(h.is_compact());
        h.insert(b"f", b(&long));
        assert!(!h.is_compact(), "overwriting with a long value upgrades");
        assert_eq!(h.get(b"f"), Some(&b(&long)));

        let mut h = Hash::new();
        h.insert(long.as_bytes(), b("v"));
        assert!(!h.is_compact(), "a long field name upgrades");
    }

    #[test]
    fn scan_and_random() {
        let mut h = Hash::new();
        assert!(h.random().is_none());
        for i in 0..300 {
            h.insert(format!("f{i}").as_bytes(), b(&i.to_string()));
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0;
        loop {
            cursor = h.scan(cursor, |field, _| {
                seen.insert(field.clone());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 300);
        let (field, value) = h.random().unwrap();
        assert_eq!(h.get(field), Some(value));
    }
}
