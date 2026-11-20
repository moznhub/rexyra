use std::cmp::Ordering;
use std::ops::Range;

use bytes::Bytes;

use crate::dict::Dict;
use crate::{random_u64, COMPACT_MAX_ENTRIES, COMPACT_MAX_VALUE};

/// A sorted set: unique members, each with a score, ordered by
/// `(score, member)`.
///
/// Two structures are kept in step, as in Redis:
///
/// * a hash table `member → score`, for O(1) score lookups;
/// * a skip list ordered by `(score, member)`, whose links record how many
///   elements they skip, so that "what is the rank of X" and "give me the
///   element at rank N" are both O(log n).
///
/// Every range query — by rank, by score or by member — is first turned into
/// a range of ranks, then read from the skip list.
#[derive(Clone)]
pub struct ZSet {
    scores: Dict<f64>,
    list: SkipList,
    /// Whether Redis would still be using its compact encoding for this set
    /// (`OBJECT ENCODING`). Ordering is identical either way, so only the
    /// reported name depends on it.
    compact: bool,
}

/// One end of a score range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

/// One end of a lexicographic range (`ZRANGEBYLEX`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// `-`: before every member.
    Min,
    /// `+`: after every member.
    Max,
    Inclusive(Bytes),
    Exclusive(Bytes),
}

impl Default for ZSet {
    fn default() -> Self {
        Self {
            scores: Dict::new(),
            list: SkipList::new(),
            compact: true,
        }
    }
}

impl std::fmt::Debug for ZSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ZSet {
    fn eq(&self, other: &Self) -> bool {
        self.scores == other.scores
    }
}

impl ZSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn is_compact(&self) -> bool {
        self.compact
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// Adds `member` or changes its score. Returns the previous score if the
    /// member already existed. `score` must not be NaN.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        debug_assert!(!score.is_nan());
        match self.scores.find(member) {
            Some(pos) => {
                let (key, stored) = self.scores.at_mut(pos);
                let previous = *stored;
                if previous != score {
                    *stored = score;
                    let key = key.clone();
                    self.list.remove(previous, &key);
                    self.list.insert(score, key);
                }
                Some(previous)
            }
            None => {
                let key = Bytes::copy_from_slice(member);
                self.scores.insert(key.clone(), score);
                self.list.insert(score, key);
                if self.len() > COMPACT_MAX_ENTRIES || member.len() > COMPACT_MAX_VALUE {
                    self.compact = false;
                }
                None
            }
        }
    }

    /// Removes `member`, returning its score if it was present.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let (key, score) = self.scores.remove(member)?;
        self.list.remove(score, &key);
        Some(score)
    }

    /// Zero-based position of `member` in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.score(member)?;
        Some(
            self.list
                .count_while(|s, m| compare(s, m, score, member) == Ordering::Less),
        )
    }

    /// The ranks of the members whose score lies between `min` and `max`.
    pub fn score_range(&self, min: ScoreBound, max: ScoreBound) -> Range<usize> {
        let start = self.list.count_while(|score, _| {
            if min.exclusive {
                score <= min.value
            } else {
                score < min.value
            }
        });
        let end = self.list.count_while(|score, _| {
            if max.exclusive {
                score < max.value
            } else {
                score <= max.value
            }
        });
        start..end.max(start)
    }

    /// The ranks of the members between `min` and `max` in byte order. Only
    /// meaningful when every member has the same score, as with Redis.
    pub fn lex_range(&self, min: &LexBound, max: &LexBound) -> Range<usize> {
        let start = match min {
            LexBound::Min => 0,
            LexBound::Max => self.len(),
            LexBound::Inclusive(bound) => self.list.count_while(|_, m| m[..] < bound[..]),
            LexBound::Exclusive(bound) => self.list.count_while(|_, m| m[..] <= bound[..]),
        };
        let end = match max {
            LexBound::Min => 0,
            LexBound::Max => self.len(),
            LexBound::Inclusive(bound) => self.list.count_while(|_, m| m[..] <= bound[..]),
            LexBound::Exclusive(bound) => self.list.count_while(|_, m| m[..] < bound[..]),
        };
        start..end.max(start)
    }

    /// The members at `ranks`, ascending, or descending when `reverse`.
    pub fn range(&self, ranks: Range<usize>, reverse: bool) -> Iter<'_> {
        let ranks = ranks.start.min(self.len())..ranks.end.min(self.len());
        let remaining = ranks.end.saturating_sub(ranks.start);
        let node = if remaining == 0 {
            NIL
        } else if reverse {
            self.list.node_at(ranks.end - 1)
        } else {
            self.list.node_at(ranks.start)
        };
        Iter {
            list: &self.list,
            node,
            remaining,
            reverse,
        }
    }

    /// Every member in ascending order.
    pub fn iter(&self) -> Iter<'_> {
        self.range(0..self.len(), false)
    }

    /// Cursor iteration for `ZSCAN`. A compact set is returned whole, in
    /// order, in one call (cursor `0`), as Redis does.
    pub fn scan(&self, cursor: u64, mut visit: impl FnMut(&Bytes, f64)) -> u64 {
        if self.compact {
            for (member, score) in self.iter() {
                visit(member, score);
            }
            0
        } else {
            self.scores
                .scan(cursor, |member, score| visit(member, *score))
        }
    }

    /// A pseudo-randomly chosen member.
    pub fn random(&self) -> Option<(&Bytes, f64)> {
        if self.is_empty() {
            return None;
        }
        let rank = random_u64() as usize % self.len();
        self.range(rank..rank + 1, false).next()
    }
}

/// Ascending or descending walk over part of a [`ZSet`].
pub struct Iter<'a> {
    list: &'a SkipList,
    node: u32,
    remaining: usize,
    reverse: bool,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Bytes, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.node == NIL {
            return None;
        }
        let node = &self.list.nodes[self.node as usize];
        self.remaining -= 1;
        self.node = if self.reverse {
            node.prev
        } else {
            node.links[0].next
        };
        Some((&node.member, node.score))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for Iter<'_> {}

/// Orders by score, then by member bytes. Scores are never NaN.
fn compare(score: f64, member: &[u8], other_score: f64, other_member: &[u8]) -> Ordering {
    score
        .partial_cmp(&other_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| member.cmp(other_member))
}

// ---------------------------------------------------------------------------
// Skip list
// ---------------------------------------------------------------------------

const MAX_LEVEL: usize = 32;
/// "No node": the end of a chain.
const NIL: u32 = u32::MAX;
/// The sentinel before the first element. It has every level and no data.
const HEAD: u32 = 0;

#[derive(Clone, Copy)]
struct Link {
    next: u32,
    /// How many elements this link advances by. For a link that ends the
    /// chain, how many elements come after its owner.
    span: u32,
}

#[derive(Clone)]
struct Node {
    member: Bytes,
    score: f64,
    /// One link per level this node takes part in; `links[0]` is the plain
    /// linked list of every element.
    links: Vec<Link>,
    /// Previous element at level 0 (`NIL` for the first), for reverse walks.
    prev: u32,
}

/// A skip list stored in a vector, with indexes instead of pointers so it
/// needs no `unsafe`. Removed nodes are recycled through a free list.
#[derive(Clone)]
struct SkipList {
    nodes: Vec<Node>,
    free: Vec<u32>,
    /// Number of levels currently in use (at least 1).
    level: usize,
    len: usize,
}

impl SkipList {
    fn new() -> Self {
        let head = Node {
            member: Bytes::new(),
            score: 0.0,
            links: vec![Link { next: NIL, span: 0 }; MAX_LEVEL],
            prev: NIL,
        };
        Self {
            nodes: vec![head],
            free: Vec::new(),
            level: 1,
            len: 0,
        }
    }

    fn link(&self, node: u32, level: usize) -> Link {
        self.nodes[node as usize].links[level]
    }

    fn link_mut(&mut self, node: u32, level: usize) -> &mut Link {
        &mut self.nodes[node as usize].links[level]
    }

    /// Geometric distribution: each extra level has probability 1/4.
    fn random_level() -> usize {
        let mut bits = random_u64();
        let mut level = 1;
        while level < MAX_LEVEL && bits & 3 == 0 {
            level += 1;
            bits >>= 2;
        }
        level
    }

    /// For each level, the last node ordered before `(score, member)`, and
    /// how many elements precede-or-equal that node.
    fn search(&self, score: f64, member: &[u8]) -> ([u32; MAX_LEVEL], [u32; MAX_LEVEL]) {
        let mut update = [HEAD; MAX_LEVEL];
        let mut rank = [0u32; MAX_LEVEL];
        let mut node = HEAD;
        for level in (0..self.level).rev() {
            rank[level] = if level + 1 == self.level {
                0
            } else {
                rank[level + 1]
            };
            loop {
                let link = self.link(node, level);
                if link.next == NIL {
                    break;
                }
                let next = &self.nodes[link.next as usize];
                if compare(next.score, &next.member, score, member) != Ordering::Less {
                    break;
                }
                rank[level] += link.span;
                node = link.next;
            }
            update[level] = node;
        }
        (update, rank)
    }

    /// Inserts an element. The caller guarantees it is not already present.
    // `update` and `rank` are parallel per-level arrays; indexing them by
    // level reads more clearly than zipped iterators.
    #[allow(clippy::needless_range_loop)]
    fn insert(&mut self, score: f64, member: Bytes) {
        let (mut update, mut rank) = self.search(score, &member);

        let level = Self::random_level();
        if level > self.level {
            // New top levels start as one link from the head over everything.
            for new_level in self.level..level {
                rank[new_level] = 0;
                update[new_level] = HEAD;
                self.link_mut(HEAD, new_level).span = self.len as u32;
            }
            self.level = level;
        }

        let node = Node {
            member,
            score,
            links: vec![Link { next: NIL, span: 0 }; level],
            prev: if update[0] == HEAD { NIL } else { update[0] },
        };
        let index = match self.free.pop() {
            Some(index) => {
                self.nodes[index as usize] = node;
                index
            }
            None => {
                self.nodes.push(node);
                (self.nodes.len() - 1) as u32
            }
        };

        for current in 0..level {
            let before = self.link(update[current], current);
            // Elements between this level's predecessor and the new node.
            let skipped = rank[0] - rank[current];
            *self.link_mut(index, current) = Link {
                next: before.next,
                span: before.span - skipped,
            };
            *self.link_mut(update[current], current) = Link {
                next: index,
                span: skipped + 1,
            };
        }
        // Higher levels pass over the new node: their links got longer.
        for current in level..self.level {
            self.link_mut(update[current], current).span += 1;
        }

        let next = self.link(index, 0).next;
        if next != NIL {
            self.nodes[next as usize].prev = index;
        }
        self.len += 1;
    }

    /// Removes an element. Returns whether it was present.
    #[allow(clippy::needless_range_loop)]
    fn remove(&mut self, score: f64, member: &[u8]) -> bool {
        let (update, _) = self.search(score, member);
        let target = self.link(update[0], 0).next;
        if target == NIL {
            return false;
        }
        let found = &self.nodes[target as usize];
        if compare(found.score, &found.member, score, member) != Ordering::Equal {
            return false;
        }

        for level in 0..self.level {
            let before = self.link(update[level], level);
            if before.next == target {
                let removed = self.link(target, level);
                *self.link_mut(update[level], level) = Link {
                    next: removed.next,
                    span: before.span + removed.span - 1,
                };
            } else {
                self.link_mut(update[level], level).span -= 1;
            }
        }
        let (next, prev) = (self.link(target, 0).next, self.nodes[target as usize].prev);
        if next != NIL {
            self.nodes[next as usize].prev = prev;
        }
        while self.level > 1 && self.link(HEAD, self.level - 1).next == NIL {
            self.level -= 1;
        }

        // Release what the node owns now; the slot itself is reused later.
        let slot = &mut self.nodes[target as usize];
        slot.member = Bytes::new();
        slot.links = Vec::new();
        self.free.push(target);
        self.len -= 1;
        true
    }

    /// Number of leading elements for which `before` holds. `before` must be
    /// true for a prefix of the list and false for the rest.
    fn count_while(&self, before: impl Fn(f64, &Bytes) -> bool) -> usize {
        let mut node = HEAD;
        let mut count = 0usize;
        for level in (0..self.level).rev() {
            loop {
                let link = self.link(node, level);
                if link.next == NIL {
                    break;
                }
                let next = &self.nodes[link.next as usize];
                if !before(next.score, &next.member) {
                    break;
                }
                count += link.span as usize;
                node = link.next;
            }
        }
        count
    }

    /// The node at a zero-based rank, or `NIL` if out of range.
    fn node_at(&self, rank: usize) -> u32 {
        let target = rank + 1; // the head is position 0
        let mut node = HEAD;
        let mut position = 0usize;
        for level in (0..self.level).rev() {
            loop {
                let link = self.link(node, level);
                if link.next == NIL || position + link.span as usize > target {
                    break;
                }
                position += link.span as usize;
                node = link.next;
            }
            if position == target {
                return node;
            }
        }
        NIL
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn zset(items: &[(&str, f64)]) -> ZSet {
        let mut z = ZSet::new();
        for (member, score) in items {
            z.insert(member.as_bytes(), *score);
        }
        z
    }

    fn members(iter: Iter<'_>) -> Vec<String> {
        iter.map(|(m, _)| String::from_utf8(m.to_vec()).unwrap())
            .collect()
    }

    fn incl(value: f64) -> ScoreBound {
        ScoreBound {
            value,
            exclusive: false,
        }
    }

    fn excl(value: f64) -> ScoreBound {
        ScoreBound {
            value,
            exclusive: true,
        }
    }

    #[test]
    fn orders_by_score_then_member() {
        let z = zset(&[("c", 1.0), ("a", 2.0), ("b", 1.0), ("d", -5.0)]);
        assert_eq!(members(z.iter()), ["d", "b", "c", "a"]);
        assert_eq!(z.len(), 4);
        assert_eq!(z.score(b"a"), Some(2.0));
        assert_eq!(z.score(b"zz"), None);
        assert_eq!(z.rank(b"d"), Some(0));
        assert_eq!(z.rank(b"c"), Some(2));
        assert_eq!(z.rank(b"zz"), None);
    }

    #[test]
    fn insert_updates_score_and_position() {
        let mut z = zset(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        assert_eq!(z.insert(b"a", 10.0), Some(1.0));
        assert_eq!(members(z.iter()), ["b", "c", "a"]);
        assert_eq!(z.insert(b"a", 10.0), Some(10.0), "same score is a no-op");
        assert_eq!(z.insert(b"new", 0.0), None);
        assert_eq!(z.len(), 4);
        assert_eq!(z.rank(b"new"), Some(0));
    }

    #[test]
    fn remove() {
        let mut z = zset(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        assert_eq!(z.remove(b"b"), Some(2.0));
        assert_eq!(z.remove(b"b"), None);
        assert_eq!(members(z.iter()), ["a", "c"]);
        assert_eq!(z.rank(b"c"), Some(1));
        z.remove(b"a");
        z.remove(b"c");
        assert!(z.is_empty());
        assert_eq!(members(z.iter()), Vec::<String>::new());
        // Slots are recycled.
        z.insert(b"x", 1.0);
        assert_eq!(members(z.iter()), ["x"]);
    }

    #[test]
    fn rank_ranges_forward_and_reverse() {
        let z = zset(&[("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);
        assert_eq!(members(z.range(1..3, false)), ["b", "c"]);
        assert_eq!(members(z.range(1..3, true)), ["c", "b"]);
        assert_eq!(members(z.range(0..100, true)), ["d", "c", "b", "a"]);
        assert_eq!(members(z.range(2..2, false)), Vec::<String>::new());
        assert_eq!(members(z.range(9..12, false)), Vec::<String>::new());
        assert_eq!(z.range(0..4, false).len(), 4);
    }

    #[test]
    fn score_ranges() {
        let z = zset(&[("a", 1.0), ("b", 2.0), ("c", 2.0), ("d", 3.0)]);
        assert_eq!(z.score_range(incl(2.0), incl(3.0)), 1..4);
        assert_eq!(z.score_range(excl(2.0), incl(3.0)), 3..4);
        assert_eq!(z.score_range(incl(2.0), excl(3.0)), 1..3);
        assert_eq!(z.score_range(excl(1.0), excl(3.0)), 1..3);
        assert_eq!(
            z.score_range(incl(f64::NEG_INFINITY), incl(f64::INFINITY)),
            0..4
        );
        assert!(z.score_range(incl(5.0), incl(9.0)).is_empty());
        assert!(z.score_range(incl(3.0), incl(1.0)).is_empty());
        assert!(z.score_range(excl(2.0), excl(2.0)).is_empty());
    }

    #[test]
    fn infinite_scores_sort_at_the_ends() {
        let z = zset(&[
            ("mid", 0.0),
            ("top", f64::INFINITY),
            ("bottom", f64::NEG_INFINITY),
        ]);
        assert_eq!(members(z.iter()), ["bottom", "mid", "top"]);
        assert_eq!(
            z.score_range(incl(f64::INFINITY), incl(f64::INFINITY)),
            2..3
        );
    }

    #[test]
    fn lex_ranges() {
        let z = zset(&[("a", 0.0), ("b", 0.0), ("c", 0.0), ("d", 0.0)]);
        let b = |s: &str| Bytes::copy_from_slice(s.as_bytes());
        assert_eq!(z.lex_range(&LexBound::Min, &LexBound::Max), 0..4);
        assert_eq!(
            z.lex_range(&LexBound::Inclusive(b("b")), &LexBound::Inclusive(b("c"))),
            1..3
        );
        assert_eq!(
            z.lex_range(&LexBound::Exclusive(b("b")), &LexBound::Max),
            2..4
        );
        assert_eq!(
            z.lex_range(&LexBound::Min, &LexBound::Exclusive(b("c"))),
            0..2
        );
        assert_eq!(
            z.lex_range(&LexBound::Inclusive(b("bb")), &LexBound::Inclusive(b("cc"))),
            2..3
        );
        assert!(z.lex_range(&LexBound::Max, &LexBound::Min).is_empty());
    }

    #[test]
    fn compact_flag_follows_redis_thresholds() {
        let mut z = ZSet::new();
        for i in 0..COMPACT_MAX_ENTRIES {
            z.insert(format!("m{i}").as_bytes(), i as f64);
        }
        assert!(z.is_compact());
        z.insert(b"one-more", 0.0);
        assert!(!z.is_compact());

        let mut long = ZSet::new();
        long.insert("x".repeat(COMPACT_MAX_VALUE + 1).as_bytes(), 1.0);
        assert!(!long.is_compact());
    }

    #[test]
    fn large_set_keeps_ranks_consistent() {
        let mut z = ZSet::new();
        for i in 0..5_000u32 {
            // Insert in a scrambled order.
            let n = (i * 7919) % 5_000;
            z.insert(format!("m{n:05}").as_bytes(), f64::from(n));
        }
        assert_eq!(z.len(), 5_000);
        for n in (0..5_000).step_by(131) {
            let member = format!("m{n:05}");
            assert_eq!(z.rank(member.as_bytes()), Some(n));
            assert_eq!(members(z.range(n..n + 1, false)), [member]);
        }
        for n in (0..5_000).step_by(2) {
            z.remove(format!("m{n:05}").as_bytes());
        }
        assert_eq!(z.len(), 2_500);
        assert_eq!(z.rank(b"m00001"), Some(0));
        assert_eq!(z.rank(b"m04999"), Some(2_499));
        assert_eq!(z.score_range(incl(1000.0), incl(1999.0)).len(), 500);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u8, i8),
        Remove(u8),
    }

    proptest! {
        /// After any sequence of inserts, score changes and removals, the
        /// sorted set agrees with a sorted vector on order, ranks, lookups by
        /// rank, score ranges and reverse iteration.
        #[test]
        fn matches_sorted_vector(ops in prop::collection::vec(
            prop_oneof![
                3 => (0u8..60, -8i8..8).prop_map(|(m, s)| Op::Insert(m, s)),
                1 => (0u8..60).prop_map(Op::Remove),
            ],
            0..400,
        )) {
            let mut zset = ZSet::new();
            let mut model: Vec<(f64, Vec<u8>)> = Vec::new();
            for op in ops {
                match op {
                    Op::Insert(m, s) => {
                        let (member, score) = (vec![b'a' + m % 26, m], f64::from(s));
                        let previous = model.iter().position(|(_, x)| *x == member).map(|i| model.remove(i).0);
                        prop_assert_eq!(zset.insert(&member, score), previous);
                        model.push((score, member));
                    }
                    Op::Remove(m) => {
                        let member = vec![b'a' + m % 26, m];
                        let previous = model.iter().position(|(_, x)| *x == member).map(|i| model.remove(i).0);
                        prop_assert_eq!(zset.remove(&member), previous);
                    }
                }
                model.sort_by(|a, b| compare(a.0, &a.1, b.0, &b.1));
                prop_assert_eq!(zset.len(), model.len());
            }

            let forward: Vec<(f64, Vec<u8>)> = zset.iter().map(|(m, s)| (s, m.to_vec())).collect();
            prop_assert_eq!(&forward, &model);
            let mut backward: Vec<(f64, Vec<u8>)> =
                zset.range(0..zset.len(), true).map(|(m, s)| (s, m.to_vec())).collect();
            backward.reverse();
            prop_assert_eq!(&backward, &model);

            for (rank, (score, member)) in model.iter().enumerate() {
                prop_assert_eq!(zset.rank(member), Some(rank));
                prop_assert_eq!(zset.score(member), Some(*score));
                let at: Vec<Vec<u8>> = zset.range(rank..rank + 1, false).map(|(m, _)| m.to_vec()).collect();
                prop_assert_eq!(at, vec![member.clone()]);
            }
            for low in -9..9 {
                let (min, max) = (f64::from(low), f64::from(low + 3));
                let expected = model.iter().filter(|(s, _)| *s >= min && *s < max).count();
                let range = zset.score_range(
                    ScoreBound { value: min, exclusive: false },
                    ScoreBound { value: max, exclusive: true },
                );
                prop_assert_eq!(range.len(), expected);
            }
        }
    }
}
