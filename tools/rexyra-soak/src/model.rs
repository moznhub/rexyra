//! What each worker believes its keys hold, the operations it sends, and the
//! reply each one must get. Every worker owns its own keys, so the model is
//! exact: any reply that differs from it is a fault in the server.

use crate::resp::Reply;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

/// A small, fast, deterministic random number generator (xorshift64*).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        (0..len)
            .map(|_| ALPHABET[self.below(ALPHABET.len())])
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Str(Vec<u8>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    List(VecDeque<Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    Zset(BTreeMap<Vec<u8>, i64>),
}

/// The reply a command must get.
#[derive(Clone, Debug)]
pub enum Expect {
    Exact(Reply),
    /// A key with a time to live: either already gone, or still this value.
    NilOr(Vec<u8>),
}

impl Expect {
    pub fn matches(&self, got: &Reply) -> bool {
        match self {
            Expect::Exact(r) => r == got,
            Expect::NilOr(v) => *got == Reply::Bulk(None) || *got == Reply::Bulk(Some(v.clone())),
        }
    }
}

pub struct Cmd {
    pub args: Vec<Vec<u8>>,
    pub expect: Expect,
}

/// One logical operation: one command, or a few sent together (a
/// transaction, or a push followed by a trim). `key` is the key it is
/// about, which is reset if anything goes wrong with it.
pub struct Op {
    pub key: Vec<u8>,
    pub cmds: Vec<Cmd>,
}

const LUA_INCR: &[u8] = b"return redis.call('INCRBY', KEYS[1], ARGV[1])";
pub const KINDS: [&str; 6] = ["s", "c", "h", "l", "t", "z"];
const FIELDS: usize = 32;
const MEMBERS: usize = 64;
const LIST_MAX: usize = 64;

fn ok() -> Expect {
    Expect::Exact(Reply::Simple("OK".into()))
}
fn int(n: i64) -> Expect {
    Expect::Exact(Reply::Int(n))
}
fn bulk(v: Option<Vec<u8>>) -> Expect {
    Expect::Exact(Reply::Bulk(v))
}
fn num(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}
fn args(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

pub struct Model {
    pub worker: usize,
    pub keys_per_kind: usize,
    pub data: HashMap<Vec<u8>, Val>,
    /// Keys written with a time to live, and the last value written.
    pub volatile: HashMap<Vec<u8>, Vec<u8>>,
    rng: Rng,
}

impl Model {
    pub fn new(worker: usize, keys_per_kind: usize, seed: u64) -> Self {
        Self {
            worker,
            keys_per_kind,
            data: HashMap::new(),
            volatile: HashMap::new(),
            rng: Rng::new(seed ^ ((worker as u64 + 1) * 0x1000_0001)),
        }
    }

    pub fn key(&self, kind: &str, i: usize) -> Vec<u8> {
        format!("soak:w{}:{kind}:{i}", self.worker).into_bytes()
    }

    pub fn volatile_keys(&self) -> usize {
        (self.keys_per_kind / 10).max(1)
    }

    fn pick(&mut self, kind: &str) -> Vec<u8> {
        let i = self.rng.below(self.keys_per_kind);
        self.key(kind, i)
    }

    fn field(&mut self) -> Vec<u8> {
        format!("f{}", self.rng.below(FIELDS)).into_bytes()
    }

    fn member(&mut self) -> Vec<u8> {
        format!("m{}", self.rng.below(MEMBERS)).into_bytes()
    }

    fn value(&mut self) -> Vec<u8> {
        let len = 8 + self.rng.below(248);
        self.rng.bytes(len)
    }

    /// Forgets a key, after a reply that did not match or a lost connection
    /// left its state unknown. The caller deletes it on the server too.
    pub fn forget(&mut self, key: &[u8]) {
        self.data.remove(key);
        self.volatile.remove(key);
    }

    /// The next operation, with the model already updated to the state the
    /// server must be in once it has run.
    pub fn next_op(&mut self) -> Op {
        match self.rng.below(100) {
            0..=19 => self.string_op(),
            20..=29 => self.counter_op(),
            30..=44 => self.hash_op(),
            45..=57 => self.list_op(),
            58..=69 => self.set_op(),
            70..=83 => self.zset_op(),
            84..=88 => self.transaction(),
            89..=92 => self.script(),
            93..=96 => self.volatile_op(),
            _ => self.delete(),
        }
    }

    fn string_op(&mut self) -> Op {
        let key = self.pick("s");
        let current = match self.data.get(&key) {
            Some(Val::Str(v)) => Some(v.clone()),
            _ => None,
        };
        let cmd = match self.rng.below(10) {
            0..=3 => {
                let v = self.value();
                self.data.insert(key.clone(), Val::Str(v.clone()));
                Cmd {
                    args: args(&[b"SET", &key, &v]),
                    expect: ok(),
                }
            }
            4..=7 => Cmd {
                args: args(&[b"GET", &key]),
                expect: bulk(current),
            },
            8 => {
                let mut v = current.unwrap_or_default();
                if v.len() > 2048 {
                    v.clear();
                    self.data.remove(&key);
                    return Op {
                        key: key.clone(),
                        cmds: vec![Cmd {
                            args: args(&[b"DEL", &key]),
                            expect: int(1),
                        }],
                    };
                }
                let extra = self.value();
                v.extend_from_slice(&extra);
                let len = v.len() as i64;
                self.data.insert(key.clone(), Val::Str(v));
                Cmd {
                    args: args(&[b"APPEND", &key, &extra]),
                    expect: int(len),
                }
            }
            _ => Cmd {
                args: args(&[b"STRLEN", &key]),
                expect: int(current.map_or(0, |v| v.len() as i64)),
            },
        };
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn counter(&self, key: &[u8]) -> i64 {
        match self.data.get(key) {
            Some(Val::Str(v)) => std::str::from_utf8(v).unwrap_or("0").parse().unwrap_or(0),
            _ => 0,
        }
    }

    fn incr(&mut self, key: &[u8], by: i64) -> i64 {
        let n = self.counter(key) + by;
        self.data.insert(key.to_vec(), Val::Str(num(n)));
        n
    }

    fn counter_op(&mut self) -> Op {
        let key = self.pick("c");
        let cmd = if self.rng.below(3) == 0 {
            let current = match self.data.get(&key) {
                Some(Val::Str(v)) => Some(v.clone()),
                _ => None,
            };
            Cmd {
                args: args(&[b"GET", &key]),
                expect: bulk(current),
            }
        } else {
            let by = self.rng.range_i64(-1000, 1000);
            let n = self.incr(&key, by);
            Cmd {
                args: args(&[b"INCRBY", &key, &num(by)]),
                expect: int(n),
            }
        };
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn transaction(&mut self) -> Op {
        let a = self.pick("c");
        let mut b = self.pick("c");
        while b == a && self.keys_per_kind > 1 {
            b = self.pick("c");
        }
        let (x, y) = (self.rng.range_i64(1, 9), self.rng.range_i64(1, 9));
        let na = self.incr(&a, x);
        let nb = self.incr(&b, y);
        let queued = || Expect::Exact(Reply::Simple("QUEUED".into()));
        Op {
            key: a.clone(),
            cmds: vec![
                Cmd {
                    args: args(&[b"MULTI"]),
                    expect: ok(),
                },
                Cmd {
                    args: args(&[b"INCRBY", &a, &num(x)]),
                    expect: queued(),
                },
                Cmd {
                    args: args(&[b"INCRBY", &b, &num(y)]),
                    expect: queued(),
                },
                Cmd {
                    args: args(&[b"EXEC"]),
                    expect: Expect::Exact(Reply::Array(Some(vec![Reply::Int(na), Reply::Int(nb)]))),
                },
            ],
        }
    }

    fn script(&mut self) -> Op {
        let key = self.pick("c");
        let by = self.rng.range_i64(-50, 50);
        let n = self.incr(&key, by);
        Op {
            key: key.clone(),
            cmds: vec![Cmd {
                args: args(&[b"EVAL", LUA_INCR, b"1", &key, &num(by)]),
                expect: int(n),
            }],
        }
    }

    fn hash_op(&mut self) -> Op {
        let key = self.pick("h");
        let f = self.field();
        let mut h = match self.data.remove(&key) {
            Some(Val::Hash(h)) => h,
            _ => BTreeMap::new(),
        };
        let cmd = match self.rng.below(10) {
            0..=3 => {
                let v = self.value();
                let new = h.insert(f.clone(), v.clone()).is_none();
                Cmd {
                    args: args(&[b"HSET", &key, &f, &v]),
                    expect: int(new as i64),
                }
            }
            4..=6 => Cmd {
                args: args(&[b"HGET", &key, &f]),
                expect: bulk(h.get(&f).cloned()),
            },
            7..=8 => Cmd {
                args: args(&[b"HDEL", &key, &f]),
                expect: int(h.remove(&f).is_some() as i64),
            },
            _ => Cmd {
                args: args(&[b"HLEN", &key]),
                expect: int(h.len() as i64),
            },
        };
        if !h.is_empty() {
            self.data.insert(key.clone(), Val::Hash(h));
        }
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn list_op(&mut self) -> Op {
        let key = self.pick("l");
        let mut l = match self.data.remove(&key) {
            Some(Val::List(l)) => l,
            _ => VecDeque::new(),
        };
        let mut cmds = Vec::new();
        match self.rng.below(10) {
            0..=4 => {
                let v = self.value();
                l.push_back(v.clone());
                cmds.push(Cmd {
                    args: args(&[b"RPUSH", &key, &v]),
                    expect: int(l.len() as i64),
                });
                if l.len() > LIST_MAX {
                    while l.len() > LIST_MAX {
                        l.pop_front();
                    }
                    cmds.push(Cmd {
                        args: args(&[b"LTRIM", &key, &num(-(LIST_MAX as i64)), b"-1"]),
                        expect: ok(),
                    });
                }
            }
            5..=6 => cmds.push(Cmd {
                args: args(&[b"LPOP", &key]),
                expect: bulk(l.pop_front()),
            }),
            7..=8 => {
                let i = self.rng.below(LIST_MAX + 4);
                cmds.push(Cmd {
                    args: args(&[b"LINDEX", &key, &num(i as i64)]),
                    expect: bulk(l.get(i).cloned()),
                });
            }
            _ => cmds.push(Cmd {
                args: args(&[b"LLEN", &key]),
                expect: int(l.len() as i64),
            }),
        }
        if !l.is_empty() {
            self.data.insert(key.clone(), Val::List(l));
        }
        Op { key, cmds }
    }

    fn set_op(&mut self) -> Op {
        let key = self.pick("t");
        let m = self.member();
        let mut s = match self.data.remove(&key) {
            Some(Val::Set(s)) => s,
            _ => BTreeSet::new(),
        };
        let cmd = match self.rng.below(10) {
            0..=3 => Cmd {
                args: args(&[b"SADD", &key, &m]),
                expect: int(s.insert(m.clone()) as i64),
            },
            4..=5 => Cmd {
                args: args(&[b"SREM", &key, &m]),
                expect: int(s.remove(&m) as i64),
            },
            6..=8 => Cmd {
                args: args(&[b"SISMEMBER", &key, &m]),
                expect: int(s.contains(&m) as i64),
            },
            _ => Cmd {
                args: args(&[b"SCARD", &key]),
                expect: int(s.len() as i64),
            },
        };
        if !s.is_empty() {
            self.data.insert(key.clone(), Val::Set(s));
        }
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn zset_op(&mut self) -> Op {
        let key = self.pick("z");
        let m = self.member();
        let mut z = match self.data.remove(&key) {
            Some(Val::Zset(z)) => z,
            _ => BTreeMap::new(),
        };
        let cmd = match self.rng.below(10) {
            0..=2 => {
                let score = self.rng.range_i64(-100_000, 100_000);
                let new = z.insert(m.clone(), score).is_none();
                Cmd {
                    args: args(&[b"ZADD", &key, &num(score), &m]),
                    expect: int(new as i64),
                }
            }
            3..=4 => {
                let by = self.rng.range_i64(-100, 100);
                let score = z.get(&m).copied().unwrap_or(0) + by;
                z.insert(m.clone(), score);
                Cmd {
                    args: args(&[b"ZINCRBY", &key, &num(by), &m]),
                    expect: bulk(Some(num(score))),
                }
            }
            5..=6 => Cmd {
                args: args(&[b"ZSCORE", &key, &m]),
                expect: bulk(z.get(&m).map(|s| num(*s))),
            },
            7 => Cmd {
                args: args(&[b"ZREM", &key, &m]),
                expect: int(z.remove(&m).is_some() as i64),
            },
            _ => Cmd {
                args: args(&[b"ZCARD", &key]),
                expect: int(z.len() as i64),
            },
        };
        if !z.is_empty() {
            self.data.insert(key.clone(), Val::Zset(z));
        }
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn volatile_op(&mut self) -> Op {
        let i = self.rng.below(self.volatile_keys());
        let key = self.key("v", i);
        let cmd = if self.rng.below(2) == 0 {
            let v = self.value();
            let ms = 50 + self.rng.below(3000);
            self.volatile.insert(key.clone(), v.clone());
            Cmd {
                args: args(&[b"SET", &key, &v, b"PX", &num(ms as i64)]),
                expect: ok(),
            }
        } else {
            match self.volatile.get(&key) {
                Some(v) => Cmd {
                    args: args(&[b"GET", &key]),
                    expect: Expect::NilOr(v.clone()),
                },
                None => Cmd {
                    args: args(&[b"GET", &key]),
                    expect: bulk(None),
                },
            }
        };
        Op {
            key,
            cmds: vec![cmd],
        }
    }

    fn delete(&mut self) -> Op {
        let kind = KINDS[self.rng.below(KINDS.len())];
        let key = self.pick(kind);
        let existed = self.data.remove(&key).is_some();
        Op {
            key: key.clone(),
            cmds: vec![Cmd {
                args: args(&[b"DEL", &key]),
                expect: int(existed as i64),
            }],
        }
    }

    /// Every key this worker can ever touch, with the command that reads it
    /// back and the reply it must get.
    pub fn checks(&self) -> Vec<(Vec<u8>, Cmd)> {
        let mut out = Vec::new();
        for kind in KINDS {
            for i in 0..self.keys_per_kind {
                let key = self.key(kind, i);
                let cmd = match self.data.get(&key) {
                    None => Cmd {
                        args: args(&[b"EXISTS", &key]),
                        expect: int(0),
                    },
                    Some(Val::Str(v)) => Cmd {
                        args: args(&[b"GET", &key]),
                        expect: bulk(Some(v.clone())),
                    },
                    Some(Val::Hash(h)) => Cmd {
                        args: args(&[b"HGETALL", &key]),
                        expect: Expect::Exact(Reply::Simple(format!("hash:{}", h.len()))),
                    },
                    Some(Val::List(l)) => Cmd {
                        args: args(&[b"LRANGE", &key, b"0", b"-1"]),
                        expect: Expect::Exact(Reply::Array(Some(
                            l.iter().map(|v| Reply::Bulk(Some(v.clone()))).collect(),
                        ))),
                    },
                    Some(Val::Set(_)) => Cmd {
                        args: args(&[b"SMEMBERS", &key]),
                        expect: Expect::Exact(Reply::Simple("set".into())),
                    },
                    Some(Val::Zset(z)) => {
                        let mut pairs: Vec<(&Vec<u8>, &i64)> = z.iter().collect();
                        pairs.sort_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)));
                        let mut items = Vec::new();
                        for (m, s) in pairs {
                            items.push(Reply::Bulk(Some(m.clone())));
                            items.push(Reply::Bulk(Some(num(*s))));
                        }
                        Cmd {
                            args: args(&[b"ZRANGE", &key, b"0", b"-1", b"WITHSCORES"]),
                            expect: Expect::Exact(Reply::Array(Some(items))),
                        }
                    }
                };
                out.push((key, cmd));
            }
        }
        for i in 0..self.volatile_keys() {
            let key = self.key("v", i);
            let expect = match self.volatile.get(&key) {
                Some(v) => Expect::NilOr(v.clone()),
                None => bulk(None),
            };
            out.push((
                key.clone(),
                Cmd {
                    args: args(&[b"GET", &key]),
                    expect,
                },
            ));
        }
        out
    }

    /// Checks a read-back reply for a key, including the unordered replies
    /// (`HGETALL`, `SMEMBERS`) that `checks` cannot express exactly.
    pub fn check_reply(&self, key: &[u8], cmd: &Cmd, got: &Reply) -> bool {
        match self.data.get(key) {
            Some(Val::Hash(h)) => {
                let Reply::Array(Some(items)) = got else {
                    return false;
                };
                if items.len() != h.len() * 2 {
                    return false;
                }
                let mut seen = BTreeMap::new();
                for pair in items.chunks(2) {
                    match (&pair[0], &pair[1]) {
                        (Reply::Bulk(Some(f)), Reply::Bulk(Some(v))) => {
                            seen.insert(f.clone(), v.clone());
                        }
                        _ => return false,
                    }
                }
                &seen == h
            }
            Some(Val::Set(s)) => {
                let Reply::Array(Some(items)) = got else {
                    return false;
                };
                let mut seen = BTreeSet::new();
                for item in items {
                    match item {
                        Reply::Bulk(Some(m)) => {
                            seen.insert(m.clone());
                        }
                        _ => return false,
                    }
                }
                items.len() == s.len() && &seen == s
            }
            _ => cmd.expect.matches(got),
        }
    }
}
