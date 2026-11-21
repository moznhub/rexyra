//! Bitmap commands. A bitmap is an ordinary string addressed bit by bit;
//! bit 0 is the most significant bit of the first byte.

use bytes::{Bytes, BytesMut};
use rexyra_engine::{Db, SetTtl, Value};
use rexyra_resp::{parse_i64, Frame, MAX_BULK_LEN};

use crate::notify::{notify, GENERIC, STRING};
use crate::strings::{editable, read, read_counted, string_mut};
use crate::{int, is, reply, Ctx, ERR_SYNTAX};

/// One past the highest addressable bit (strings are capped at 512 MB).
const MAX_BITS: u64 = MAX_BULK_LEN as u64 * 8;
const ERR_BIT_OFFSET: &str = "ERR bit offset is not an integer or out of range";

fn bit_offset(arg: &[u8]) -> Result<u64, Frame> {
    match parse_i64(arg) {
        Some(n) if n >= 0 && (n as u64) < MAX_BITS => Ok(n as u64),
        _ => Err(Frame::error(ERR_BIT_OFFSET)),
    }
}

fn bit_value(arg: &[u8], error: &str) -> Result<bool, Frame> {
    match arg {
        b"1" => Ok(true),
        b"0" => Ok(false),
        _ => Err(Frame::error(error)),
    }
}

/// `BYTE` or `BIT`; returns whether indexes are in bits.
fn index_unit(arg: &[u8]) -> Result<bool, Frame> {
    if is(arg, "BIT") {
        Ok(true)
    } else if is(arg, "BYTE") {
        Ok(false)
    } else {
        Err(Frame::error(ERR_SYNTAX))
    }
}

/// Resolves an inclusive `start..=end` range with Redis's rules: negative
/// indexes count from the end, and both ends are clamped to `0..total`.
/// Returns `None` when the range is empty.
fn clamp_range(start: i64, end: i64, total: i64) -> Option<(usize, usize)> {
    if start < 0 && end < 0 && start > end {
        return None;
    }
    let resolve = |index: i64| {
        if index < 0 {
            index.saturating_add(total).max(0)
        } else {
            index
        }
    };
    let (start, end) = (resolve(start), resolve(end).min(total - 1));
    (start <= end).then_some((start as usize, end as usize))
}

/// The string at `key` as an editable buffer (empty if the key is missing).
/// The caller writes it back with [`store_buffer`].
fn take_buffer(db: &mut Db, key: &[u8], now: u64) -> Result<BytesMut, Frame> {
    Ok(match string_mut(db, key, now)? {
        Some(stored) => editable(stored, 0),
        None => BytesMut::new(),
    })
}

fn popcount(bytes: &[u8]) -> u64 {
    let (words, rest) = bytes.as_chunks::<8>();
    let in_words: u64 = words
        .iter()
        .map(|word| u64::from(u64::from_ne_bytes(*word).count_ones()))
        .sum();
    let in_rest: u64 = rest.iter().map(|byte| u64::from(byte.count_ones())).sum();
    in_words + in_rest
}

/// Mask selecting the bits of a byte at or after bit position `bit` (0–7).
fn from_bit(bit: usize) -> u8 {
    0xFF >> (bit & 7)
}

/// Mask selecting the bits of a byte up to and including position `bit`.
fn through_bit(bit: usize) -> u8 {
    !((1u8 << (7 - (bit & 7))) - 1)
}

// --- single bits ------------------------------------------------------------

/// `SETBIT key offset value`
/// Writes back a buffer taken with [`take_buffer`], preserving the TTL of an
/// existing key. `changed` says whether the contents differ from what was
/// taken; clients watching the key are only disturbed if they do.
fn store_buffer(db: &mut Db, key: &[u8], now: u64, buf: BytesMut, changed: bool) {
    let value = buf.freeze();
    match db.get_mut(key, now) {
        Some(Value::String(stored)) => {
            *stored = value;
            if !changed {
                db.unchanged(key);
            }
        }
        _ => db.set(key, Value::String(value), SetTtl::Keep, now),
    }
}

pub(crate) fn setbit(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let offset = bit_offset(&args[2])?;
        let on = bit_value(&args[3], "ERR bit is not an integer or out of range")?;
        let (db, now) = ctx.db();
        let mut buf = take_buffer(db, &args[1], now)?;

        let byte = (offset >> 3) as usize;
        let grew = buf.len() <= byte;
        if grew {
            buf.resize(byte + 1, 0);
        }
        let mask = 0x80u8 >> (offset & 7);
        let was_on = buf[byte] & mask != 0;
        if on {
            buf[byte] |= mask;
        } else {
            buf[byte] &= !mask;
        }
        let changed = grew || was_on != on;
        store_buffer(db, &args[1], now, buf, changed);
        if changed {
            notify(ctx, STRING, "setbit", &args[1]);
        }
        Ok(Frame::Integer(i64::from(was_on)))
    })())
}

/// `GETBIT key offset`
pub(crate) fn getbit(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let offset = bit_offset(&args[2])?;
        let data = read(ctx, &args[1])?.unwrap_or_default();
        let on = data
            .get((offset >> 3) as usize)
            .is_some_and(|byte| byte & (0x80 >> (offset & 7)) != 0);
        Ok(Frame::Integer(i64::from(on)))
    })())
}

// --- counting and searching -------------------------------------------------

/// `BITCOUNT key [start end [BYTE | BIT]]`
pub(crate) fn bitcount(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let range = match &args[2..] {
            [] => None,
            [start, end] => Some((int(start)?, int(end)?, false)),
            [start, end, unit] => Some((int(start)?, int(end)?, index_unit(unit)?)),
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let data = read(ctx, &args[1])?.unwrap_or_default();
        let Some((start, end, in_bits)) = range else {
            return Ok(Frame::Integer(popcount(&data) as i64));
        };

        let total = data.len() as i64 * if in_bits { 8 } else { 1 };
        let Some((start, end)) = clamp_range(start, end, total) else {
            return Ok(Frame::Integer(0));
        };
        let count = if in_bits {
            let (first, last) = (start >> 3, end >> 3);
            // Count whole bytes, then take off the bits outside the range in
            // the first and last byte.
            popcount(&data[first..=last])
                - u64::from((data[first] & !from_bit(start)).count_ones())
                - u64::from((data[last] & !through_bit(end)).count_ones())
        } else {
            popcount(&data[start..=end])
        };
        Ok(Frame::Integer(count as i64))
    })())
}

/// `BITPOS key bit [start [end [BYTE | BIT]]]`
pub(crate) fn bitpos(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let bit = bit_value(&args[2], "ERR The bit argument must be 1 or 0.")?;
        let (start, end, end_given, in_bits) = match &args[3..] {
            [] => (0, -1, false, false),
            [start] => (int(start)?, -1, false, false),
            [start, end] => (int(start)?, int(end)?, true, false),
            [start, end, unit] => (int(start)?, int(end)?, true, index_unit(unit)?),
            _ => return Err(Frame::error(ERR_SYNTAX)),
        };
        let Some(data) = read(ctx, &args[1])? else {
            // A missing key is an endless run of zero bits.
            return Ok(Frame::Integer(if bit { -1 } else { 0 }));
        };

        let total = data.len() as i64 * if in_bits { 8 } else { 1 };
        let Some((start, end)) = clamp_range(start, end, total) else {
            return Ok(Frame::Integer(-1));
        };
        let (first_bit, last_bit) = if in_bits {
            (start, end)
        } else {
            (start * 8, end * 8 + 7)
        };
        let (first, last) = (first_bit >> 3, last_bit >> 3);

        for index in first..=last {
            // Turn the bits we are looking for into ones, then drop any that
            // fall outside the requested range.
            let mut candidates = if bit { data[index] } else { !data[index] };
            if index == first {
                candidates &= from_bit(first_bit);
            }
            if index == last {
                candidates &= through_bit(last_bit);
            }
            if candidates != 0 {
                let position = index * 8 + candidates.leading_zeros() as usize;
                return Ok(Frame::Integer(position as i64));
            }
        }
        // Looking for a zero in a string of all ones with no explicit end:
        // the string is treated as padded with zeros, so the answer is the
        // first bit past it.
        Ok(Frame::Integer(if !bit && !end_given {
            last_bit as i64 + 1
        } else {
            -1
        }))
    })())
}

// --- BITOP --------------------------------------------------------------------

/// `BITOP <AND | OR | XOR | NOT> destkey key [key ...]`
pub(crate) fn bitop(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (op, dest, sources) = (&args[1], &args[2], &args[3..]);
    let combine: fn(u8, u8) -> u8 = if is(op, "AND") {
        |a, b| a & b
    } else if is(op, "OR") {
        |a, b| a | b
    } else if is(op, "XOR") {
        |a, b| a ^ b
    } else if is(op, "NOT") {
        if sources.len() != 1 {
            return Frame::error("ERR BITOP NOT must be called with a single source key.");
        }
        |a, _| !a
    } else {
        return Frame::error(ERR_SYNTAX);
    };

    // Missing keys count as empty strings; shorter inputs are zero-padded.
    let inputs: Vec<Bytes> = match sources
        .iter()
        .map(|key| read_counted(ctx, key).map(Option::unwrap_or_default))
        .collect::<Result<_, _>>()
    {
        Ok(inputs) => inputs,
        Err(e) => return e,
    };
    let len = inputs.iter().map(Bytes::len).max().unwrap_or(0);

    let (db, now) = ctx.db();
    if len == 0 {
        if db.remove(dest, now) {
            notify(ctx, GENERIC, "del", dest);
        }
        return Frame::Integer(0);
    }
    let mut result = BytesMut::zeroed(len);
    result[..inputs[0].len()].copy_from_slice(&inputs[0]);
    if is(op, "NOT") {
        for byte in result.iter_mut() {
            *byte = combine(*byte, 0);
        }
    } else {
        for input in &inputs[1..] {
            for (index, byte) in result.iter_mut().enumerate() {
                *byte = combine(*byte, input.get(index).copied().unwrap_or(0));
            }
        }
    }
    db.set(dest, Value::String(result.freeze()), SetTtl::Clear, now);
    notify(ctx, STRING, "set", dest);
    Frame::Integer(len as i64)
}

// --- BITFIELD -----------------------------------------------------------------

/// An integer type such as `u8` or `i16`.
#[derive(Clone, Copy)]
struct Field {
    signed: bool,
    bits: u32,
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

enum Op {
    Get(Field, u64),
    Set(Field, u64, i64, Overflow),
    IncrBy(Field, u64, i64, Overflow),
}

fn field_type(arg: &[u8]) -> Result<Field, Frame> {
    let parsed = match arg.split_first() {
        Some((b'i' | b'I', bits)) => parse_i64(bits).map(|bits| (true, bits, 64)),
        Some((b'u' | b'U', bits)) => parse_i64(bits).map(|bits| (false, bits, 63)),
        _ => None,
    };
    match parsed {
        Some((signed, bits, max)) if (1..=max).contains(&bits) => Ok(Field {
            signed,
            bits: bits as u32,
        }),
        _ => Err(Frame::error(
            "ERR Invalid bitfield type. Use something like i16 u8. \
             Note that u64 is not supported but i64 is.",
        )),
    }
}

/// A bit offset, or `#N` meaning "the Nth field of this width".
fn field_offset(arg: &[u8], field: Field) -> Result<u64, Frame> {
    let offset = match arg.strip_prefix(b"#") {
        Some(index) => parse_i64(index).and_then(|n| n.checked_mul(i64::from(field.bits))),
        None => parse_i64(arg),
    };
    match offset {
        Some(n) if n >= 0 && n as u64 + u64::from(field.bits) <= MAX_BITS => Ok(n as u64),
        _ => Err(Frame::error(ERR_BIT_OFFSET)),
    }
}

fn parse_ops(mut rest: &[Bytes]) -> Result<Vec<Op>, Frame> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    loop {
        match rest {
            [] => return Ok(ops),
            [name, kind, offset, tail @ ..] if is(name, "GET") => {
                let field = field_type(kind)?;
                ops.push(Op::Get(field, field_offset(offset, field)?));
                rest = tail;
            }
            [name, kind, offset, value, tail @ ..] if is(name, "SET") => {
                let field = field_type(kind)?;
                let offset = field_offset(offset, field)?;
                ops.push(Op::Set(field, offset, int(value)?, overflow));
                rest = tail;
            }
            [name, kind, offset, value, tail @ ..] if is(name, "INCRBY") => {
                let field = field_type(kind)?;
                let offset = field_offset(offset, field)?;
                ops.push(Op::IncrBy(field, offset, int(value)?, overflow));
                rest = tail;
            }
            [name, mode, tail @ ..] if is(name, "OVERFLOW") => {
                overflow = if is(mode, "WRAP") {
                    Overflow::Wrap
                } else if is(mode, "SAT") {
                    Overflow::Sat
                } else if is(mode, "FAIL") {
                    Overflow::Fail
                } else {
                    return Err(Frame::error("ERR Invalid OVERFLOW type specified"));
                };
                rest = tail;
            }
            _ => return Err(Frame::error(ERR_SYNTAX)),
        }
    }
}

/// Reads a field. Bits past the end of the string read as zero.
fn read_field(data: &[u8], field: Field, offset: u64) -> i64 {
    let mut raw: u64 = 0;
    for position in offset..offset + u64::from(field.bits) {
        let byte = data.get((position >> 3) as usize).copied().unwrap_or(0);
        raw = (raw << 1) | u64::from((byte >> (7 - (position & 7))) & 1);
    }
    if field.signed && field.bits < 64 && raw & (1 << (field.bits - 1)) != 0 {
        raw |= u64::MAX << field.bits; // sign-extend
    }
    raw as i64
}

/// Writes the low `field.bits` bits of `value`. The buffer must be long enough.
fn write_field(data: &mut [u8], field: Field, offset: u64, value: i64) {
    for index in 0..u64::from(field.bits) {
        let position = offset + index;
        let mask = 0x80u8 >> (position & 7);
        let byte = &mut data[(position >> 3) as usize];
        if (value as u64 >> (u64::from(field.bits) - 1 - index)) & 1 == 1 {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }
}

/// Fits an exact result into the field, applying the overflow policy.
/// `None` means `OVERFLOW FAIL` rejected it.
fn fit(exact: i128, field: Field, overflow: Overflow) -> Option<i64> {
    let span = 1i128 << field.bits;
    let (min, max) = if field.signed {
        (-(span / 2), span / 2 - 1)
    } else {
        (0, span - 1)
    };
    if (min..=max).contains(&exact) {
        return Some(exact as i64);
    }
    match overflow {
        Overflow::Wrap => Some(((exact - min).rem_euclid(span) + min) as i64),
        Overflow::Sat => Some(exact.clamp(min, max) as i64),
        Overflow::Fail => None,
    }
}

/// `BITFIELD key [GET type offset] [SET type offset value]
/// [INCRBY type offset increment] [OVERFLOW WRAP | SAT | FAIL] ...`
pub(crate) fn bitfield(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let ops = parse_ops(&args[2..])?;
        let key = &args[1];

        // The bytes every write needs to exist; zero when there are only GETs.
        let needed = ops
            .iter()
            .filter_map(|op| match op {
                Op::Get(..) => None,
                Op::Set(field, offset, ..) | Op::IncrBy(field, offset, ..) => {
                    Some((offset + u64::from(field.bits)).div_ceil(8) as usize)
                }
            })
            .max();

        let Some(needed) = needed else {
            // Read-only: never creates or touches the key.
            let data = read_counted(ctx, key)?.unwrap_or_default();
            return Ok(Frame::Array(
                ops.iter()
                    .map(|op| match op {
                        Op::Get(field, offset) => {
                            Frame::Integer(read_field(&data, *field, *offset))
                        }
                        Op::Set(..) | Op::IncrBy(..) => unreachable!("no writes in this branch"),
                    })
                    .collect(),
            ));
        };

        let (db, now) = ctx.db();
        let mut buf = take_buffer(db, key, now)?;
        let mut changed = buf.len() < needed;
        if changed {
            buf.resize(needed, 0);
        }
        // Writes that were carried out, whether or not they changed a bit:
        // Redis reports the command as a change if there was any.
        let mut writes = 0;
        let replies = ops
            .iter()
            .map(|op| match *op {
                Op::Get(field, offset) => Frame::Integer(read_field(&buf, field, offset)),
                Op::Set(field, offset, value, overflow) => {
                    let old = read_field(&buf, field, offset);
                    match fit(i128::from(value), field, overflow) {
                        Some(value) => {
                            write_field(&mut buf, field, offset, value);
                            changed |= value != old;
                            writes += 1;
                            Frame::Integer(old)
                        }
                        None => Frame::Null,
                    }
                }
                Op::IncrBy(field, offset, delta, overflow) => {
                    let old = read_field(&buf, field, offset);
                    match fit(i128::from(old) + i128::from(delta), field, overflow) {
                        Some(value) => {
                            write_field(&mut buf, field, offset, value);
                            changed |= value != old;
                            writes += 1;
                            Frame::Integer(value)
                        }
                        None => Frame::Null,
                    }
                }
            })
            .collect();
        store_buffer(db, key, now, buf, changed);
        if writes > 0 {
            notify(ctx, STRING, "setbit", key);
        }
        Ok(Frame::Array(replies))
    })())
}

/// `BITFIELD_RO key [GET type offset] ...`
pub(crate) fn bitfield_ro(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let only_gets = args[2..]
        .chunks(3)
        .all(|chunk| chunk.first().is_some_and(|name| is(name, "GET")));
    if !only_gets {
        return Frame::error("ERR BITFIELD_RO only supports the GET subcommand");
    }
    bitfield(ctx, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, err, Harness};

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    fn ints(values: &[Option<i64>]) -> Frame {
        Frame::Array(
            values
                .iter()
                .map(|v| v.map_or(Frame::Null, Frame::Integer))
                .collect(),
        )
    }

    fn bytes(h: &mut Harness, key: &str) -> Vec<u8> {
        match h.cmd(&["GET", key]) {
            Frame::Bulk(b) => b.to_vec(),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    fn set_bytes(h: &mut Harness, key: &str, value: &[u8]) {
        h.raw(&[
            Bytes::from_static(b"SET"),
            Bytes::copy_from_slice(key.as_bytes()),
            Bytes::copy_from_slice(value),
        ]);
    }

    #[test]
    fn setbit_getbit() {
        let mut h = Harness::new();
        assert_eq!(h.run("GETBIT k 7"), int(0));
        assert_eq!(h.run("SETBIT k 7 1"), int(0));
        assert_eq!(h.run("SETBIT k 7 1"), int(1));
        assert_eq!(h.run("GETBIT k 7"), int(1));
        assert_eq!(
            bytes(&mut h, "k"),
            [0x01],
            "bit 7 is the lowest bit of byte 0"
        );

        assert_eq!(h.run("SETBIT k 0 1"), int(0));
        assert_eq!(bytes(&mut h, "k"), [0x81]);
        assert_eq!(h.run("SETBIT k 7 0"), int(1));
        assert_eq!(bytes(&mut h, "k"), [0x80]);

        assert_eq!(h.run("SETBIT k 20 1"), int(0), "grows with zero padding");
        assert_eq!(bytes(&mut h, "k"), [0x80, 0x00, 0x08]);
        assert_eq!(h.run("GETBIT k 1000"), int(0));
    }

    #[test]
    fn setbit_errors_and_ttl() {
        let mut h = Harness::new();
        let offset = "ERR bit offset is not an integer or out of range";
        assert_eq!(h.run("SETBIT k -1 1"), err(offset));
        assert_eq!(h.run("SETBIT k x 1"), err(offset));
        assert_eq!(h.run("SETBIT k 4294967296 1"), err(offset));
        assert_eq!(h.run("GETBIT k 4294967296"), err(offset));
        assert_eq!(
            h.run("SETBIT k 0 2"),
            err("ERR bit is not an integer or out of range")
        );
        assert_eq!(h.run("EXISTS k"), int(0));

        h.run("SET t a EX 100");
        h.run("SETBIT t 0 1");
        assert_eq!(h.run("TTL t"), int(100));
    }

    #[test]
    fn bitcount() {
        let mut h = Harness::new();
        assert_eq!(h.run("BITCOUNT missing"), int(0));
        h.run("SET k foobar");
        assert_eq!(h.run("BITCOUNT k"), int(26));
        assert_eq!(h.run("BITCOUNT k 0 0"), int(4));
        assert_eq!(h.run("BITCOUNT k 1 1"), int(6));
        assert_eq!(h.run("BITCOUNT k 1 1 BYTE"), int(6));
        assert_eq!(h.run("BITCOUNT k 5 30 BIT"), int(17));
        assert_eq!(h.run("BITCOUNT k -1 -1"), int(4));
        assert_eq!(h.run("BITCOUNT k 0 -1"), int(26));
        assert_eq!(h.run("BITCOUNT k 0 -1 bit"), int(26));
        assert_eq!(h.run("BITCOUNT k 3 1"), int(0));
        assert_eq!(h.run("BITCOUNT k 100 200"), int(0));
        assert_eq!(h.run("BITCOUNT k 0"), err("ERR syntax error"));
        assert_eq!(h.run("BITCOUNT k 0 1 NIBBLE"), err("ERR syntax error"));
        assert_eq!(
            h.run("BITCOUNT k a b"),
            err("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn bitcount_bit_ranges_match_a_naive_count() {
        let mut h = Harness::new();
        let data: Vec<u8> = (0..23u32).map(|i| (i * 37 + 11) as u8).collect();
        set_bytes(&mut h, "k", &data);
        let bit = |i: usize| (data[i / 8] >> (7 - i % 8)) & 1;
        let total = data.len() * 8;
        for start in (0..total).step_by(5) {
            for end in (start..total).step_by(7) {
                let expected: i64 = (start..=end).map(|i| i64::from(bit(i))).sum();
                assert_eq!(
                    h.run(&format!("BITCOUNT k {start} {end} BIT")),
                    int(expected),
                    "{start}..={end}"
                );
            }
        }
        assert_eq!(popcount(&data), (0..total).map(|i| u64::from(bit(i))).sum());
    }

    #[test]
    fn bitpos() {
        let mut h = Harness::new();
        assert_eq!(h.run("BITPOS missing 1"), int(-1));
        assert_eq!(h.run("BITPOS missing 0"), int(0));

        set_bytes(&mut h, "k", &[0xff, 0xf0, 0x00]);
        assert_eq!(h.run("BITPOS k 0"), int(12));
        assert_eq!(h.run("BITPOS k 1"), int(0));
        assert_eq!(h.run("BITPOS k 1 2"), int(-1));
        assert_eq!(h.run("BITPOS k 0 2"), int(16));

        set_bytes(&mut h, "k", &[0x00, 0xff, 0xf0]);
        assert_eq!(h.run("BITPOS k 1 0"), int(8));
        assert_eq!(h.run("BITPOS k 1 2"), int(16));
        assert_eq!(h.run("BITPOS k 1 7 15 BIT"), int(8));
        assert_eq!(h.run("BITPOS k 1 0 7 BIT"), int(-1));
        assert_eq!(h.run("BITPOS k 0 9 15 BIT"), int(-1));
        assert_eq!(h.run("BITPOS k 1 -1 -1"), int(16));

        set_bytes(&mut h, "k", &[0xff, 0xff, 0xff]);
        assert_eq!(
            h.run("BITPOS k 0"),
            int(24),
            "padded with zeros past the end"
        );
        assert_eq!(h.run("BITPOS k 0 1"), int(24));
        assert_eq!(
            h.run("BITPOS k 0 0 -1"),
            int(-1),
            "an explicit end disables padding"
        );

        assert_eq!(
            h.run("BITPOS k 2"),
            err("ERR The bit argument must be 1 or 0.")
        );
        assert_eq!(h.run("BITPOS k 1 0 1 BIT extra"), err("ERR syntax error"));
    }

    #[test]
    fn bitop() {
        let mut h = Harness::new();
        set_bytes(&mut h, "a", &[0b1100_1100, 0xff]);
        set_bytes(&mut h, "b", &[0b1010_1010]);

        assert_eq!(h.run("BITOP AND d a b"), int(2));
        assert_eq!(
            bytes(&mut h, "d"),
            [0b1000_1000, 0x00],
            "short input is zero-padded"
        );
        assert_eq!(h.run("BITOP OR d a b"), int(2));
        assert_eq!(bytes(&mut h, "d"), [0b1110_1110, 0xff]);
        assert_eq!(h.run("BITOP xor d a b"), int(2));
        assert_eq!(bytes(&mut h, "d"), [0b0110_0110, 0xff]);
        assert_eq!(h.run("BITOP NOT d a"), int(2));
        assert_eq!(bytes(&mut h, "d"), [0b0011_0011, 0x00]);

        assert_eq!(h.run("BITOP AND d a missing"), int(2));
        assert_eq!(bytes(&mut h, "d"), [0, 0]);

        assert_eq!(h.run("BITOP OR d missing1 missing2"), int(0));
        assert_eq!(
            h.run("EXISTS d"),
            int(0),
            "an empty result deletes the destination"
        );

        assert_eq!(
            h.run("BITOP NOT d a b"),
            err("ERR BITOP NOT must be called with a single source key.")
        );
        assert_eq!(h.run("BITOP NAND d a b"), err("ERR syntax error"));
    }

    #[test]
    fn bitop_may_overwrite_one_of_its_sources() {
        let mut h = Harness::new();
        set_bytes(&mut h, "a", &[0x0f]);
        set_bytes(&mut h, "b", &[0x3c]);
        assert_eq!(h.run("BITOP XOR a a b"), int(1));
        assert_eq!(bytes(&mut h, "a"), [0x33]);
    }

    #[test]
    fn bitfield_get_set() {
        let mut h = Harness::new();
        assert_eq!(h.run("BITFIELD k GET u8 0"), ints(&[Some(0)]));
        assert_eq!(h.run("EXISTS k"), int(0), "GET alone creates nothing");

        assert_eq!(h.run("BITFIELD k SET u8 0 255"), ints(&[Some(0)]));
        assert_eq!(
            h.run("BITFIELD k GET u8 0 GET i8 0 GET u4 0 GET u4 4 GET u8 4"),
            ints(&[Some(255), Some(-1), Some(15), Some(15), Some(240)])
        );
        assert_eq!(h.run("BITFIELD k SET i5 3 -7"), ints(&[Some(-1)]));
        assert_eq!(h.run("BITFIELD k GET i5 3"), ints(&[Some(-7)]));
        assert_eq!(bytes(&mut h, "k"), [0b1111_1001]);

        // `#N` addresses the Nth field of that width.
        assert_eq!(h.run("BITFIELD k SET u8 #2 7"), ints(&[Some(0)]));
        assert_eq!(bytes(&mut h, "k"), [0b1111_1001, 0, 7]);
        assert_eq!(
            h.run("BITFIELD k GET u8 #2 GET u8 16"),
            ints(&[Some(7), Some(7)])
        );
    }

    #[test]
    fn bitfield_full_width_types() {
        let mut h = Harness::new();
        assert_eq!(
            h.run("BITFIELD k SET i64 0 -9223372036854775808"),
            ints(&[Some(0)])
        );
        assert_eq!(h.run("BITFIELD k GET i64 0"), ints(&[Some(i64::MIN)]));
        assert_eq!(
            h.run("BITFIELD k SET u63 1 9223372036854775807"),
            ints(&[Some(0)])
        );
        assert_eq!(h.run("BITFIELD k GET u63 1"), ints(&[Some(i64::MAX)]));
        assert_eq!(h.run("BITFIELD k GET i64 0"), ints(&[Some(-1)]));
    }

    #[test]
    fn bitfield_incrby_and_overflow() {
        let mut h = Harness::new();
        assert_eq!(h.run("BITFIELD k INCRBY u2 0 1"), ints(&[Some(1)]));
        assert_eq!(h.run("BITFIELD k INCRBY u2 0 1"), ints(&[Some(2)]));
        assert_eq!(h.run("BITFIELD k INCRBY u2 0 1"), ints(&[Some(3)]));
        assert_eq!(
            h.run("BITFIELD k INCRBY u2 0 1"),
            ints(&[Some(0)]),
            "wraps by default"
        );

        assert_eq!(
            h.run("BITFIELD s OVERFLOW SAT INCRBY u2 0 10 INCRBY i4 4 100 INCRBY i4 8 -100"),
            ints(&[Some(3), Some(7), Some(-8)])
        );
        assert_eq!(
            h.run("BITFIELD f OVERFLOW FAIL INCRBY u2 0 3 INCRBY u2 0 1 GET u2 0"),
            ints(&[Some(3), None, Some(3)])
        );
        // Signed wrap goes from the maximum to the minimum.
        assert_eq!(
            h.run("BITFIELD w SET i8 0 127 INCRBY i8 0 1"),
            ints(&[Some(0), Some(-128)])
        );
        // The overflow mode applies to the operations that follow it.
        assert_eq!(
            h.run("BITFIELD m INCRBY u2 0 5 OVERFLOW SAT INCRBY u2 2 5"),
            ints(&[Some(1), Some(3)])
        );
        assert_eq!(
            h.run("BITFIELD x OVERFLOW FAIL SET u4 0 99 GET u4 0"),
            ints(&[None, Some(0)])
        );
        assert_eq!(
            h.run("BITFIELD y SET u4 0 99 GET u4 0"),
            ints(&[Some(0), Some(3)])
        );
    }

    #[test]
    fn bitfield_errors() {
        let mut h = Harness::new();
        let bad_type = "ERR Invalid bitfield type. Use something like i16 u8. \
                        Note that u64 is not supported but i64 is.";
        for kind in ["u64", "i65", "u0", "x8", "i", "8"] {
            assert_eq!(
                h.run(&format!("BITFIELD k GET {kind} 0")),
                err(bad_type),
                "{kind}"
            );
        }
        let bad_offset = "ERR bit offset is not an integer or out of range";
        assert_eq!(h.run("BITFIELD k GET u8 -1"), err(bad_offset));
        assert_eq!(h.run("BITFIELD k GET u8 x"), err(bad_offset));
        assert_eq!(h.run("BITFIELD k SET u8 4294967290 1"), err(bad_offset));
        assert_eq!(h.run("BITFIELD k GET u8"), err("ERR syntax error"));
        assert_eq!(h.run("BITFIELD k FROB u8 0"), err("ERR syntax error"));
        assert_eq!(
            h.run("BITFIELD k OVERFLOW BOOM"),
            err("ERR Invalid OVERFLOW type specified")
        );
        assert_eq!(
            h.run("BITFIELD k SET u8 0 x"),
            err("ERR value is not an integer or out of range")
        );
        // A bad later operation must not leave earlier ones applied.
        assert_eq!(h.run("BITFIELD k SET u8 0 1 GET u99 0"), err(bad_type));
        assert_eq!(h.run("EXISTS k"), int(0));
    }

    #[test]
    fn bitfield_ro() {
        let mut h = Harness::new();
        h.run("SET k a");
        assert_eq!(
            h.run("BITFIELD_RO k GET u8 0 GET u4 0"),
            ints(&[Some(97), Some(6)])
        );
        assert_eq!(
            h.run("BITFIELD_RO k SET u8 0 1"),
            err("ERR BITFIELD_RO only supports the GET subcommand")
        );
        assert_eq!(
            h.run("BITFIELD_RO k GET u8 0 INCRBY u8 0 1"),
            err("ERR BITFIELD_RO only supports the GET subcommand")
        );
        assert_eq!(h.run("GET k"), bulk("a"));
    }

    #[test]
    fn bitfield_keeps_the_ttl() {
        let mut h = Harness::new();
        h.run("SET k a EX 100");
        h.run("BITFIELD k SET u8 0 98");
        assert_eq!(h.run("GET k"), bulk("b"));
        assert_eq!(h.run("TTL k"), int(100));
    }
}
