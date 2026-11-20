//! The extra libraries Redis gives scripts on top of standard Lua: `bit`,
//! `cjson`, `struct`, `cmsgpack`, and a `math.random` whose sequence is the
//! same on every platform.
//!
//! They are reimplemented here in Rust rather than compiled from the C
//! originals, following those libraries' observable behaviour: the same
//! encodings, the same number formatting and, where scripts could depend on
//! them, the same error messages.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use mlua::{Error as LuaError, Lua, Result, Table, Value, Variadic};

fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(LuaError::runtime(message.into()))
}

/// Creates a library function whose errors start with the position of the
/// script line that called it (`user_script:3: ...`), as errors from Lua's
/// own C libraries do.
fn located<A, R, F>(lua: &Lua, function: F) -> Result<mlua::Function>
where
    A: mlua::FromLuaMulti,
    R: mlua::IntoLuaMulti,
    F: Fn(&Lua, A) -> Result<R> + Send + 'static,
{
    lua.create_function(move |lua, args: A| {
        function(lua, args).map_err(|error| match error {
            LuaError::RuntimeError(text) => crate::scripting::error_at_caller(lua, text),
            other => other,
        })
    })
}

/// The Lua name of a value's type, for error messages.
fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Nil => "nil",
        Value::Boolean(_) => "boolean",
        Value::LightUserData(_) | Value::UserData(_) => "userdata",
        Value::Integer(_) | Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Table(_) => "table",
        Value::Function(_) => "function",
        Value::Thread(_) => "thread",
        _ => "userdata",
    }
}

/// A Lua number argument, accepting numeric strings as Lua does.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(n) => Some(*n as f64),
        Value::Number(n) => Some(*n),
        Value::String(text) => text.to_str().ok()?.trim().parse().ok(),
        _ => None,
    }
}

fn number_arg(args: &[Value], index: usize, function: &str) -> Result<f64> {
    match args.get(index).and_then(number) {
        Some(n) => Ok(n),
        None => fail(format!(
            "bad argument #{} to '{function}' (number expected, got {})",
            index + 1,
            args.get(index).map_or("no value", type_name)
        )),
    }
}

/// A whole-number result as a Lua number.
fn whole(n: f64) -> Value {
    // Negative zero must stay a float to keep its sign.
    let negative_zero = n == 0.0 && n.is_sign_negative();
    if n.fract() == 0.0 && n.abs() < 9.0e15 && !negative_zero {
        Value::Integer(n as i64)
    } else {
        Value::Number(n)
    }
}

/// Formats a number as C's `%.14g` does, which is how Lua 5.1 and the JSON
/// encoder print numbers: up to 14 significant digits, no trailing zeros,
/// scientific notation for very large or small magnitudes.
pub(crate) fn format_g14(n: f64) -> String {
    if n == 0.0 {
        return if n.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    if n.is_nan() {
        return if n.is_sign_negative() { "-nan" } else { "nan" }.to_owned();
    }
    if n.is_infinite() {
        return if n > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    // 14 significant digits: one before the point and 13 after.
    let scientific = format!("{n:.13e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always contains an exponent");
    let exponent: i32 = exponent.parse().expect("`{:e}` exponent is an integer");
    let trim = |text: &str| -> String {
        if text.contains('.') {
            text.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            text.to_owned()
        }
    };
    if (-4..14).contains(&exponent) {
        trim(&format!("{n:.*}", (13 - exponent) as usize))
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    }
}

/// Installs every library into the interpreter's globals.
pub(crate) fn install(lua: &Lua) -> Result<()> {
    let globals = lua.globals();
    globals.raw_set("bit", bit::library(lua)?)?;
    globals.raw_set("cjson", cjson::library(lua)?)?;
    globals.raw_set("struct", structs::library(lua)?)?;
    globals.raw_set("cmsgpack", cmsgpack::library(lua)?)?;
    random::install(lua)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// math.random
// ---------------------------------------------------------------------------

mod random {
    use super::*;

    /// The 48-bit linear congruential generator behind C's `lrand48`, which
    /// Redis uses so that scripts see the same sequence everywhere.
    fn next(state: &AtomicU64) -> f64 {
        const A: u64 = 0x5_DEEC_E66D;
        const C: u64 = 0xB;
        const MAX: u64 = i32::MAX as u64;
        let advanced = (state
            .load(Ordering::Relaxed)
            .wrapping_mul(A)
            .wrapping_add(C))
            & ((1 << 48) - 1);
        state.store(advanced, Ordering::Relaxed);
        ((advanced >> 17) % MAX) as f64 / MAX as f64
    }

    fn seed(state: &AtomicU64, seed: u32) {
        state.store((u64::from(seed) << 16) | 0x330E, Ordering::Relaxed);
    }

    pub(super) fn install(lua: &Lua) -> Result<()> {
        let math: Table = lua.globals().raw_get("math")?;
        let state = Arc::new(AtomicU64::new(0));
        seed(&state, 0);

        let shared = Arc::clone(&state);
        math.raw_set(
            "random",
            located(lua, move |_, args: Variadic<Value>| {
                let r = next(&shared);
                let bound = |index: usize| number_arg(&args, index, "random").map(f64::floor);
                match args.len() {
                    0 => Ok(Value::Number(r)),
                    1 => {
                        let upper = bound(0)?;
                        if upper < 1.0 {
                            return fail("bad argument #1 to 'random' (interval is empty)");
                        }
                        Ok(whole((r * upper).floor() + 1.0))
                    }
                    2 => {
                        let (lower, upper) = (bound(0)?, bound(1)?);
                        if lower > upper {
                            return fail("bad argument #2 to 'random' (interval is empty)");
                        }
                        Ok(whole((r * (upper - lower + 1.0)).floor() + lower))
                    }
                    _ => fail("wrong number of arguments"),
                }
            })?,
        )?;
        math.raw_set(
            "randomseed",
            located(lua, move |_, args: Variadic<Value>| {
                seed(&state, number_arg(&args, 0, "randomseed")? as i64 as u32);
                Ok(())
            })?,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// bit
// ---------------------------------------------------------------------------

mod bit {
    use super::*;

    /// Reduces a Lua number to 32 bits the way the `bit` library does:
    /// rounded to an integer, then wrapped.
    fn to_bits(args: &[Value], index: usize, function: &str) -> Result<i32> {
        let n = number_arg(args, index, function)?;
        Ok(n.round_ties_even().rem_euclid(4_294_967_296.0) as u64 as u32 as i32)
    }

    fn fold(lua: &Lua, name: &'static str, op: fn(i32, i32) -> i32) -> Result<mlua::Function> {
        located(lua, move |_, args: Variadic<Value>| {
            let mut result = to_bits(&args, 0, name)?;
            for index in 1..args.len() {
                result = op(result, to_bits(&args, index, name)?);
            }
            Ok(i64::from(result))
        })
    }

    fn shift(lua: &Lua, name: &'static str, op: fn(i32, u32) -> i32) -> Result<mlua::Function> {
        located(lua, move |_, args: Variadic<Value>| {
            let value = to_bits(&args, 0, name)?;
            let by = to_bits(&args, 1, name)? as u32 & 31;
            Ok(i64::from(op(value, by)))
        })
    }

    pub(super) fn library(lua: &Lua) -> Result<Table> {
        let bit = lua.create_table()?;
        bit.raw_set("band", fold(lua, "band", |a, b| a & b)?)?;
        bit.raw_set("bor", fold(lua, "bor", |a, b| a | b)?)?;
        bit.raw_set("bxor", fold(lua, "bxor", |a, b| a ^ b)?)?;
        bit.raw_set(
            "lshift",
            shift(lua, "lshift", |v, n| ((v as u32) << n) as i32)?,
        )?;
        bit.raw_set(
            "rshift",
            shift(lua, "rshift", |v, n| ((v as u32) >> n) as i32)?,
        )?;
        bit.raw_set("arshift", shift(lua, "arshift", |v, n| v >> n)?)?;
        bit.raw_set(
            "rol",
            shift(lua, "rol", |v, n| (v as u32).rotate_left(n) as i32)?,
        )?;
        bit.raw_set(
            "ror",
            shift(lua, "ror", |v, n| (v as u32).rotate_right(n) as i32)?,
        )?;
        bit.raw_set(
            "tobit",
            located(lua, |_, args: Variadic<Value>| {
                Ok(i64::from(to_bits(&args, 0, "tobit")?))
            })?,
        )?;
        bit.raw_set(
            "bnot",
            located(lua, |_, args: Variadic<Value>| {
                Ok(i64::from(!to_bits(&args, 0, "bnot")?))
            })?,
        )?;
        bit.raw_set(
            "bswap",
            located(lua, |_, args: Variadic<Value>| {
                Ok(i64::from(to_bits(&args, 0, "bswap")?.swap_bytes()))
            })?,
        )?;
        bit.raw_set(
            "tohex",
            located(lua, |_, args: Variadic<Value>| {
                let value = to_bits(&args, 0, "tohex")? as u32;
                // A negative width asks for upper-case digits.
                let width = if args.len() > 1 {
                    to_bits(&args, 1, "tohex")?
                } else {
                    8
                };
                let digits = (width.unsigned_abs() as usize).min(8);
                let full = if width < 0 {
                    format!("{value:08X}")
                } else {
                    format!("{value:08x}")
                };
                Ok(full[8 - digits..].to_owned())
            })?,
        )?;
        Ok(bit)
    }
}

// ---------------------------------------------------------------------------
// cjson
// ---------------------------------------------------------------------------

mod cjson {
    use super::*;

    const MAX_DEPTH: usize = 1000;
    /// A table with integer keys is an array unless it is this sparse.
    const SPARSE_RATIO: usize = 2;
    const SPARSE_SAFE: usize = 10;

    pub(super) fn library(lua: &Lua) -> Result<Table> {
        let cjson = lua.create_table()?;
        cjson.raw_set(
            "encode",
            located(lua, |lua, args: Variadic<Value>| {
                let [value] = &args[..] else {
                    return fail("bad argument #1 to 'encode' (expected 1 argument)");
                };
                let mut out = Vec::new();
                encode(value, 0, &mut out)?;
                lua.create_string(out)
            })?,
        )?;
        cjson.raw_set(
            "decode",
            located(lua, |lua, args: Variadic<Value>| {
                let [value] = &args[..] else {
                    return fail("bad argument #1 to 'decode' (expected 1 argument)");
                };
                // A number is accepted as its text, as Lua functions do.
                let text = match value {
                    Value::String(text) => text.clone(),
                    Value::Integer(_) | Value::Number(_) => {
                        match lua.coerce_string(value.clone())? {
                            Some(text) => text,
                            None => return fail("bad argument #1 to 'decode' (string expected)"),
                        }
                    }
                    other => {
                        return fail(format!(
                            "bad argument #1 to 'decode' (string expected, got {})",
                            type_name(other)
                        ))
                    }
                };
                let bytes = text.as_bytes();
                let mut parser = Parser {
                    lua,
                    bytes: &bytes,
                    at: 0,
                };
                let value = parser.value(0)?;
                let (token, at) = parser.peek();
                if token != "T_END" {
                    return expected("the end", token, at);
                }
                Ok(value)
            })?,
        )?;
        // JSON null, which is neither nil nor false.
        cjson.raw_set("null", Value::NULL)?;
        Ok(cjson)
    }

    /// The largest index if `table` is an array (every key a positive whole
    /// number); `None` if it must be encoded as an object.
    fn array_length(table: &Table) -> Result<Option<usize>> {
        let (mut max, mut items) = (0usize, 0usize);
        for pair in table.pairs::<Value, Value>() {
            let (key, _) = pair?;
            let index = match key {
                Value::Integer(n) if n >= 1 => n as usize,
                Value::Number(n) if n >= 1.0 && n.fract() == 0.0 => n as usize,
                _ => return Ok(None),
            };
            max = max.max(index);
            items += 1;
        }
        if max > items * SPARSE_RATIO && max > SPARSE_SAFE {
            return fail("Cannot serialise table: excessively sparse array");
        }
        Ok(Some(max))
    }

    fn encode_string(bytes: &[u8], out: &mut Vec<u8>) {
        out.push(b'"');
        for &byte in bytes {
            match byte {
                b'"' => out.extend_from_slice(b"\\\""),
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'/' => out.extend_from_slice(b"\\/"),
                0x08 => out.extend_from_slice(b"\\b"),
                0x09 => out.extend_from_slice(b"\\t"),
                0x0A => out.extend_from_slice(b"\\n"),
                0x0C => out.extend_from_slice(b"\\f"),
                0x0D => out.extend_from_slice(b"\\r"),
                0..=0x1F | 0x7F => out.extend_from_slice(format!("\\u{byte:04x}").as_bytes()),
                _ => out.push(byte),
            }
        }
        out.push(b'"');
    }

    fn encode_number(n: f64, out: &mut Vec<u8>) -> Result<()> {
        if !n.is_finite() {
            return fail("Cannot serialise number: must not be NaN or Inf");
        }
        out.extend_from_slice(format_g14(n).as_bytes());
        Ok(())
    }

    fn encode(value: &Value, depth: usize, out: &mut Vec<u8>) -> Result<()> {
        match value {
            Value::Nil => out.extend_from_slice(b"null"),
            Value::LightUserData(pointer) if pointer.0.is_null() => {
                out.extend_from_slice(b"null");
            }
            Value::Boolean(true) => out.extend_from_slice(b"true"),
            Value::Boolean(false) => out.extend_from_slice(b"false"),
            Value::Integer(n) => encode_number(*n as f64, out)?,
            Value::Number(n) => encode_number(*n, out)?,
            Value::String(text) => encode_string(&text.as_bytes(), out),
            Value::Table(table) => {
                let depth = depth + 1;
                if depth > MAX_DEPTH {
                    return fail(format!("Cannot serialise, excessive nesting ({depth})"));
                }
                match array_length(table)? {
                    Some(length) if length > 0 => {
                        out.push(b'[');
                        for index in 1..=length {
                            if index > 1 {
                                out.push(b',');
                            }
                            encode(&table.raw_get::<Value>(index)?, depth, out)?;
                        }
                        out.push(b']');
                    }
                    _ => {
                        out.push(b'{');
                        let mut first = true;
                        for pair in table.pairs::<Value, Value>() {
                            let (key, item) = pair?;
                            if !std::mem::take(&mut first) {
                                out.push(b',');
                            }
                            match &key {
                                Value::String(text) => encode_string(&text.as_bytes(), out),
                                Value::Integer(n) => {
                                    encode_string(format_g14(*n as f64).as_bytes(), out);
                                }
                                Value::Number(n) => encode_string(format_g14(*n).as_bytes(), out),
                                other => {
                                    return fail(format!(
                                        "Cannot serialise {}: table key must be a number or string",
                                        type_name(other)
                                    ))
                                }
                            }
                            out.push(b':');
                            encode(&item, depth, out)?;
                        }
                        out.push(b'}');
                    }
                }
            }
            other => {
                return fail(format!(
                    "Cannot serialise {}: type not supported",
                    type_name(other)
                ))
            }
        }
        Ok(())
    }

    fn expected<T>(what: &str, found: &str, at: usize) -> Result<T> {
        fail(format!(
            "Expected {what} but found {found} at character {}",
            at + 1
        ))
    }

    struct Parser<'a> {
        lua: &'a Lua,
        bytes: &'a [u8],
        at: usize,
    }

    impl Parser<'_> {
        /// The kind of the next token and where it starts, without
        /// consuming it.
        fn peek(&mut self) -> (&'static str, usize) {
            while self
                .bytes
                .get(self.at)
                .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
            {
                self.at += 1;
            }
            let rest = &self.bytes[self.at..];
            let kind = match rest.first() {
                None => "T_END",
                Some(b'{') => "T_OBJ_BEGIN",
                Some(b'}') => "T_OBJ_END",
                Some(b'[') => "T_ARR_BEGIN",
                Some(b']') => "T_ARR_END",
                Some(b':') => "T_COLON",
                Some(b',') => "T_COMMA",
                Some(b'"') => "T_STRING",
                Some(b'-' | b'0'..=b'9') => "T_NUMBER",
                _ if rest.starts_with(b"true") || rest.starts_with(b"false") => "T_BOOLEAN",
                _ if rest.starts_with(b"null") => "T_NULL",
                _ => "invalid token",
            };
            (kind, self.at)
        }

        fn value(&mut self, depth: usize) -> Result<Value> {
            let (kind, at) = self.peek();
            match kind {
                "T_STRING" => Ok(Value::String(self.string()?)),
                "T_NUMBER" => self.number(),
                "T_BOOLEAN" => {
                    let flag = self.bytes[at..].starts_with(b"true");
                    self.at += if flag { 4 } else { 5 };
                    Ok(Value::Boolean(flag))
                }
                "T_NULL" => {
                    self.at += 4;
                    Ok(Value::NULL)
                }
                "T_OBJ_BEGIN" | "T_ARR_BEGIN" => {
                    let depth = depth + 1;
                    if depth > MAX_DEPTH {
                        return fail(format!(
                            "Found too many nested data structures ({depth}) at character {}",
                            at + 1
                        ));
                    }
                    self.at += 1;
                    if kind == "T_OBJ_BEGIN" {
                        self.object(depth)
                    } else {
                        self.array(depth)
                    }
                }
                other => expected("value", other, at),
            }
        }

        fn object(&mut self, depth: usize) -> Result<Value> {
            let table = self.lua.create_table()?;
            if self.peek().0 == "T_OBJ_END" {
                self.at += 1;
                return Ok(Value::Table(table));
            }
            loop {
                let (kind, at) = self.peek();
                if kind != "T_STRING" {
                    return expected("object key string", kind, at);
                }
                let key = self.string()?;
                let (kind, at) = self.peek();
                if kind != "T_COLON" {
                    return expected("colon", kind, at);
                }
                self.at += 1;
                table.raw_set(key, self.value(depth)?)?;
                let (kind, at) = self.peek();
                self.at += 1;
                match kind {
                    "T_OBJ_END" => return Ok(Value::Table(table)),
                    "T_COMMA" => {}
                    other => return expected("comma or object end", other, at),
                }
            }
        }

        fn array(&mut self, depth: usize) -> Result<Value> {
            let table = self.lua.create_table()?;
            if self.peek().0 == "T_ARR_END" {
                self.at += 1;
                return Ok(Value::Table(table));
            }
            for index in 1.. {
                table.raw_seti(index, self.value(depth)?)?;
                let (kind, at) = self.peek();
                self.at += 1;
                match kind {
                    "T_ARR_END" => break,
                    "T_COMMA" => {}
                    other => return expected("comma or array end", other, at),
                }
            }
            Ok(Value::Table(table))
        }

        /// Reads the longest prefix that is a JSON number.
        fn number(&mut self) -> Result<Value> {
            let start = self.at;
            let digits = |parser: &mut Self| {
                let from = parser.at;
                while parser.bytes.get(parser.at).is_some_and(u8::is_ascii_digit) {
                    parser.at += 1;
                }
                parser.at > from
            };
            if self.bytes.get(self.at) == Some(&b'-') {
                self.at += 1;
            }
            if !digits(self) {
                return expected("value", "invalid number", start);
            }
            if self.bytes.get(self.at) == Some(&b'.') {
                let point = self.at;
                self.at += 1;
                if !digits(self) {
                    self.at = point;
                }
            }
            if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
                let exponent = self.at;
                self.at += 1;
                if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                    self.at += 1;
                }
                if !digits(self) {
                    self.at = exponent;
                }
            }
            let text = std::str::from_utf8(&self.bytes[start..self.at]).unwrap_or_default();
            match text.parse::<f64>() {
                Ok(n) => Ok(whole(n)),
                Err(_) => expected("value", "invalid number", start),
            }
        }

        fn hex4(&mut self) -> Option<u32> {
            let digits = self.bytes.get(self.at..self.at + 4)?;
            let code = u32::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()?;
            self.at += 4;
            Some(code)
        }

        fn string(&mut self) -> Result<mlua::LuaString> {
            self.at += 1; // the opening quote
            let mut out = Vec::new();
            loop {
                let Some(&byte) = self.bytes.get(self.at) else {
                    return expected("value", "unexpected end of string", self.at);
                };
                self.at += 1;
                match byte {
                    b'"' => break,
                    b'\\' => {
                        // Errors point at the backslash.
                        let escape_at = self.at - 1;
                        let bad_escape = || expected("value", "invalid escape code", escape_at);
                        let bad_unicode =
                            || expected("value", "invalid unicode escape code", escape_at);
                        let Some(&escape) = self.bytes.get(self.at) else {
                            return bad_escape();
                        };
                        self.at += 1;
                        match escape {
                            b'"' | b'\\' | b'/' => out.push(escape),
                            b'b' => out.push(0x08),
                            b't' => out.push(0x09),
                            b'n' => out.push(0x0A),
                            b'f' => out.push(0x0C),
                            b'r' => out.push(0x0D),
                            b'u' => {
                                let Some(mut code) = self.hex4() else {
                                    return bad_unicode();
                                };
                                // A surrogate pair encodes one character.
                                if (0xD800..0xDC00).contains(&code) {
                                    if !self.bytes[self.at..].starts_with(b"\\u") {
                                        return bad_unicode();
                                    }
                                    self.at += 2;
                                    match self.hex4() {
                                        Some(low) if (0xDC00..0xE000).contains(&low) => {
                                            code =
                                                0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                                        }
                                        _ => return bad_unicode(),
                                    }
                                }
                                let Some(character) = char::from_u32(code) else {
                                    return bad_unicode();
                                };
                                let mut buffer = [0u8; 4];
                                out.extend_from_slice(
                                    character.encode_utf8(&mut buffer).as_bytes(),
                                );
                            }
                            _ => return bad_escape(),
                        }
                    }
                    _ => out.push(byte),
                }
            }
            self.lua.create_string(out)
        }
    }
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

mod structs {
    use super::*;

    /// Largest integer the format can describe, in bytes.
    const MAX_INT_SIZE: usize = 32;
    const DEFAULT_ALIGN: usize = 8;

    struct Layout {
        little_endian: bool,
        /// Fields are padded to a multiple of their size, up to this.
        align: usize,
    }

    /// One item of a format string.
    struct Item {
        option: u8,
        size: usize,
    }

    /// Walks a format string, applying layout controls and yielding the
    /// items that hold data.
    struct Format<'a> {
        bytes: &'a [u8],
        at: usize,
        layout: Layout,
        /// The library function being served, for error messages.
        function: &'static str,
    }

    impl<'a> Format<'a> {
        fn new(bytes: &'a [u8], function: &'static str) -> Self {
            Format {
                bytes,
                function,
                at: 0,
                layout: Layout {
                    little_endian: true,
                    align: 1,
                },
            }
        }

        /// An optional decimal number following an option.
        fn count(&mut self, default: usize) -> usize {
            let start = self.at;
            let mut value = 0usize;
            while let Some(digit) = self.bytes.get(self.at).filter(|byte| byte.is_ascii_digit()) {
                value = value
                    .saturating_mul(10)
                    .saturating_add(usize::from(digit - b'0'));
                self.at += 1;
            }
            if self.at == start {
                default
            } else {
                value
            }
        }

        fn next(&mut self) -> Result<Option<Item>> {
            loop {
                let Some(&option) = self.bytes.get(self.at) else {
                    return Ok(None);
                };
                self.at += 1;
                let size = match option {
                    b' ' => continue,
                    b'>' => {
                        self.layout.little_endian = false;
                        continue;
                    }
                    b'<' => {
                        self.layout.little_endian = true;
                        continue;
                    }
                    b'!' => {
                        let align = self.count(DEFAULT_ALIGN);
                        if !align.is_power_of_two() {
                            return fail(format!("alignment {align} is not a power of 2"));
                        }
                        self.layout.align = align;
                        continue;
                    }
                    b'b' | b'B' | b'x' => 1,
                    b'h' | b'H' => 2,
                    b'l' | b'L' | b'T' | b'd' => 8,
                    b'f' => 4,
                    b'i' | b'I' => {
                        let size = self.count(4);
                        if size > MAX_INT_SIZE {
                            return fail(format!(
                                "integral size {size} is larger than limit of {MAX_INT_SIZE}"
                            ));
                        }
                        size
                    }
                    b'c' => self.count(1),
                    b's' => 0,
                    // `struct.size` counts an unknown option as nothing.
                    _ if self.function == "size" => continue,
                    other => {
                        return fail(format!(
                            "bad argument #1 to '{}' (invalid format option '{}')",
                            self.function, other as char
                        ));
                    }
                };
                return Ok(Some(Item { option, size }));
            }
        }

        /// Padding needed before an item so that it starts aligned.
        fn padding(&self, offset: usize, item: &Item) -> usize {
            let align = item.size.min(self.layout.align);
            if align <= 1 || matches!(item.option, b'c' | b's' | b'x') {
                return 0;
            }
            (align - (offset & (align - 1))) & (align - 1)
        }
    }

    fn is_integer(option: u8) -> bool {
        matches!(
            option,
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I'
        )
    }

    /// A numeric argument of `pack`. A missing one is reported as nil.
    fn pack_number(args: &[Value], index: usize) -> Result<f64> {
        match args.get(index).and_then(number) {
            Some(n) => Ok(n),
            None => fail(format!(
                "bad argument #{} to 'pack' (number expected, got {})",
                index + 1,
                args.get(index).map_or("nil", type_name)
            )),
        }
    }

    fn pack(lua: &Lua, args: Variadic<Value>) -> Result<mlua::LuaString> {
        let Some(Value::String(format)) = args.first() else {
            return fail("bad argument #1 to 'pack' (string expected)");
        };
        let format = format.as_bytes();
        let mut format = Format::new(&format, "pack");
        let mut out: Vec<u8> = Vec::new();
        let mut index = 1;

        while let Some(item) = format.next()? {
            out.resize(out.len() + format.padding(out.len(), &item), 0);
            let little = format.layout.little_endian;
            let put = |bytes: &mut [u8], out: &mut Vec<u8>| {
                if !little {
                    bytes.reverse();
                }
                out.extend_from_slice(bytes);
            };
            match item.option {
                b'x' => out.push(0),
                option if is_integer(option) => {
                    let n = pack_number(&args, index)?;
                    index += 1;
                    let value = if n < 0.0 { n as i64 as u64 } else { n as u64 };
                    // Little-endian bytes; anything beyond eight is zero.
                    let mut bytes = vec![0u8; item.size];
                    for (slot, byte) in bytes.iter_mut().zip(value.to_le_bytes()) {
                        *slot = byte;
                    }
                    put(&mut bytes, &mut out);
                }
                b'f' => {
                    let n = pack_number(&args, index)?;
                    index += 1;
                    put(&mut (n as f32).to_le_bytes(), &mut out);
                }
                b'd' => {
                    let n = pack_number(&args, index)?;
                    index += 1;
                    put(&mut n.to_le_bytes(), &mut out);
                }
                b'c' | b's' => {
                    let text = match args.get(index) {
                        Some(Value::String(text)) => text.as_bytes().to_vec(),
                        Some(value @ (Value::Integer(_) | Value::Number(_))) => lua
                            .coerce_string(value.clone())?
                            .map(|text| text.as_bytes().to_vec())
                            .unwrap_or_default(),
                        other => {
                            return fail(format!(
                                "bad argument #{} to 'pack' (string expected, got {})",
                                index + 1,
                                other.map_or("no value", type_name)
                            ))
                        }
                    };
                    index += 1;
                    if item.option == b's' {
                        out.extend_from_slice(&text);
                        out.push(0);
                    } else {
                        // `c0` writes the whole string.
                        let size = if item.size == 0 {
                            text.len()
                        } else {
                            item.size
                        };
                        if text.len() < size {
                            return fail(format!(
                                "bad argument #{} to 'pack' (string too short)",
                                index + 1
                            ));
                        }
                        out.extend_from_slice(&text[..size]);
                    }
                }
                _ => unreachable!("Format::next yields only data options"),
            }
        }
        lua.create_string(out)
    }

    fn unpack(lua: &Lua, args: Variadic<Value>) -> Result<Variadic<Value>> {
        let (Some(Value::String(format)), Some(Value::String(data))) = (args.first(), args.get(1))
        else {
            return fail("bad argument to 'unpack' (string expected)");
        };
        let format = format.as_bytes();
        let data = data.as_bytes();
        let mut offset = match args.get(2) {
            None | Some(Value::Nil) => 0,
            Some(_) => {
                let position = number_arg(&args, 2, "unpack")?;
                if position < 1.0 {
                    return fail("bad argument #3 to 'unpack' (offset must be 1 or greater)");
                }
                position as usize - 1
            }
        };
        let mut format = Format::new(&format, "unpack");
        let mut values: Vec<Value> = Vec::new();
        let short = || fail::<()>("bad argument #2 to 'unpack' (data string too short)");

        while let Some(item) = format.next()? {
            offset += format.padding(offset, &item);
            let mut size = item.size;
            if item.option == b'c' && size == 0 {
                // `c0` takes its length from the value unpacked just before.
                match values.pop().as_ref().and_then(number) {
                    Some(length) => size = length as usize,
                    None => return fail("format 'c0' needs a previous size"),
                }
            }
            if item.option == b's' {
                let Some(length) = data
                    .get(offset.min(data.len())..)
                    .and_then(|rest| rest.iter().position(|&byte| byte == 0))
                else {
                    return fail("unfinished string in data");
                };
                values.push(Value::String(
                    lua.create_string(&data[offset..offset + length])?,
                ));
                offset += length + 1;
                continue;
            }
            if size > data.len() || offset > data.len() - size {
                short()?;
            }
            let mut field = data[offset..offset + size].to_vec();
            offset += size;
            if !format.layout.little_endian {
                field.reverse();
            }
            match item.option {
                b'x' => {}
                b'c' => values.push(Value::String(
                    lua.create_string(&data[offset - size..offset])?,
                )),
                b'f' => {
                    let bytes: [u8; 4] = field[..].try_into().unwrap_or_default();
                    values.push(Value::Number(f64::from(f32::from_le_bytes(bytes))));
                }
                b'd' => {
                    let bytes: [u8; 8] = field[..].try_into().unwrap_or_default();
                    values.push(whole(f64::from_le_bytes(bytes)));
                }
                option => {
                    let signed = option.is_ascii_lowercase();
                    let negative = signed && field.last().is_some_and(|byte| byte & 0x80 != 0);
                    let mut low = [if negative { 0xFF } else { 0 }; 8];
                    for (slot, byte) in low.iter_mut().zip(&field) {
                        *slot = *byte;
                    }
                    values.push(whole(if signed {
                        i64::from_le_bytes(low) as f64
                    } else {
                        u64::from_le_bytes(low) as f64
                    }));
                }
            }
        }
        values.push(Value::Integer(offset as i64 + 1));
        Ok(Variadic::from_iter(values))
    }

    fn size(_: &Lua, args: Variadic<Value>) -> Result<i64> {
        let Some(Value::String(format)) = args.first() else {
            return fail("bad argument #1 to 'size' (string expected)");
        };
        let format = format.as_bytes();
        let mut format = Format::new(&format, "size");
        let mut total = 0usize;
        while let Some(item) = format.next()? {
            if item.option == b's' {
                return fail("bad argument #1 to 'size' (option 's' has no fixed size)");
            }
            if item.option == b'c' && item.size == 0 {
                return fail("bad argument #1 to 'size' (option 'c0' has no fixed size)");
            }
            total += format.padding(total, &item) + item.size;
        }
        Ok(total as i64)
    }

    pub(super) fn library(lua: &Lua) -> Result<Table> {
        let library = lua.create_table()?;
        library.raw_set("pack", located(lua, pack)?)?;
        library.raw_set("unpack", located(lua, unpack)?)?;
        library.raw_set("size", located(lua, size)?)?;
        Ok(library)
    }
}

// ---------------------------------------------------------------------------
// cmsgpack
// ---------------------------------------------------------------------------

mod cmsgpack {
    use super::*;

    /// Tables nested deeper than this are encoded as nil.
    const MAX_NESTING: usize = 16;

    fn encode_int(n: i64, out: &mut Vec<u8>) {
        if n >= 0 {
            match n {
                0..=127 => out.push(n as u8),
                128..=0xFF => out.extend_from_slice(&[0xCC, n as u8]),
                0x100..=0xFFFF => {
                    out.push(0xCD);
                    out.extend_from_slice(&(n as u16).to_be_bytes());
                }
                0x1_0000..=0xFFFF_FFFF => {
                    out.push(0xCE);
                    out.extend_from_slice(&(n as u32).to_be_bytes());
                }
                _ => {
                    out.push(0xCF);
                    out.extend_from_slice(&(n as u64).to_be_bytes());
                }
            }
        } else {
            match n {
                -32..=-1 => out.push(n as u8),
                -128..=-33 => out.extend_from_slice(&[0xD0, n as u8]),
                -32_768..=-129 => {
                    out.push(0xD1);
                    out.extend_from_slice(&(n as i16).to_be_bytes());
                }
                -2_147_483_648..=-32_769 => {
                    out.push(0xD2);
                    out.extend_from_slice(&(n as i32).to_be_bytes());
                }
                _ => {
                    out.push(0xD3);
                    out.extend_from_slice(&n.to_be_bytes());
                }
            }
        }
    }

    fn encode_number(n: f64, out: &mut Vec<u8>) {
        if n.fract() == 0.0 && n.abs() < 9.2e18 {
            encode_int(n as i64, out);
        } else if f64::from(n as f32) == n {
            out.push(0xCA);
            out.extend_from_slice(&(n as f32).to_be_bytes());
        } else {
            out.push(0xCB);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }

    /// Writes a length header: `small | len` if it fits in `small_bits`
    /// bits, otherwise one of the wider forms.
    fn header(out: &mut Vec<u8>, len: usize, small: (u8, usize), wide: &[(u8, usize)]) {
        if len <= small.1 {
            out.push(small.0 | len as u8);
            return;
        }
        for &(tag, bytes) in wide {
            if bytes == 4 || len < 1 << (bytes * 8) {
                out.push(tag);
                out.extend_from_slice(&(len as u32).to_be_bytes()[4 - bytes..]);
                return;
            }
        }
    }

    /// The length if `table` is a sequence 1..n with no other keys.
    fn sequence_length(table: &Table) -> Result<Option<usize>> {
        let (mut max, mut count) = (0usize, 0usize);
        for pair in table.pairs::<Value, Value>() {
            let (key, _) = pair?;
            let index = match key {
                Value::Integer(n) if n >= 1 => n as usize,
                Value::Number(n) if n >= 1.0 && n.fract() == 0.0 => n as usize,
                _ => return Ok(None),
            };
            max = max.max(index);
            count += 1;
        }
        Ok((max == count).then_some(count))
    }

    fn encode(value: &Value, level: usize, out: &mut Vec<u8>) -> Result<()> {
        match value {
            Value::Boolean(true) => out.push(0xC3),
            Value::Boolean(false) => out.push(0xC2),
            Value::Integer(n) => encode_int(*n, out),
            Value::Number(n) => encode_number(*n, out),
            Value::String(text) => {
                let bytes = text.as_bytes();
                header(
                    out,
                    bytes.len(),
                    (0xA0, 31),
                    &[(0xD9, 1), (0xDA, 2), (0xDB, 4)],
                );
                out.extend_from_slice(&bytes);
            }
            Value::Table(table) if level < MAX_NESTING => match sequence_length(table)? {
                Some(length) => {
                    header(out, length, (0x90, 15), &[(0xDC, 2), (0xDD, 4)]);
                    for index in 1..=length {
                        encode(&table.raw_get::<Value>(index)?, level + 1, out)?;
                    }
                }
                None => {
                    let pairs: Vec<(Value, Value)> =
                        table.pairs::<Value, Value>().collect::<Result<_>>()?;
                    header(out, pairs.len(), (0x80, 15), &[(0xDE, 2), (0xDF, 4)]);
                    for (key, item) in &pairs {
                        encode(key, level + 1, out)?;
                        encode(item, level + 1, out)?;
                    }
                }
            },
            // nil, tables nested too deeply, and anything MessagePack has
            // no representation for.
            _ => out.push(0xC0),
        }
        Ok(())
    }

    struct Reader<'a> {
        lua: &'a Lua,
        bytes: &'a [u8],
        at: usize,
    }

    impl Reader<'_> {
        fn take(&mut self, count: usize) -> Result<&[u8]> {
            let end = self
                .at
                .checked_add(count)
                .filter(|&end| end <= self.bytes.len());
            match end {
                Some(end) => {
                    let slice = &self.bytes[self.at..end];
                    self.at = end;
                    Ok(slice)
                }
                None => fail("Missing bytes in input."),
            }
        }

        fn unsigned(&mut self, bytes: usize) -> Result<u64> {
            Ok(self
                .take(bytes)?
                .iter()
                .fold(0u64, |value, &byte| (value << 8) | u64::from(byte)))
        }

        fn string(&mut self, length: usize) -> Result<Value> {
            let lua = self.lua;
            let bytes = self.take(length)?;
            Ok(Value::String(lua.create_string(bytes)?))
        }

        fn array(&mut self, length: usize) -> Result<Value> {
            let table = self.lua.create_table()?;
            for index in 1..=length {
                table.raw_seti(index, self.value()?)?;
            }
            Ok(Value::Table(table))
        }

        fn map(&mut self, length: usize) -> Result<Value> {
            let table = self.lua.create_table()?;
            for _ in 0..length {
                let key = self.value()?;
                let item = self.value()?;
                // A nil key cannot be stored in a table.
                if !key.is_nil() {
                    table.raw_set(key, item)?;
                }
            }
            Ok(Value::Table(table))
        }

        fn value(&mut self) -> Result<Value> {
            let tag = self.take(1)?[0];
            Ok(match tag {
                0x00..=0x7F => Value::Integer(i64::from(tag)),
                0x80..=0x8F => self.map(usize::from(tag & 0x0F))?,
                0x90..=0x9F => self.array(usize::from(tag & 0x0F))?,
                0xA0..=0xBF => self.string(usize::from(tag & 0x1F))?,
                0xC0 => Value::Nil,
                0xC2 => Value::Boolean(false),
                0xC3 => Value::Boolean(true),
                0xC4 | 0xD9 => {
                    let length = self.unsigned(1)? as usize;
                    self.string(length)?
                }
                0xC5 | 0xDA => {
                    let length = self.unsigned(2)? as usize;
                    self.string(length)?
                }
                0xC6 | 0xDB => {
                    let length = self.unsigned(4)? as usize;
                    self.string(length)?
                }
                0xCA => {
                    let bits = self.unsigned(4)? as u32;
                    whole(f64::from(f32::from_bits(bits)))
                }
                0xCB => whole(f64::from_bits(self.unsigned(8)?)),
                0xCC => Value::Integer(self.unsigned(1)? as i64),
                0xCD => Value::Integer(self.unsigned(2)? as i64),
                0xCE => Value::Integer(self.unsigned(4)? as i64),
                0xCF => whole(self.unsigned(8)? as f64),
                0xD0 => Value::Integer(i64::from(self.unsigned(1)? as u8 as i8)),
                0xD1 => Value::Integer(i64::from(self.unsigned(2)? as u16 as i16)),
                0xD2 => Value::Integer(i64::from(self.unsigned(4)? as u32 as i32)),
                0xD3 => whole(self.unsigned(8)? as i64 as f64),
                0xDC => {
                    let length = self.unsigned(2)? as usize;
                    self.array(length)?
                }
                0xDD => {
                    let length = self.unsigned(4)? as usize;
                    self.array(length)?
                }
                0xDE => {
                    let length = self.unsigned(2)? as usize;
                    self.map(length)?
                }
                0xDF => {
                    let length = self.unsigned(4)? as usize;
                    self.map(length)?
                }
                0xE0..=0xFF => Value::Integer(i64::from(tag as i8)),
                _ => return fail("Bad data format in input."),
            })
        }
    }

    /// Decodes up to `limit` values (all of them when zero) starting at
    /// `offset`. Returns the values and the offset of what is left, or -1 if
    /// everything was consumed.
    fn decode(
        lua: &Lua,
        args: &[Value],
        limit: usize,
        offset_arg: usize,
    ) -> Result<(Vec<Value>, i64)> {
        // A number is accepted as its text, as Lua functions do.
        let text = match args.first() {
            Some(Value::String(text)) => text.clone(),
            Some(value @ (Value::Integer(_) | Value::Number(_))) => {
                match lua.coerce_string(value.clone())? {
                    Some(text) => text,
                    None => return fail("bad argument #1 to 'unpack' (string expected)"),
                }
            }
            other => {
                return fail(format!(
                    "bad argument #1 to 'unpack' (string expected, got {})",
                    other.map_or("no value", type_name)
                ))
            }
        };
        let bytes = text.as_bytes();
        let offset = match args.get(offset_arg) {
            None | Some(Value::Nil) => 0,
            Some(_) => number_arg(args, offset_arg, "unpack")? as i64,
        };
        if offset < 0 || offset as usize > bytes.len() {
            return fail(format!(
                "Start offset {offset} greater than input length {}.",
                bytes.len()
            ));
        }
        let mut reader = Reader {
            lua,
            bytes: &bytes,
            at: offset as usize,
        };
        let mut values = Vec::new();
        while reader.at < bytes.len() && (limit == 0 || values.len() < limit) {
            values.push(reader.value()?);
        }
        let left = if reader.at < bytes.len() {
            reader.at as i64
        } else {
            -1
        };
        Ok((values, left))
    }

    pub(super) fn library(lua: &Lua) -> Result<Table> {
        let library = lua.create_table()?;
        library.raw_set(
            "pack",
            located(lua, |lua, args: Variadic<Value>| {
                if args.is_empty() {
                    return fail("bad argument #0 to 'pack' (MessagePack pack needs input.)");
                }
                let mut out = Vec::new();
                for value in args.iter() {
                    encode(value, 0, &mut out)?;
                }
                lua.create_string(out)
            })?,
        )?;
        library.raw_set(
            "unpack",
            located(lua, |lua, args: Variadic<Value>| {
                let (values, _) = decode(lua, &args, 0, usize::MAX)?;
                Ok(Variadic::from_iter(values))
            })?,
        )?;
        library.raw_set(
            "unpack_one",
            located(lua, |lua, args: Variadic<Value>| {
                let (mut values, left) = decode(lua, &args, 1, 1)?;
                values.insert(0, Value::Integer(left));
                Ok(Variadic::from_iter(values))
            })?,
        )?;
        library.raw_set(
            "unpack_limit",
            located(lua, |lua, args: Variadic<Value>| {
                let limit = number_arg(&args, 1, "unpack_limit")?.max(0.0) as usize;
                let (mut values, left) = decode(lua, &args, limit, 2)?;
                values.insert(0, Value::Integer(left));
                Ok(Variadic::from_iter(values))
            })?,
        )?;
        Ok(library)
    }
}

#[cfg(test)]
mod tests {
    use super::format_g14;

    #[test]
    fn numbers_print_as_c_does_with_14_significant_digits() {
        for (value, expected) in [
            (0.0, "0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (0.1, "0.1"),
            (1.0 / 3.0, "0.33333333333333"),
            (2.0 / 3.0, "0.66666666666667"),
            (100.0, "100"),
            (123456789012345.0, "1.2345678901234e+14"),
            (12345678901234.0, "12345678901234"),
            (1e14, "1e+14"),
            (1e15, "1e+15"),
            (1e100, "1e+100"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (2f64.powi(53), "9.007199254741e+15"),
            (-0.5, "-0.5"),
            (f64::INFINITY, "inf"),
        ] {
            assert_eq!(format_g14(value), expected, "{value}");
        }
    }
}
