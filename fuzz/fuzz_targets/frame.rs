#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rexyra_resp::{decode, encode, Frame, Protocol};

fn has_nan(frame: &Frame) -> bool {
    match frame {
        Frame::Double(d) => d.is_nan(),
        Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => items.iter().any(has_nan),
        Frame::Map(pairs) => pairs.iter().any(|(k, v)| has_nan(k) || has_nan(v)),
        _ => false,
    }
}

// Anything the decoder accepts must survive an encode/decode round trip.
fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let Ok(Some(frame)) = decode(&mut buf) else {
        return;
    };
    // NaN != NaN, so equality can't be checked for frames containing one.
    if has_nan(&frame) {
        return;
    }
    let mut wire = BytesMut::new();
    encode(&frame, Protocol::Resp3, &mut wire);
    let again = decode(&mut wire).expect("re-encoded frame must be valid");
    assert_eq!(again, Some(frame));
    assert!(wire.is_empty());
});
