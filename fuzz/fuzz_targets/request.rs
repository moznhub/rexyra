#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rexyra_resp::parse_request;

// The request parser faces untrusted network input. It must never panic, and
// must always either consume bytes or stop.
fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    loop {
        let before = buf.len();
        match parse_request(&mut buf) {
            Ok(Some(args)) => {
                assert!(!args.is_empty());
                assert!(buf.len() < before);
            }
            Ok(None) | Err(_) => break,
        }
    }
});
