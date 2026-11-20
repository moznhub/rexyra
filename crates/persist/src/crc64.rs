//! CRC-64 as Redis computes it (the "Jones" polynomial, reflected), used as
//! the checksum of RDB files and `DUMP` payloads.

/// The polynomial `0xad93d23594c935a9` with its bits reversed, for the
/// reflected (least-significant-bit-first) form of the algorithm.
const REFLECTED_POLYNOMIAL: u64 = 0x95AC_9329_AC4B_C9B5;

const TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ REFLECTED_POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

/// Continues a checksum over `data`. Start with zero.
pub fn update(mut crc: u64, data: &[u8]) -> u64 {
    for &byte in data {
        crc = TABLE[((crc ^ u64::from(byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::update;

    #[test]
    fn matches_the_redis_test_vectors() {
        assert_eq!(update(0, b""), 0);
        assert_eq!(update(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn can_be_computed_in_pieces() {
        let whole = update(0, b"hello world");
        assert_eq!(update(update(0, b"hello "), b"world"), whole);
    }
}
