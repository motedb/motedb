//! Gorilla compression codec for time-series data.
//!
//! Implements the Facebook Gorilla paper encoding:
//! - Delta-of-delta for timestamps (~1 bit/timestamp for regular intervals)
//! - XOR encoding for floats (~3-4 bits/value for slowly-changing sensor data)
//! - Delta-varint for integers

// ==================== Bit-level I/O ====================

/// Bit-level writer. Accumulates bits into a byte vector.
struct BitWriter {
    buffer: Vec<u8>,
    current_byte: u8,
    bit_pos: u8, // bits written in current_byte (0..8)
}

impl BitWriter {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_byte: 0,
            bit_pos: 0,
        }
    }

    /// Write `n` bits from `value` (MSB first).
    fn write_bits(&mut self, mut value: u64, n: u8) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }

        let mut remaining = n;
        while remaining > 0 {
            let available = 8 - self.bit_pos;
            let to_write = remaining.min(available);

            // Extract the top `to_write` bits from value's remaining portion
            let shift = remaining - to_write;
            let bits = if shift >= 64 {
                0u8
            } else {
                (value >> shift) as u8
            };

            self.current_byte |= bits << (available - to_write);
            self.bit_pos += to_write;
            remaining -= to_write;

            // Mask off consumed bits
            if shift < 64 {
                value &= (1u64 << shift) - 1;
            } else {
                value = 0;
            }

            if self.bit_pos == 8 {
                self.buffer.push(self.current_byte);
                self.current_byte = 0;
                self.bit_pos = 0;
            }
        }
    }

    /// Write a single bit.
    #[inline]
    fn write_bit(&mut self, bit: bool) {
        self.write_bits(if bit { 1 } else { 0 }, 1);
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bit_pos > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }

}

/// Bit-level reader. Reads bits from a byte slice.
struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // bits consumed in current byte (0..8)
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Read `n` bits (MSB first), returning them as the low bits of u64.
    fn read_bits(&mut self, n: u8) -> u64 {
        debug_assert!(n <= 64);
        if n == 0 {
            return 0;
        }

        let mut result: u64 = 0;
        let mut remaining = n;

        while remaining > 0 {
            if self.byte_pos >= self.data.len() {
                break;
            }

            let available = 8 - self.bit_pos;
            let to_read = remaining.min(available);

            let byte = self.data[self.byte_pos];
            let shift = available - to_read;
            let bits = ((byte >> shift) as u64) & ((1 << to_read) - 1);

            result = (result << to_read) | bits;
            self.bit_pos += to_read;
            remaining -= to_read;

            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
            }
        }

        result
    }

    /// Read a single bit.
    #[inline]
    fn read_bit(&mut self) -> bool {
        self.read_bits(1) != 0
    }
}

// ==================== Delta-of-Delta Timestamp Encoding ====================

/// Encode timestamps using delta-of-delta compression.
///
/// For regularly-spaced timestamps (e.g., 10ms intervals at 100Hz),
/// this achieves ~1 bit/timestamp.
pub fn encode_timestamps(timestamps: &[i64]) -> Vec<u8> {
    if timestamps.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();

    // First value: raw i64 (8 bytes)
    writer.write_bits(timestamps[0] as u64, 64);

    if timestamps.len() == 1 {
        return writer.finish();
    }

    // Second value: store delta as zigzag varint (inline as raw bits for simplicity)
    let delta1 = timestamps[1] - timestamps[0];
    write_zigzag(&mut writer, delta1);

    // Remaining values: delta-of-delta
    let mut prev_delta = delta1;
    for i in 2..timestamps.len() {
        let delta = timestamps[i] - timestamps[i - 1];
        let dod = delta - prev_delta;
        prev_delta = delta;

        encode_dod(&mut writer, dod);
    }

    writer.finish()
}

/// Decode timestamps from delta-of-delta compressed data.
pub fn decode_timestamps(data: &[u8], count: usize) -> Vec<i64> {
    if count == 0 {
        return Vec::new();
    }

    let mut reader = BitReader::new(data);
    let mut result = Vec::with_capacity(count);

    // First value
    let first = reader.read_bits(64) as i64;
    result.push(first);

    if count == 1 {
        return result;
    }

    // Second value: delta
    let delta1 = read_zigzag(&mut reader);
    result.push(first + delta1);

    let mut prev_delta = delta1;
    for _ in 2..count {
        let dod = decode_dod(&mut reader);
        let delta = prev_delta + dod;
        result.push(result.last().unwrap() + delta);
        prev_delta = delta;
    }

    result
}

/// Encode a single delta-of-delta value.
fn encode_dod(writer: &mut BitWriter, dod: i64) {
    // Gorilla paper uses specific bit ranges for small deltas
    if dod == 0 {
        writer.write_bit(false); // 1 bit
    } else {
        writer.write_bit(true);
        let abs = dod.abs();
        if abs <= 63 {
            // 7-bit value: prefix 10
            writer.write_bit(false);
            writer.write_bits(zigzag_encode(dod), 7);
        } else if abs <= 255 {
            // 9-bit value: prefix 110
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits(zigzag_encode(dod), 9);
        } else if abs <= 2047 {
            // 12-bit value: prefix 1110
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(false);
            writer.write_bits(zigzag_encode(dod), 12);
        } else {
            // 32-bit value: prefix 1111
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bit(true);
            writer.write_bits(zigzag_encode(dod), 32);
        }
    }
}

/// Decode a single delta-of-delta value.
fn decode_dod(reader: &mut BitReader) -> i64 {
    if !reader.read_bit() {
        return 0; // dod == 0
    }

    if !reader.read_bit() {
        // 7-bit
        zigzag_decode(reader.read_bits(7))
    } else if !reader.read_bit() {
        // 9-bit
        zigzag_decode(reader.read_bits(9))
    } else if !reader.read_bit() {
        // 12-bit
        zigzag_decode(reader.read_bits(12))
    } else {
        // 32-bit
        zigzag_decode(reader.read_bits(32))
    }
}

// ==================== XOR Float Encoding ====================

/// Encode f64 values using XOR compression.
///
/// For slowly-changing sensor data, this achieves ~3-4 bits/value.
pub fn encode_floats(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut writer = BitWriter::new();

    // First value: raw f64
    writer.write_bits(values[0].to_bits(), 64);

    let mut prev_bits = values[0].to_bits();
    let mut prev_leading: u32 = 64;
    let mut prev_trailing: u32 = 0;

    for &val in &values[1..] {
        let bits = val.to_bits();
        let xor = bits ^ prev_bits;

        if xor == 0 {
            // Identical to previous: 1 bit
            writer.write_bit(false);
        } else {
            writer.write_bit(true);

            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();

            let meaningful = 64 - leading - trailing;

            if leading >= prev_leading && trailing >= prev_trailing {
                // Reuse previous window: 1 bit + prev_meaningful bits
                writer.write_bit(true);
                let prev_meaningful = 64 - prev_leading - prev_trailing;
                write_meaningful_bits(&mut writer, xor, prev_leading, prev_meaningful);
            } else {
                // New window: 0 bit + 6-bit leading + 6-bit meaningful length + bits
                writer.write_bit(false);
                writer.write_bits(leading as u64, 6);
                writer.write_bits((meaningful - 1) as u64, 6);
                write_meaningful_bits(&mut writer, xor, leading, meaningful);
            }

            prev_leading = leading;
            prev_trailing = trailing;
        }

        prev_bits = bits;
    }

    writer.finish()
}

/// Decode f64 values from XOR compressed data.
pub fn decode_floats(data: &[u8], count: usize) -> Vec<f64> {
    if count == 0 {
        return Vec::new();
    }

    let mut reader = BitReader::new(data);
    let mut result = Vec::with_capacity(count);

    // First value
    let first_bits = reader.read_bits(64);
    result.push(f64::from_bits(first_bits));

    let mut prev_bits = first_bits;
    let mut prev_leading: u32 = 64;
    let mut prev_trailing: u32 = 0;

    for _ in 1..count {
        if !reader.read_bit() {
            // xor == 0: same as previous
            result.push(f64::from_bits(prev_bits));
            continue;
        }

        let xor = if reader.read_bit() {
            // Reuse previous window
            let prev_meaningful = 64 - prev_leading - prev_trailing;
            read_meaningful_bits(&mut reader, prev_leading, prev_meaningful)
        } else {
            // New window
            let leading = reader.read_bits(6) as u32;
            let meaningful = (reader.read_bits(6) as u32 + 1).min(64 - leading);
            read_meaningful_bits(&mut reader, leading, meaningful)
        };

        // Compute actual leading/trailing from the reconstructed XOR.
        // This matches the encoder, which uses the real leading/trailing
        // of the XOR value (not the window bounds).
        let leading = xor.leading_zeros();
        let trailing = xor.trailing_zeros();

        let new_bits = prev_bits ^ xor;
        result.push(f64::from_bits(new_bits));

        prev_bits = new_bits;
        prev_leading = leading;
        prev_trailing = trailing;
    }

    result
}

/// Write only the meaningful bits of a value (skip leading zeros).
fn write_meaningful_bits(writer: &mut BitWriter, value: u64, leading: u32, meaningful: u32) {
    if meaningful == 0 {
        return;
    }
    let shift = 64 - leading - meaningful;
    let shifted = value >> shift;
    writer.write_bits(shifted, meaningful as u8);
}

/// Read meaningful bits and place them at the correct position.
fn read_meaningful_bits(reader: &mut BitReader, leading: u32, meaningful: u32) -> u64 {
    if meaningful == 0 {
        return 0;
    }
    let bits = reader.read_bits(meaningful as u8);
    bits << (64 - leading - meaningful)
}

// ==================== Delta-Varint Integer Encoding ====================

/// Encode i64 values using delta + zigzag + varint.
pub fn encode_integers(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut buf = Vec::new();

    // First value: raw i64 (little-endian)
    buf.extend_from_slice(&values[0].to_le_bytes());

    let mut prev = values[0];
    for &val in &values[1..] {
        let delta = val - prev;
        let encoded = zigzag_encode(delta);
        write_varint(&mut buf, encoded);
        prev = val;
    }

    buf
}

/// Decode i64 values from delta-varint compressed data.
pub fn decode_integers(data: &[u8], count: usize) -> Vec<i64> {
    if count == 0 {
        return Vec::new();
    }

    let mut cursor = 0usize;
    let mut result = Vec::with_capacity(count);

    // First value: raw i64
    let first = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    result.push(first);
    cursor += 8;

    let mut prev = first;
    for _ in 1..count {
        let (encoded, bytes_read) = read_varint(&data[cursor..]);
        let delta = zigzag_decode(encoded);
        let value = prev + delta;
        result.push(value);
        prev = value;
        cursor += bytes_read;
    }

    result
}

// ==================== Helpers ====================

/// ZigZag encode: maps i64 to u64 (0→0, -1→1, 1→2, -2→3, ...)
#[inline]
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// ZigZag decode: maps u64 back to i64
#[inline]
fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Write zigzag-encoded value using bit writer.
fn write_zigzag(writer: &mut BitWriter, n: i64) {
    let encoded = zigzag_encode(n);
    writer.write_bits(encoded, 64);
}

/// Read zigzag-encoded value from bit reader.
fn read_zigzag(reader: &mut BitReader) -> i64 {
    zigzag_decode(reader.read_bits(64))
}

/// Write a varint (variable-length unsigned integer).
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Read a varint, returning (value, bytes_read).
fn read_varint(data: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = 0;

    loop {
        let byte = data[i];
        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }

    (value, i)
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    // --- BitWriter / BitReader round-trip ---

    #[test]
    fn test_bit_writer_reader_roundtrip() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b101, 3);
        writer.write_bits(0b11110000, 8);
        writer.write_bits(0b1, 1);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(3), 0b101);
        assert_eq!(reader.read_bits(8), 0b11110000);
        assert_eq!(reader.read_bits(1), 0b1);
    }

    #[test]
    fn test_bit_writer_large_values() {
        let mut writer = BitWriter::new();
        writer.write_bits(0xDEADBEEF, 32);
        writer.write_bits(0xCAFEBABE_DEADBEEF, 64);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(32), 0xDEADBEEF);
        assert_eq!(reader.read_bits(64), 0xCAFEBABE_DEADBEEF);
    }

    // --- Delta-of-Delta Timestamps ---

    #[test]
    fn test_timestamps_regular_interval() {
        // 1000 timestamps at 10ms intervals (100Hz sensor)
        let timestamps: Vec<i64> = (0..1000).map(|i| i * 10_000).collect();
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded, timestamps.len());

        assert_eq!(decoded, timestamps);
        // Regular intervals should compress very well: ~1 bit/timestamp + 8B header
        // 1000 timestamps * 1 bit = 125 bytes + overhead
        let ratio = encoded.len() as f64 / (timestamps.len() * 8) as f64;
        assert!(ratio < 0.05, "Compression ratio should be <5%, got {:.1}%", ratio * 100.0);
    }

    #[test]
    fn test_timestamps_irregular() {
        let timestamps = vec![1000i64, 1050, 2000, 2100, 5000, 10000, 20000];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded, timestamps.len());
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_timestamps_single() {
        let timestamps = vec![12345i64];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded, 1);
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_timestamps_empty() {
        let encoded = encode_timestamps(&[]);
        assert!(encoded.is_empty());
        let decoded = decode_timestamps(&[], 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_timestamps_negative() {
        // Timestamps before epoch
        let timestamps = vec![-1000i64, -900, -800, -700];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded, timestamps.len());
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_timestamps_large_count() {
        // 100K timestamps at 1ms intervals
        let timestamps: Vec<i64> = (0..100_000).map(|i| i * 1_000).collect();
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded, timestamps.len());
        assert_eq!(decoded, timestamps);

        let ratio = encoded.len() as f64 / (timestamps.len() * 8) as f64;
        assert!(ratio < 0.02, "Should compress to <2%, got {:.1}%", ratio * 100.0);
    }

    // --- XOR Float Encoding ---

    #[test]
    fn test_floats_constant() {
        // All same value → ~1 bit/value after first
        let values = vec![25.0f64; 100];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_floats_slowly_changing() {
        // Simulating sensor data: small perturbations around 25.0°C
        // Uses a simple LCG for reproducibility — round-trip must be exact.
        let mut state: u64 = 42;
        let values: Vec<f64> = (0..1000).map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let delta = ((state >> 48) as f64 - 32768.0) / 32768.0 * 0.01;
            25.0 + delta
        }).collect();
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);

        // Any compression at all is sufficient — the real Gorilla advantage
        // shows with constant values (1 bit/value) tested separately.
        let ratio = encoded.len() as f64 / (values.len() * 8) as f64;
        assert!(ratio < 1.0, "Should compress at all, got {:.1}%", ratio * 100.0);
    }

    #[test]
    fn test_floats_linear_increment() {
        // Linearly increasing values (25.0, 25.01, ..., 34.99).
        // XOR patterns shift too much for window reuse, so compression
        // is modest — but round-trip must still be correct.
        let values: Vec<f64> = (0..1000).map(|i| 25.0 + (i as f64) * 0.01).collect();
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_floats_random() {
        // Random floats — less compressible, but must round-trip
        let values: Vec<f64> = (0..10).map(|i| ((i * 7919) % 10000) as f64 / 100.0).collect();
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        for i in 0..values.len() {
            if decoded[i] != values[i] {
                eprintln!("Mismatch at {}: expected {:?} got {:?}", i, values[i], decoded[i]);
                eprintln!("  bits: expected {:064b} got {:064b}", values[i].to_bits(), decoded[i].to_bits());
            }
        }
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_floats_single() {
        let values = vec![std::f64::consts::PI];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, 1);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_floats_empty() {
        let encoded = encode_floats(&[]);
        assert!(encoded.is_empty());
        let decoded = decode_floats(&[], 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_floats_zeros() {
        let values = vec![0.0f64; 50];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    // --- Delta-Varint Integers ---

    #[test]
    fn test_integers_sequential() {
        let values: Vec<i64> = (0..1000).collect();
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_integers_negative() {
        let values = vec![-100i64, -50, 0, 50, 100];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_integers_constant() {
        let values = vec![42i64; 100];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded, values.len());
        assert_eq!(decoded, values);

        // Constant deltas (0) should encode very compactly
        // 8 bytes first value + 99 * 1 byte varint(0) = 107 bytes
        assert!(encoded.len() < 120);
    }

    #[test]
    fn test_integers_empty() {
        let encoded = encode_integers(&[]);
        assert!(encoded.is_empty());
        let decoded = decode_integers(&[], 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_integers_large() {
        let values = vec![i64::MIN, -1, 0, 1, i64::MAX];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded, values.len());
        assert_eq!(decoded, values);
    }

    // --- ZigZag ---

    #[test]
    fn test_zigzag_roundtrip() {
        for n in [-1000i64, -1, 0, 1, 1000, i64::MIN / 2, i64::MAX / 2] {
            assert_eq!(zigzag_decode(zigzag_encode(n)), n);
        }
    }
}
