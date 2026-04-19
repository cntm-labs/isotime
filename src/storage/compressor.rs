#[cfg(feature = "simd")]
use std::simd::{u64x4, Simd};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionPolicy {
    /// Minimal compression, maximum read/write speed.
    Fastest = 0,
    /// Good mix of speed and space saving.
    #[default]
    Balanced = 1,
    /// Aggressive de-duplication and heavy SIMD compression.
    ExtremeSpace = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None = 0,
    DeltaDelta = 1,
    BitPackedDelta = 2,
}

pub struct Compressor;

impl Compressor {
    pub fn compress(data: &[u8], policy: CompressionPolicy) -> (CompressionType, Vec<u8>) {
        if policy == CompressionPolicy::Fastest {
            return (CompressionType::None, data.to_vec());
        }

        // Only attempt DeltaDelta or BitPacked if we have enough 64-bit values
        if data.len() >= 32 && data.len().is_multiple_of(8) {
            if policy == CompressionPolicy::ExtremeSpace {
                return (
                    CompressionType::BitPackedDelta,
                    Self::compress_bitpacked(data),
                );
            }

            #[cfg(feature = "simd")]
            {
                #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                {
                    if std::is_x86_feature_detected!("avx2") {
                        return (
                            CompressionType::DeltaDelta,
                            Self::compress_delta_delta_simd(data),
                        );
                    }
                }
                #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
                {
                    return (
                        CompressionType::DeltaDelta,
                        Self::compress_delta_delta_simd(data),
                    );
                }
            }
        }

        (CompressionType::None, data.to_vec())
    }

    pub fn decompress(comp_type: CompressionType, data: &[u8]) -> Vec<u8> {
        match comp_type {
            CompressionType::None => data.to_vec(),
            CompressionType::DeltaDelta => {
                #[cfg(feature = "simd")]
                {
                    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                    {
                        if std::is_x86_feature_detected!("avx2") {
                            return Self::decompress_delta_delta_simd(data);
                        }
                    }
                    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
                    {
                        return Self::decompress_delta_delta_simd(data);
                    }
                }
                data.to_vec()
            }
            CompressionType::BitPackedDelta => Self::decompress_bitpacked(data),
        }
    }

    #[cfg(feature = "simd")]
    fn compress_delta_delta_simd(data: &[u8]) -> Vec<u8> {
        let mut values = Vec::with_capacity(data.len() / 8);
        for chunk in data.chunks_exact(8) {
            values.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }

        if values.len() < 3 {
            return data.to_vec();
        }

        let mut result = Vec::with_capacity(data.len());
        result.extend_from_slice(&values[0].to_le_bytes());
        result.extend_from_slice(&values[1].to_le_bytes());

        let mut i = 2;
        while i + 3 < values.len() {
            let curr = u64x4::from_array([values[i], values[i + 1], values[i + 2], values[i + 3]]);
            let prev = u64x4::from_array([values[i - 1], values[i], values[i + 1], values[i + 2]]);
            let prev2 = u64x4::from_array([values[i - 2], values[i - 1], values[i], values[i + 1]]);

            let shift = Simd::from_array([1; 4]);
            let dd = curr - (prev << shift) + prev2;

            for &val in dd.as_array() {
                result.extend_from_slice(&val.to_le_bytes());
            }
            i += 4;
        }

        while i < values.len() {
            let dd = values[i]
                .wrapping_sub(values[i - 1].wrapping_shl(1))
                .wrapping_add(values[i - 2]);
            result.extend_from_slice(&dd.to_le_bytes());
            i += 1;
        }

        result
    }

    #[cfg(feature = "simd")]
    fn decompress_delta_delta_simd(data: &[u8]) -> Vec<u8> {
        if data.len() < 16 {
            return data.to_vec();
        }

        let mut values = Vec::with_capacity(data.len() / 8);
        for chunk in data.chunks_exact(8) {
            values.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }

        let mut result_values = Vec::with_capacity(values.len());
        result_values.push(values[0]);
        result_values.push(values[1]);

        for i in 2..values.len() {
            let val = values[i]
                .wrapping_add(result_values[i - 1].wrapping_shl(1))
                .wrapping_sub(result_values[i - 2]);
            result_values.push(val);
        }

        let mut result_bytes = Vec::with_capacity(result_values.len() * 8);
        for val in result_values {
            result_bytes.extend_from_slice(&val.to_le_bytes());
        }
        result_bytes
    }

    fn compress_bitpacked(data: &[u8]) -> Vec<u8> {
        let mut values = Vec::new();
        for chunk in data.chunks_exact(8) {
            values.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        if values.len() < 3 {
            return data.to_vec();
        }

        let mut deltas = Vec::new();
        let mut max_abs_delta = 0u64;
        for i in 2..values.len() {
            let dd = values[i]
                .wrapping_sub(values[i - 1].wrapping_shl(1))
                .wrapping_add(values[i - 2]);
            deltas.push(dd);
            max_abs_delta = max_abs_delta.max(dd);
        }

        let bits = if max_abs_delta <= 0xFF {
            8
        } else if max_abs_delta <= 0xFFFF {
            16
        } else if max_abs_delta <= 0xFFFFFFFF {
            32
        } else {
            64
        };

        let mut result = vec![bits as u8];
        result.extend_from_slice(&values[0].to_le_bytes());
        result.extend_from_slice(&values[1].to_le_bytes());

        for d in deltas {
            match bits {
                8 => result.push(d as u8),
                16 => result.extend_from_slice(&(d as u16).to_le_bytes()),
                32 => result.extend_from_slice(&(d as u32).to_le_bytes()),
                _ => result.extend_from_slice(&d.to_le_bytes()),
            }
        }
        result
    }

    fn decompress_bitpacked(data: &[u8]) -> Vec<u8> {
        if data.len() < 17 {
            return data.to_vec();
        }
        let bits = data[0];
        let v0 = u64::from_le_bytes(data[1..9].try_into().unwrap());
        let v1 = u64::from_le_bytes(data[9..17].try_into().unwrap());

        let mut result_values = vec![v0, v1];
        let mut i = 17;
        while i < data.len() {
            let dd = match bits {
                8 => {
                    let val = data[i] as u64;
                    i += 1;
                    val
                }
                16 => {
                    let val = u16::from_le_bytes(data[i..i + 2].try_into().unwrap()) as u64;
                    i += 2;
                    val
                }
                32 => {
                    let val = u32::from_le_bytes(data[i..i + 4].try_into().unwrap()) as u64;
                    i += 4;
                    val
                }
                _ => {
                    let val = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
                    i += 8;
                    val
                }
            };
            let prev = result_values[result_values.len() - 1];
            let prev2 = result_values[result_values.len() - 2];
            let val = dd.wrapping_add(prev.wrapping_shl(1)).wrapping_sub(prev2);
            result_values.push(val);
        }

        let mut result_bytes = Vec::with_capacity(result_values.len() * 8);
        for val in result_values {
            result_bytes.extend_from_slice(&val.to_le_bytes());
        }
        result_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_delta_cycle() {
        #[cfg(feature = "simd")]
        {
            let mut original = Vec::new();
            let mut curr = 1000u64;
            let mut step = 10u64;
            for _ in 0..100 {
                original.extend_from_slice(&curr.to_le_bytes());
                curr += step;
                step += 1;
            }

            let (ctype, compressed) = Compressor::compress(&original, CompressionPolicy::Balanced);
            assert!(matches!(ctype, CompressionType::DeltaDelta));

            let decompressed = Compressor::decompress(ctype, &compressed);
            assert_eq!(original, decompressed);
        }
    }

    #[test]
    fn test_bitpacked_cycle() {
        let mut original = Vec::new();
        let mut curr = 1000u64;
        for _ in 0..100 {
            original.extend_from_slice(&curr.to_le_bytes());
            curr += 10; // Constant delta -> DeltaDelta is 0
        }

        let (ctype, compressed) = Compressor::compress(&original, CompressionPolicy::ExtremeSpace);
        assert_eq!(ctype, CompressionType::BitPackedDelta);
        assert!(compressed.len() < original.len());

        let decompressed = Compressor::decompress(ctype, &compressed);
        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_none_compression() {
        let data = b"small data".to_vec();
        let (ctype, compressed) = Compressor::compress(&data, CompressionPolicy::Balanced);
        assert!(matches!(ctype, CompressionType::None));
        assert_eq!(data, compressed);

        let decompressed = Compressor::decompress(ctype, &compressed);
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_extreme_compression_ratio() {
        let mut original = Vec::new();
        let mut curr = 1000u64;
        for _ in 0..100 {
            original.extend_from_slice(&curr.to_le_bytes());
            curr += 10;
        }

        let (_, balanced) = Compressor::compress(&original, CompressionPolicy::Balanced);
        let (_, extreme) = Compressor::compress(&original, CompressionPolicy::ExtremeSpace);

        // Extreme (bit-packed) should be smaller than Balanced (full 64-bit deltas)
        // Balanced (DeltaDelta) uses 8 bytes for 2 headers + 8 bytes * 98 deltas = 800 bytes
        // Extreme (BitPacked) uses 1 byte (bits) + 16 bytes (headers) + 1 byte * 98 deltas = 115 bytes
        println!("Balanced size: {}, Extreme size: {}", balanced.len(), extreme.len());
        assert!(extreme.len() < balanced.len());
    }

    #[test]
    fn test_fastest_policy() {
        let mut original = Vec::new();
        for i in 0..100u64 {
            original.extend_from_slice(&i.to_le_bytes());
        }

        let (ctype, compressed) = Compressor::compress(&original, CompressionPolicy::Fastest);
        assert!(matches!(ctype, CompressionType::None));
        assert_eq!(original, compressed);
    }
}
