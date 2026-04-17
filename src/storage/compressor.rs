#[cfg(feature = "simd")]
use std::simd::{u64x4, Simd};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None = 0,
    DeltaDelta = 1,
}

pub struct Compressor;

impl Compressor {
    pub fn compress(data: &[u8]) -> (CompressionType, Vec<u8>) {
        // Only attempt DeltaDelta if we have enough 64-bit values and SIMD is enabled
        #[cfg(feature = "simd")]
        if data.len() >= 32 && data.len().is_multiple_of(8) {
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
                // On non-x86, we still use portable SIMD if the feature is enabled
                return (
                    CompressionType::DeltaDelta,
                    Self::compress_delta_delta_simd(data),
                );
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
                // Fallback or error
                data.to_vec()
            }
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

            // Use operators which are wrapping for Simd
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
            #[allow(clippy::explicit_counter_loop)]
            for _ in 0..100 {
                original.extend_from_slice(&curr.to_le_bytes());
                curr += step;
                step += 1;
            }

            let (ctype, compressed) = Compressor::compress(&original);
            assert!(matches!(ctype, CompressionType::DeltaDelta));

            let decompressed = Compressor::decompress(ctype, &compressed);
            assert_eq!(original, decompressed);
        }
    }

    #[test]
    fn test_none_compression() {
        let data = b"small data".to_vec();
        let (ctype, compressed) = Compressor::compress(&data);
        assert!(matches!(ctype, CompressionType::None));
        assert_eq!(data, compressed);

        let decompressed = Compressor::decompress(ctype, &compressed);
        assert_eq!(data, decompressed);
    }
}
