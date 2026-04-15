use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
    num_bits: usize,
}

impl BloomFilter {
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        let ln2 = std::f64::consts::LN_2;
        // m = -(n * ln(p)) / (ln(2)^2)
        let num_bits = (-(capacity as f64) * fp_rate.ln() / (ln2 * ln2)).ceil() as usize;
        // k = (m / n) * ln(2)
        let num_hashes = ((num_bits as f64 / capacity as f64) * ln2).ceil() as usize;
        
        let num_hashes = num_hashes.max(1);
        // Ensure some bits, round up to multiple of 64
        let num_u64s = (num_bits + 63) / 64;
        let num_u64s = num_u64s.max(1);
        
        Self {
            bits: vec![0; num_u64s],
            num_hashes,
            num_bits: num_u64s * 64,
        }
    }

    pub fn from_vec(bits: Vec<u8>, num_hashes: usize) -> Self {
        let num_u64s = (bits.len() + 7) / 8;
        let mut u64_bits = vec![0u64; num_u64s];
        for (i, chunk) in bits.chunks(8).enumerate() {
            let mut val = 0u64;
            for (j, &b) in chunk.iter().enumerate() {
                val |= (b as u64) << (j * 8);
            }
            u64_bits[i] = val;
        }
        let num_bits = u64_bits.len() * 64;
        Self {
            bits: u64_bits,
            num_hashes,
            num_bits,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.bits.len() * 8);
        for &val in &self.bits {
            res.extend_from_slice(&val.to_le_bytes());
        }
        res
    }

    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    fn get_hash_pair(key: &[u8]) -> (u64, u64) {
        let mut s1 = DefaultHasher::new();
        key.hash(&mut s1);
        let h1 = s1.finish();

        let mut s2 = DefaultHasher::new();
        key.hash(&mut s2);
        0x9e3779b9_u64.hash(&mut s2); // Some salt
        let h2 = s2.finish();
        (h1, h2)
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = Self::get_hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_idx = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
            let u64_idx = bit_idx / 64;
            let bit_in_u64 = bit_idx % 64;
            self.bits[u64_idx] |= 1 << bit_in_u64;
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return false;
        }
        let (h1, h2) = Self::get_hash_pair(key);
        for i in 0..self.num_hashes {
            let bit_idx = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
            let u64_idx = bit_idx / 64;
            let bit_in_u64 = bit_idx % 64;
            if (self.bits[u64_idx] & (1 << bit_in_u64)) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        bloom.add(b"key1");
        assert!(bloom.contains(b"key1"));
        assert!(!bloom.contains(b"key2"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let n = 1000;
        let fp_target = 0.01;
        let mut bloom = BloomFilter::new(n, fp_target);
        for i in 0..n {
            bloom.add(format!("key{}", i).as_bytes());
        }

        let mut fp_count = 0;
        let test_count = 10000;
        for i in n..(n + test_count) {
            if bloom.contains(format!("key{}", i).as_bytes()) {
                fp_count += 1;
            }
        }

        let fp_rate = fp_count as f64 / test_count as f64;
        println!("FP Rate: {}", fp_rate);
        assert!(fp_rate < fp_target * 2.0); // Allow some leeway
    }

    #[test]
    fn test_bloom_filter_serde() {
        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.add(b"hello");
        bloom.add(b"world");
        
        let bytes = bloom.to_bytes();
        let num_hashes = bloom.num_hashes();
        
        let bloom2 = BloomFilter::from_vec(bytes, num_hashes);
        assert!(bloom2.contains(b"hello"));
        assert!(bloom2.contains(b"world"));
        assert!(!bloom2.contains(b"rust"));
    }
}
