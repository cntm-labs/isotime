use aes_gcm_siv::{
    aead::{Aead, KeyInit},
    Aes256GcmSiv, Nonce,
};
use getrandom::getrandom;
use std::io;

pub struct EncryptionManager {
    cipher: Aes256GcmSiv,
}

impl EncryptionManager {
    pub const NONCE_SIZE: usize = 12;

    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256GcmSiv::new(key.into());
        Self { cipher }
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> io::Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; Self::NONCE_SIZE];
        getrandom(&mut nonce_bytes).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut result = Vec::with_capacity(Self::NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    pub fn decrypt(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        if data.len() < Self::NONCE_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Data too short"));
        }

        let (nonce_bytes, ciphertext) = data.split_at(Self::NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_roundtrip() {
        let key = [0u8; 32];
        let manager = EncryptionManager::new(&key);
        let plaintext = b"secret data to encrypt";

        let encrypted = manager.encrypt(plaintext).unwrap();
        assert_ne!(plaintext.to_vec(), encrypted);

        let decrypted = manager.decrypt(&encrypted).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_decryption_failure() {
        let key = [0u8; 32];
        let manager = EncryptionManager::new(&key);
        let encrypted = manager.encrypt(b"data").unwrap();

        let mut corrupted = encrypted.clone();
        if let Some(last) = corrupted.last_mut() {
            *last ^= 1;
        }

        assert!(manager.decrypt(&corrupted).is_err());
    }
}
