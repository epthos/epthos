//! Cryptographic operations for the Source.
use prost::Message;
use ring::{
    aead::{self, BoundKey},
    hkdf,
    rand::{self, SecureRandom},
};
use std::sync::Arc;
use thiserror::Error;

mod convert;
pub mod key;
pub mod model;
mod nonce;

const DESCRIPTOR_KEY_INFO: [&[u8]; 1] = [b"descriptor key"];
const BLOCK_KEY_INFO: [&[u8]; 1] = [b"block key"];

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("internal crypto error {0}")]
    Internal(String),
}

pub trait RandomApi {
    fn generate_file_id(&self) -> anyhow::Result<model::FileId>;
    fn generate_block_id(&self) -> anyhow::Result<model::BlockId>;
}
pub type SharedRandom = Arc<dyn RandomApi + Send + Sync>;

/// Random number generator for cryptographic purposes.
/// Only contains methods for specific needs, to avoid misuse.
pub struct Random {
    rnd: rand::SystemRandom,
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

impl Random {
    pub fn new() -> Self {
        Self {
            rnd: rand::SystemRandom::new(),
        }
    }

    // Generates a new random Key. This is only expected to happen once
    // when the Source is created.
    pub fn generate_root_key(&self) -> anyhow::Result<key::Durable> {
        let mut id = [0u8; key::KEY_LEN];
        self.rnd
            .fill(&mut id)
            .map_err(|_| CryptoError::Internal("while generating root Key".to_string()))?;
        Ok(key::Durable::new(id.into(), 0))
    }
}

impl RandomApi for Random {
    /// Generates a new random FileId. For complete safety, unicity should
    /// be checked against the database.
    fn generate_file_id(&self) -> anyhow::Result<model::FileId> {
        let mut id = model::FileId::default();
        self.rnd
            .fill(id.as_mut_bytes())
            .map_err(|_| CryptoError::Internal("while generating file_id".to_string()))?;
        Ok(id)
    }

    /// Generates a new random BlockId. For complete safety, unicity should
    /// be checked against the database in the scope of its corresponding FileId.
    fn generate_block_id(&self) -> anyhow::Result<model::BlockId> {
        let mut id = model::BlockId::default();
        self.rnd
            .fill(id.as_mut_bytes())
            .map_err(|_| CryptoError::Internal("while generating file_id".to_string()))?;
        Ok(id)
    }
}

/// The cryptographic keys used to encrypt and decrypt the data.
pub struct Keys {
    durable: key::Durable,
}

impl Keys {
    pub fn new(durable: key::Durable) -> Self {
        Self { durable }
    }

    /// Encrypts the protected part of a descriptor and sign the verified part.
    pub fn encrypt_descriptor(
        &self,
        verified: model::VerifiedDescriptor,
        encrypted: model::ProtectedDescriptor,
    ) -> anyhow::Result<model::Descriptor> {
        let key = self.derive_descriptor_key(&verified.file_id);
        let nonce = get_descriptor_nonce(&verified.file_id, verified.version, verified.index);

        let vp: data_proto::VerifiedDescriptor = verified.into();
        let ep: data_proto::EncryptedDescriptor = encrypted.into();

        let aad = vp.encode_to_vec();
        let mut encrypted = ep.encode_to_vec();

        encrypt_base(
            &key,
            &nonce,
            aead::Aad::from(aad.as_slice()),
            &mut encrypted,
        )?;
        Ok(model::Descriptor {
            verified: aad,
            protected: encrypted,
        })
    }

    /// Encrypts the protected part of a block and sign the verified part.
    pub fn encrypt_block(
        &self,
        verified: model::VerifiedBlock,
        protected: model::ProtectedBlock,
    ) -> anyhow::Result<model::Block> {
        let key = self.derive_block_key(&verified.file_id);
        let nonce = *verified.block_id.as_bytes();

        let vp: data_proto::VerifiedBlockPart = verified.into();
        let ep: data_proto::EncryptedChunk = protected.into();

        let aad = vp.encode_to_vec();
        let mut encrypted = ep.encode_to_vec();

        encrypt_base(
            &key,
            &nonce,
            aead::Aad::from(aad.as_slice()),
            &mut encrypted,
        )?;
        Ok(model::Block {
            verified: aad,
            protected: Arc::new(encrypted),
        })
    }

    /// Decrypts a descriptor and verifies the protected part.
    pub fn decrypt_descriptor(
        &self,
        descriptor: &model::Descriptor,
    ) -> anyhow::Result<(model::VerifiedDescriptor, model::ProtectedDescriptor)> {
        // The serialized proto is first assumed correct to know the
        // various arguments, but we won't return anything before validating
        // the tag and decrypting.
        let vp = data_proto::VerifiedDescriptor::decode(descriptor.verified.as_slice())?;
        let verified: model::VerifiedDescriptor = vp.try_into()?;

        let key = self.derive_descriptor_key(&verified.file_id);
        let nonce = get_descriptor_nonce(&verified.file_id, verified.version, verified.index);
        let aad = aead::Aad::from(descriptor.verified.as_slice());

        let mut data = descriptor.protected.clone();
        let es = decrypt_base(&key, &nonce, aad, &mut data)?;
        let ep = data_proto::EncryptedDescriptor::decode(es)?;

        Ok((verified, ep.try_into()?))
    }

    /// Decrypts a block and verifies the protected part.
    pub fn decrypt_block(
        &self,
        block: &model::Block,
    ) -> anyhow::Result<(model::VerifiedBlock, model::ProtectedBlock)> {
        let vp = data_proto::VerifiedBlockPart::decode(block.verified.as_slice())?;
        let verified: model::VerifiedBlock = vp.try_into()?;

        let key = self.derive_block_key(&verified.file_id);
        let nonce = verified.block_id.as_bytes();
        let aad = aead::Aad::from(block.verified.as_slice());

        let mut data = block.protected.as_ref().clone();
        let es = decrypt_base(&key, nonce, aad, &mut data)?;
        let ep = data_proto::EncryptedChunk::decode(es)?;

        Ok((verified, ep.try_into()?))
    }

    // Descriptor key = HKDF(key, salt = file_id, info = "descriptor key")
    fn derive_descriptor_key(&self, file_id: &model::FileId) -> key::Key {
        derive_key(&self.durable, file_id, &DESCRIPTOR_KEY_INFO)
    }

    // Block key = HKDF(key, salt = file_id, info = "block key")
    fn derive_block_key(&self, file_id: &model::FileId) -> key::Key {
        derive_key(&self.durable, file_id, &BLOCK_KEY_INFO)
    }
}

// HKDF key derivation helper. All keys derive from the FileId, with different
// info strings and input key material.
fn derive_key(key: &key::Durable, file_id: &model::FileId, info: &[&[u8]]) -> key::Key {
    // TODO: keep the salt with the FileId if it's a performnce issue.
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, file_id.as_bytes());
    let prk = salt.extract(key.key().as_ref());
    let okm = prk
        .expand(info, &aead::AES_128_GCM)
        // Only failure is on length mismatch, which is static (and tested).
        .unwrap();
    let mut key = [0u8; key::KEY_LEN];
    // Only failure is on length mismatch, which is static (and tested).
    okm.fill(&mut key).unwrap();
    key.into()
}

// The 96 bit nonce of the descriptor is deterministic and is defined as:
//   file_id || version || index
//        48 ||      32 ||    16
fn get_descriptor_nonce(file_id: &model::FileId, version: u32, index: u16) -> nonce::Nonce {
    let mut nonce = [0u8; nonce::NONCE_LEN];
    nonce[0..model::FILE_ID_LEN].copy_from_slice(file_id.as_bytes());
    nonce[model::FILE_ID_LEN..model::FILE_ID_LEN + 4].copy_from_slice(&version.to_le_bytes());
    nonce[model::FILE_ID_LEN + 4..model::FILE_ID_LEN + 6].copy_from_slice(&index.to_le_bytes());
    nonce
}

fn encrypt_base(
    key: &key::Key,
    nonce: &nonce::Nonce,
    aad: aead::Aad<&[u8]>,
    data: &mut Vec<u8>,
) -> anyhow::Result<()> {
    let unbounded = aead::UnboundKey::new(&aead::AES_128_GCM, key.as_ref())
        .map_err(|_| CryptoError::Internal("creating encryption key".to_string()))?;
    let mut sealing = aead::SealingKey::new(unbounded, nonce::OneShot::new(nonce));
    sealing
        .seal_in_place_append_tag(aad, data)
        .map_err(|_| CryptoError::Internal("encrypting data".to_string()))?;
    Ok(())
}

fn decrypt_base<'a>(
    key: &key::Key,
    nonce: &nonce::Nonce,
    aad: aead::Aad<&[u8]>,
    data: &'a mut [u8],
) -> anyhow::Result<&'a [u8]> {
    let unbounded = aead::UnboundKey::new(&aead::AES_128_GCM, key.as_ref())
        .map_err(|_| CryptoError::Internal("creating decryption key".to_string()))?;
    let mut opening = aead::OpeningKey::new(unbounded, nonce::OneShot::new(nonce));
    let data = opening
        .open_in_place(aad, data)
        .map_err(|_| CryptoError::Internal("decrypting data".to_string()))?;
    Ok(data)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_descriptor_roundtrip() -> Result<()> {
        let rnd = Random::new();
        let source = Keys::new(rnd.generate_root_key()?);

        let verified = model::VerifiedDescriptor {
            file_id: rnd.generate_file_id().unwrap(),
            version: 12,
            index: 32,
            total: 43,
            chunks: vec![
                model::BlockId::try_from([0u8; 12].as_slice()).unwrap(),
                model::BlockId::try_from([1u8; 12].as_slice()).unwrap(),
            ],
        };
        let encrypted = model::ProtectedDescriptor {
            filename: "test.txt".to_string(),
            size: 123,
        };

        let descriptor = source
            .encrypt_descriptor(verified.clone(), encrypted.clone())
            .unwrap();
        let (verified2, encrypted2) = source.decrypt_descriptor(&descriptor).unwrap();

        assert!(verified == verified2);
        assert!(encrypted == encrypted2);
        Ok(())
    }

    #[test]
    fn test_block_roundtrip() -> Result<()> {
        let rnd = Random::new();
        let source = Keys::new(rnd.generate_root_key()?);

        let verified = model::VerifiedBlock {
            file_id: rnd.generate_file_id().unwrap(),
            block_id: rnd.generate_block_id().unwrap(),
        };
        let protected = model::ProtectedBlock {
            chunk: Bytes::from_static(&[33, 12, 37]),
            padding: vec![66, 11],
        };

        let block = source
            .encrypt_block(verified.clone(), protected.clone())
            .unwrap();
        let (verified2, protected2) = source.decrypt_block(&block).unwrap();

        assert!(verified == verified2);
        assert!(protected == protected2);

        Ok(())
    }
}
