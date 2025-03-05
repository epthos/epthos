use crate::model;
use bytes::Bytes;

impl TryFrom<data_proto::VerifiedDescriptor> for model::VerifiedDescriptor {
    type Error = anyhow::Error;

    fn try_from(value: data_proto::VerifiedDescriptor) -> Result<Self, Self::Error> {
        let verified = model::VerifiedDescriptor {
            file_id: value.file_id.as_slice().try_into()?,
            version: value.version,
            index: value.index.try_into()?,
            total: value.total.try_into()?,
            chunks: value
                .content
                .iter()
                .map(|c| model::BlockId::try_from(c.block_id.as_slice()))
                .collect::<anyhow::Result<Vec<model::BlockId>>>()?,
        };
        Ok(verified)
    }
}

impl TryFrom<data_proto::EncryptedDescriptor> for model::ProtectedDescriptor {
    type Error = anyhow::Error;

    fn try_from(value: data_proto::EncryptedDescriptor) -> Result<Self, Self::Error> {
        let encrypted = model::ProtectedDescriptor {
            filename: value.filename,
            size: value.size,
        };
        Ok(encrypted)
    }
}

impl TryFrom<data_proto::VerifiedBlockPart> for model::VerifiedBlock {
    type Error = anyhow::Error;

    fn try_from(value: data_proto::VerifiedBlockPart) -> Result<Self, Self::Error> {
        let verified = model::VerifiedBlock {
            file_id: value.file_id.as_slice().try_into()?,
            block_id: value.block_id.as_slice().try_into()?,
        };
        Ok(verified)
    }
}

impl TryFrom<data_proto::EncryptedChunk> for model::ProtectedBlock {
    type Error = anyhow::Error;

    fn try_from(value: data_proto::EncryptedChunk) -> Result<Self, Self::Error> {
        let protected = model::ProtectedBlock {
            chunk: Bytes::from(value.chunk),
            padding: value.padding,
        };
        Ok(protected)
    }
}

impl From<model::VerifiedDescriptor> for data_proto::VerifiedDescriptor {
    fn from(value: model::VerifiedDescriptor) -> Self {
        data_proto::VerifiedDescriptor {
            file_id: value.file_id.as_bytes().to_vec(),
            version: value.version,
            index: value.index as u32,
            total: value.total as u32,
            content: value
                .chunks
                .iter()
                .map(|c| data_proto::BlockRef {
                    block_id: c.as_bytes().to_vec(),
                })
                .collect(),
        }
    }
}

impl From<model::ProtectedDescriptor> for data_proto::EncryptedDescriptor {
    fn from(value: model::ProtectedDescriptor) -> Self {
        data_proto::EncryptedDescriptor {
            filename: value.filename,
            size: value.size,
        }
    }
}

impl From<model::VerifiedBlock> for data_proto::VerifiedBlockPart {
    fn from(value: model::VerifiedBlock) -> Self {
        data_proto::VerifiedBlockPart {
            file_id: value.file_id.as_bytes().to_vec(),
            block_id: value.block_id.as_bytes().to_vec(),
        }
    }
}

impl From<model::ProtectedBlock> for data_proto::EncryptedChunk {
    fn from(value: model::ProtectedBlock) -> Self {
        data_proto::EncryptedChunk {
            chunk: value.chunk.into(),
            padding: value.padding,
        }
    }
}

#[cfg(test)]
mod test {
    use model::BlockId;

    use super::*;

    #[test]
    fn test_descriptor_roundtrip() {
        let verified = model::VerifiedDescriptor {
            file_id: [0u8, 1, 2, 3, 4, 5].as_slice().try_into().unwrap(),
            version: 123,
            index: 1,
            total: 2,
            chunks: vec![
                BlockId::try_from([0u8; 12].as_slice()).unwrap(),
                BlockId::try_from([1u8; 12].as_slice()).unwrap(),
            ],
        };
        let protected = model::ProtectedDescriptor {
            filename: "test.txt".to_string(),
            size: 4321,
        };

        let proto_verified: data_proto::VerifiedDescriptor = verified.clone().into();
        let proto_encrypted: data_proto::EncryptedDescriptor = protected.clone().into();

        let verified2: model::VerifiedDescriptor = proto_verified.try_into().unwrap();
        let encrypted2: model::ProtectedDescriptor = proto_encrypted.try_into().unwrap();

        assert!(verified == verified2);
        assert!(protected == encrypted2);
    }

    #[test]
    fn test_block_roundtrip() {
        let verified = model::VerifiedBlock {
            file_id: [0u8, 1, 2, 3, 4, 5].as_slice().try_into().unwrap(),
            block_id: [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
                .as_slice()
                .try_into()
                .unwrap(),
        };
        let protected = model::ProtectedBlock {
            chunk: Bytes::from_static(&[3, 45, 2, 1, 6, 6, 234]),
            padding: vec![32, 13],
        };

        let proto_verified: data_proto::VerifiedBlockPart = verified.clone().into();
        let proto_encrypted: data_proto::EncryptedChunk = protected.clone().into();

        let verified2: model::VerifiedBlock = proto_verified.try_into().unwrap();
        let encrypted2: model::ProtectedBlock = proto_encrypted.try_into().unwrap();

        assert!(verified == verified2);
        assert!(protected == encrypted2);
    }

    #[test]
    fn test_reject_descriptor_overflow() {
        let proto = data_proto::VerifiedDescriptor {
            file_id: vec![0, 1, 2, 3, 4, 5],
            version: 0,
            index: (u16::MAX as u32) + 1,
            total: 1,
            content: vec![],
        };
        assert!(model::VerifiedDescriptor::try_from(proto).is_err());

        let proto = data_proto::VerifiedDescriptor {
            file_id: vec![0, 1, 2, 3, 4, 5],
            version: 0,
            index: 1,
            total: (u16::MAX as u32) + 1,
            content: vec![],
        };
        assert!(model::VerifiedDescriptor::try_from(proto).is_err());
    }
}
