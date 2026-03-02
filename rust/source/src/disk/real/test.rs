use super::*;
use ring::digest::SHA256;
use std::{cmp::min, collections::HashMap};
use test_log::test;
use tracing::info;

#[test]
fn file_is_sparse_only() -> Result<()> {
    // Only return one Hole, disallow reading bytes to confirm we don't try.
    let reader = ReadInjector::new(
        &[],
        |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "nope",
            ))
        },
        vec![Block {
            skipped: 100,
            size: 0,
            offset: 0,
        }],
    );

    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 50);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c1,
        Chunk::Hole {
            offset: 0,
            size: 100
        }
    );
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn file_starts_with_hole() -> Result<()> {
    let mut input = [0u8; 100];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |_| Ok(100),
        vec![
            Block {
                skipped: 200,
                size: 100,
                offset: 200,
            },
            Block {
                skipped: 0,
                size: 0,
                offset: 300,
            },
        ],
    );

    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 50);
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Hole {
            offset: 0,
            size: 200
        }
    );
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Data {
            data: input[..50].to_vec(),
            offset: 200,
            hash: ring::digest::digest(&SHA256, &input[0..50]).into(),
        }
    );
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Data {
            data: input[50..100].to_vec(),
            offset: 250,
            hash: ring::digest::digest(&SHA256, &input[50..100]).into(),
        }
    );
    assert!(it.next().is_none());
    Ok(())
}

#[test]
fn file_ends_with_hole() -> Result<()> {
    let mut input = [0u8; 100];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |_| Ok(100),
        vec![
            Block {
                skipped: 0,
                size: 100,
                offset: 100,
            },
            Block {
                skipped: 200,
                size: 0,
                offset: 100,
            },
        ],
    );

    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 50);
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Data {
            data: input[..50].to_vec(),
            offset: 0,
            hash: ring::digest::digest(&SHA256, &input[0..50]).into(),
        }
    );
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Data {
            data: input[50..100].to_vec(),
            offset: 50,
            hash: ring::digest::digest(&SHA256, &input[50..100]).into(),
        }
    );
    let c = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c,
        Chunk::Hole {
            offset: 100,
            size: 200
        }
    );
    assert!(it.next().is_none());
    Ok(())
}

#[test]
fn chunk_file_on_boundary() -> Result<()> {
    let mut input = [0u8; 100];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |_| Ok(200),
        vec![Block {
            skipped: 0,
            size: 100,
            offset: 0,
        }],
    );
    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 50);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c1,
        Chunk::Data {
            data: input[..50].to_vec(),
            offset: 0,
            hash: ring::digest::digest(&SHA256, &input[0..50]).into(),
        }
    );
    let c2 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c2,
        Chunk::Data {
            data: input[50..].to_vec(),
            offset: 50,
            hash: ring::digest::digest(&SHA256, &input[50..]).into(),
        }
    );
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn chunk_file_with_different_size() -> Result<()> {
    let mut input = [0u8; 100];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |_| Ok(200),
        vec![Block {
            skipped: 0,
            size: 100,
            offset: 0,
        }],
    );
    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c1,
        Chunk::Data {
            data: input[..60].to_vec(),
            offset: 0,
            hash: ring::digest::digest(&SHA256, &input[0..60]).into(),
        }
    );
    let c2 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c2,
        Chunk::Data {
            data: input[60..].to_vec(),
            offset: 60,
            hash: ring::digest::digest(&SHA256, &input[60..]).into(),
        }
    );
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn chunk_file_handling_interrupt() -> Result<()> {
    let mut input = [0u8; 10];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |i: u8| match i {
            0 => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "again",
            )),
            _ => Ok(10),
        },
        vec![Block {
            skipped: 0,
            size: 10,
            offset: 0,
        }],
    );
    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c1,
        Chunk::Data {
            data: input[..].to_vec(),
            offset: 0,
            hash: ring::digest::digest(&SHA256, &input[..]).into(),
        }
    );
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn chunk_file_handling_small_read() -> Result<()> {
    let mut input = [0u8; 50];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |i: u8| match i {
            0 => Ok(10),
            _ => Ok(100),
        },
        vec![Block {
            skipped: 0,
            size: 110,
            offset: 0,
        }],
    );
    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))??;
    assert_eq!(
        c1,
        Chunk::Data {
            data: input[..].to_vec(),
            offset: 0,
            hash: ring::digest::digest(&SHA256, &input[..]).into(),
        }
    );
    assert!(it.next().is_none());

    Ok(())
}

#[test]
fn chunk_file_handling_fatal_error() -> Result<()> {
    let mut input = [0u8; 50];
    for idx in 0..input.len() {
        input[idx] = idx as u8;
    }
    let reader = ReadInjector::new(
        &input[..],
        |i: u8| match i {
            0 => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "nope",
            )),
            _ => Ok(100),
        },
        vec![Block {
            skipped: 0,
            size: 100,
            offset: 0,
        }],
    );
    let mut it = ChunkIterator::new(PathBuf::from("fake"), reader, 60);
    let c1 = it.next().ok_or(DiskError::Unsupported("oops".into()))?;
    assert!(c1.is_err());
    assert!(it.next().is_none());

    Ok(())
}

// Helper Read impl that allows a user-provided function to decide when
// to inject errors, small reads, etc.
struct ReadInjector<'a, F: Fn(u8) -> std::io::Result<usize>> {
    data: &'a [u8],
    // Offset in the data.
    offset: usize,
    // Index of the current read() call.
    iteration: u8,
    // Function that decides if a read is successful or not based on the
    // operation's index.
    read_op: F,
    // Chain of Blocks, indexed by the previous offset.
    blocks: HashMap<usize, Block>,
}

impl<'a, F> ReadInjector<'a, F>
where
    F: Fn(u8) -> std::io::Result<usize>,
{
    fn new(data: &'a [u8], op: F, ordered_blocks: Vec<Block>) -> Self {
        let mut blocks = HashMap::new();
        let mut key = 0;
        for block in ordered_blocks {
            let current = key;
            key = block.offset;
            blocks.insert(current, block);
        }
        ReadInjector {
            data,
            offset: 0,
            iteration: 0,
            read_op: op,
            blocks,
        }
    }
}

impl<'a, F> std::io::Read for ReadInjector<'a, F>
where
    F: Fn(u8) -> std::io::Result<usize>,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let next = (self.read_op)(self.iteration);
        self.iteration += 1; // even when returning errors.
        let next = next?;
        let avail = &self.data[self.offset..];
        let start = self.offset;
        let len = min(min(next, buf.len()), avail.len());
        self.offset += len;
        info!("serving start={} len={}", start, len);
        (&mut buf[..len]).copy_from_slice(&self.data[start..(start + len)]);
        Ok(len)
    }
}

impl<'a, F> Sparse for ReadInjector<'a, F>
where
    F: Fn(u8) -> std::io::Result<usize>,
{
    fn next_block(&mut self, previous: Block) -> std::io::Result<Block> {
        Ok(self
            .blocks
            .get(&previous.offset)
            .ok_or(std::io::Error::other(format!(
                "can't find block at offset {}",
                previous.offset
            )))?
            .clone())
    }
}
