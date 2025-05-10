// TODO: remove after migrating what might still be useful.
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use filetime::FileTime;
use std::{
    fs,
    io::{ErrorKind, Read},
    path::Path,
};
use tokio::sync::mpsc::Sender;

/// Errors returned by read_chunks.
#[derive(thiserror::Error, Debug)]
pub enum ChunkingError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),
    #[error("Premature closing of channel")]
    ChannelClosed,
}

/// Provide the content of a file in constant-sized chunks. The last chunk is the
/// only chunk that can be smaller than `chunk_size`. Empty files don't produce
/// chunks but return successfully right away.
#[tracing::instrument]
pub fn read_chunks(path: &Path, chunk_size: usize, chunks: Sender<Bytes>) -> Result<usize> {
    let mut file = fs::File::open(path)?;
    let md = file.metadata()?;
    // Preserve access time to avoid messing with other tools.
    let atime = FileTime::from_last_access_time(&md);
    let mut total_size = 0;
    let mut done = false;
    while !done {
        let mut data = BytesMut::new();
        data.resize(chunk_size, 0);

        // We try hard to fill the buffer: random interrupts should not lead to
        // different chunks being generated.
        let mut bytes_in_chunk = 0;
        while bytes_in_chunk < chunk_size {
            match file.read(&mut data[bytes_in_chunk..]) {
                // End of file.
                Ok(0) => {
                    done = true;
                    break;
                }
                // Successful read > 0.
                Ok(bytes_read) => {
                    bytes_in_chunk += bytes_read;
                    total_size += bytes_read;
                }
                // This is a retryable error.
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                // All other errors are propagated.
                Err(e) => return Err(e.into()),
            }
        }
        if bytes_in_chunk == 0 {
            break;
        }
        data.truncate(bytes_in_chunk);
        debug_assert!(!data.is_empty());

        if chunks.blocking_send(data.freeze()).is_err() {
            return Err(ChunkingError::ChannelClosed.into());
        }
    }
    drop(file);
    filetime::set_file_atime(path, atime)?;

    // A completely empty file still needs to generate a single empty chunk,
    // to ensure there is a trace of it for recovery.
    if total_size == 0 {
        chunks.blocking_send(Bytes::new())?;
    }
    Ok(total_size)
}
