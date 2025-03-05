use self::{builder::Builder, peer::Peer};
use crate::filepicker;
use anyhow::Result;
use std::path::{Path, PathBuf};

mod builder;
mod peer;

/// A Source server, which watches the filesystem and backs data up to a Sink.
pub struct Server<P: Peer> {
    roots: Vec<PathBuf>,
    _peer: P,
    picker: filepicker::Picker,
}

const _CHUNK_SIZE: usize = 2usize.pow(22); // 23 breaks the current gRPC limit.

pub fn builder() -> Builder {
    Builder::default()
}

impl<P: Peer> Server<P> {
    /// Run the server.
    pub async fn serve(self) -> Result<()> {
        // Set up the file store.
        let roots: Vec<&Path> = self.roots.iter().map(|p| p.as_ref()).collect();
        self.picker.set_roots(&roots).await?;

        let mut batch = 0;
        loop {
            match self.picker.next().await? {
                filepicker::Next::ScanDir(directory, updates) => {
                    let _span_ = tracing::trace_span!("fs_scanning").entered();
                    match std::fs::read_dir(&directory) {
                        Ok(dir_list) => {
                            let mut complete = true;
                            for dir in dir_list {
                                let Ok(dir) = dir else {
                                    tracing::info!("failed to stat file: {:?}", &dir);
                                    complete = false;
                                    continue;
                                };
                                let Ok(tp) = dir.file_type() else {
                                    tracing::info!("failed to type file: {:?}", &dir);
                                    complete = false;
                                    continue;
                                };
                                if tp.is_dir() {
                                    updates
                                        .send(filepicker::ScanUpdate::Directory(dir.file_name()))
                                        .await?;
                                } else if tp.is_file() {
                                    updates
                                        .send(filepicker::ScanUpdate::File(dir.file_name()))
                                        .await?;
                                }
                            }
                            updates.send(filepicker::ScanUpdate::Done(complete)).await?;
                        }
                        Err(err) => {
                            updates.send(filepicker::ScanUpdate::Error(err)).await?;
                        }
                    };
                    batch += 1;
                    if batch % 100 == 0 {
                        tracing::info!("scanned {:?} ({})", directory, batch);
                    }
                }
                filepicker::Next::CheckFile(file) => match std::fs::metadata(&file) {
                    Ok(_) => todo!(),
                    Err(err) => {
                        self.picker
                            .update(file, filepicker::Update::Unreadable(err))
                            .await?;
                    }
                },
            }
        }
    }
}
