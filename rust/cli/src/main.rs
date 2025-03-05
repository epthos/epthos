use anyhow::Context;
use clap::{Parser, Subcommand};
use crypto::{self, model, RandomApi};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use storage::{filesystem, fingerprint, layout};
use thiserror::Error;
use tokio::sync::mpsc;

const CHUNK_SIZE: usize = 2usize.pow(10);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // Initial file when restoring.
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Sink {
        #[arg(short, long)]
        key: String,
        #[arg(short, long)]
        cert: String,
        #[arg(short, long)]
        overwrite: bool,
    },
    Source {
        #[arg(short, long)]
        key: String,
        #[arg(short, long)]
        cert: String,
        #[arg(short, long, required=true, num_args=1..)]
        root: Vec<PathBuf>,
        #[arg(short, long)]
        overwrite: bool,
    },
    Backup {
        /// Key to use.
        #[arg(short, long)]
        key: PathBuf,
        /// File to back up.
        #[arg(short, long)]
        path: PathBuf,
        // Output directory when backing up.
        #[arg(short, long)]
        out: PathBuf,
    },
    Restore {
        /// Key to use.
        #[arg(short, long)]
        key: PathBuf,
        /// File to back up.
        #[arg(short, long)]
        src: PathBuf,
        // Output directory when backing up.
        #[arg(short, long)]
        dst: PathBuf,
    },
}

fn pad(chunk: &model::Chunk) -> Vec<u8> {
    if chunk.bytes().len() == CHUNK_SIZE {
        return vec![];
    }
    // Use the last byte of the digest to determine the padding length.
    let pad = chunk.digest()[chunk.digest().len() - 1];
    vec![pad; pad as usize]
}

async fn backup(
    path: PathBuf,
    fp: &fingerprint::Fingerprinter,
    rnd: &crypto::Random,
    key: &crypto::Keys,
    out: PathBuf,
) -> anyhow::Result<()> {
    let file_id = rnd.generate_file_id()?;
    let out = layout::Root::new(out).file(&file_id)?;

    let protected_desc = model::ProtectedDescriptor {
        filename: path.clone().to_string_lossy().to_string(),
        size: path.metadata()?.len(),
    };
    let (chunk_in, mut chunk_out) = mpsc::channel(1);
    let reader =
        tokio::task::spawn_blocking(move || filesystem::read_chunks(&path, CHUNK_SIZE, chunk_in));

    let mut verified_desc = model::VerifiedDescriptor {
        file_id: file_id.clone(),
        version: 0,
        index: 0,
        total: 1,
        chunks: vec![],
    };
    while let Some(data) = chunk_out.recv().await {
        let chunk = fp.hash(data).await;
        tracing::info!("read chunk {:?}", chunk.digest());

        let block_id = rnd.generate_block_id()?;

        // TODO: change the API as we don't need to expose the protected and verified block.
        let protected = model::ProtectedBlock {
            chunk: chunk.bytes(),
            padding: pad(&chunk),
        };
        let verified = model::VerifiedBlock {
            file_id: file_id.clone(),
            block_id: block_id.clone(),
        };

        let block = key.encrypt_block(verified, protected)?;
        verified_desc.chunks.push(block_id.clone());

        out.write_block(&block, &block_id)?;
    }
    reader.await??;
    let descriptor = key.encrypt_descriptor(verified_desc, protected_desc)?;
    out.write_descriptor(&descriptor, 0)?;
    tracing::info!("hashed and inserted file");
    Ok(())
}

async fn restore(src: PathBuf, key: &crypto::Keys, dst: PathBuf) -> anyhow::Result<()> {
    let (descriptor, root) = layout::read_descriptor(&src)?;
    let (v_desc, p_desc) = key
        .decrypt_descriptor(&descriptor)
        .context("problem decrypting the descriptor")?;
    tracing::info!("decrypted descriptor {:?}", p_desc);
    let file = root.file(&v_desc.file_id)?;
    let mut out = std::fs::File::create(dst).context("can't open output file")?;
    for block_id in v_desc.chunks {
        let block = file.read_block(&block_id)?;
        let (v_block, p_block) = key.decrypt_block(&block)?;
        tracing::info!("decrypted block {:?}", v_block);
        out.write_all(&p_block.chunk)?;
    }
    Ok(())
}

#[derive(Error, Debug)]
enum Error {
    #[error("File {0} already exists. Pass --overwrite to ignore.")]
    AlreadyExists(PathBuf),
}

fn sink(keyfile: &str, certfile: &str, overwrite: bool) -> anyhow::Result<()> {
    let anchor = settings::get_anchor(None)?;
    let path = source_settings::Builder::path(&anchor);
    if path.exists() && !overwrite {
        Err(Error::AlreadyExists(path))?;
    }
    let key = std::fs::read_to_string(keyfile)?;
    let cert = std::fs::read_to_string(certfile)?;
    sink_settings::Builder::default()
        // Accept connections from everywhere, as a sink is a service.
        .address("[::0]:0")
        .certificate(cert)
        .private_key(key)
        .save(&anchor)
}

fn source(
    keyfile: &str,
    certfile: &str,
    root: Vec<PathBuf>,
    overwrite: bool,
) -> anyhow::Result<()> {
    let anchor = settings::get_anchor(None)?;
    let path = source_settings::Builder::path(&anchor);
    if path.exists() && !overwrite {
        Err(Error::AlreadyExists(path))?;
    }
    let key = std::fs::read_to_string(keyfile)?;
    let cert = std::fs::read_to_string(certfile)?;
    source_settings::Builder::default()
        .certificate(cert)
        .private_key(key)
        .root(&root)?
        .save(&anchor)
}

fn get_source_key(path: &Path, rnd: &crypto::Random) -> anyhow::Result<crypto::Keys> {
    let durable = crypto::key::Durable::from_file(path).or_else(|_| {
        let durable = rnd.generate_root_key()?;
        durable.to_file(path)?;
        Ok::<crypto::key::Durable, anyhow::Error>(durable)
    })?;
    Ok(crypto::Keys::new(durable))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = settings::process::debug()?;

    let args = Args::parse();

    let rnd = crypto::Random::new();
    let fp = fingerprint::Fingerprinter::new(1)?;

    match args.command {
        Commands::Sink {
            key,
            cert,
            overwrite,
        } => {
            sink(&key, &cert, overwrite)?;
        }
        Commands::Source {
            key,
            cert,
            root,
            overwrite,
        } => {
            source(&key, &cert, root, overwrite)?;
        }
        Commands::Backup { key, path, out } => {
            backup(path, &fp, &rnd, &get_source_key(&key, &rnd)?, out).await?;
        }
        Commands::Restore { key, src, dst } => {
            restore(src, &get_source_key(&key, &rnd)?, dst).await?;
        }
    }
    Ok(())
}
