use crate::{
    Block, Sparse,
    unix::{fallocate, lseek},
};

use super::*;
use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, SEEK_SET};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

pub fn read_sections(file: &Path) -> anyhow::Result<Vec<Section<'static>>> {
    let mut results = vec![];
    let mut fd = File::open(&file)?;
    let mut next = fd.next_block(Block::default())?;
    loop {
        // next_block moved us to the beginning of a "len"-sized data block.
        if next.skipped > 0 {
            results.push(hole((next.skipped / BLOCK_SIZE) as u8));
        }
        if next.size == 0 {
            break;
        }
        let mut buf = vec![0_u8; next.size];
        fd.read_exact(&mut buf)?;
        let block_count = next.size / BLOCK_SIZE + if next.size % BLOCK_SIZE > 0 { 1 } else { 0 };
        results.push(owned_data(block_count as u8, buf));

        next = fd.next_block(next)?;
    }

    Ok(results)
}

pub fn write_sections(file: &Path, sections: &[Section]) -> anyhow::Result<()> {
    let mut fd = File::create(&file)?;
    if sections.is_empty() {
        return Ok(());
    }
    let mut previous = 0;
    let mut offset = 0;

    for section in sections {
        let start = offset;
        let len = (section.blocks as usize) * BLOCK_SIZE;
        previous = offset;
        offset += len;

        match &section.tp {
            SectionType::Hole => {
                fallocate(
                    &mut fd,
                    FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                    start,
                    len,
                )?;
            }
            SectionType::Data(Data { contents }) => {
                lseek(&mut fd, start, SEEK_SET)?;
                fd.write(contents)?;
            }
        }
    }
    // In order to finish with a hole, we need to resize the file.
    let last = &sections[sections.len() - 1];
    match last.tp {
        SectionType::Hole => {
            fallocate(&mut fd, 0, previous, last.blocks as usize * BLOCK_SIZE)?;
        }
        SectionType::Data(_) => {}
    }

    fd.sync_all()?;
    Ok(())
}
