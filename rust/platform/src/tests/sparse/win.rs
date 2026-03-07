use super::*;
use crate::{Block, Sparse};
use std::{
    ffi::c_void,
    fs::File,
    io::{Read, Seek, Write},
    os::windows::io::AsRawHandle,
    path::Path,
};
use windows::Win32::{
    Foundation::HANDLE,
    System::{
        IO::DeviceIoControl,
        Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_SPARSE, FSCTL_SET_ZERO_DATA},
    },
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

fn fallocate(file: &mut File, start: usize, len: usize) -> anyhow::Result<()> {
    // To reliably create a sparse "hole" as described in
    // https://learn.microsoft.com/en-us/windows/win32/fileio/sparse-file-operations
    // we first ensure the region contains non-zero data, then punch it out
    // with FSCTL_SET_ZERO_DATA. This allows the filesystem to deallocate
    // the underlying clusters.
    // file.seek(std::io::SeekFrom::Start(start as u64))?;
    // let pattern = [0xFFu8; BLOCK_SIZE];
    // let mut remaining = len;
    // while remaining > 0 {
    //     let chunk = std::cmp::min(remaining, pattern.len());
    //     file.write_all(&pattern[..chunk])?;
    //     remaining -= chunk;
    // }

    let handle = HANDLE(file.as_raw_handle() as *mut c_void);
    let mut zero_data = FILE_ZERO_DATA_INFORMATION {
        FileOffset: start as i64,
        BeyondFinalZero: (start + len) as i64,
    };
    let mut bytes_returned = 0;
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_ZERO_DATA,
            Some(&mut zero_data as *mut _ as *const _),
            std::mem::size_of::<FILE_ZERO_DATA_INFORMATION>() as u32,
            None,
            0,
            Some(&mut bytes_returned),
            None,
        )
    }?;
    Ok(())
}

pub fn write_sections(file: &Path, sections: &[Section]) -> anyhow::Result<()> {
    let mut fd = File::create(&file)?;
    let handle = HANDLE(fd.as_raw_handle() as *mut c_void);
    let mut bytes_returned = 0;
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            Some(&mut bytes_returned),
            None,
        )
    }?;

    if sections.is_empty() {
        return Ok(());
    }
    let mut offset = 0;

    for section in sections {
        let start = offset;
        let len = (section.blocks as usize) * BLOCK_SIZE;
        offset += len;

        match &section.tp {
            SectionType::Hole => {
                fallocate(&mut fd, start, len)?;
            }
            SectionType::Data(Data { contents }) => {
                fd.seek(std::io::SeekFrom::Start(start as u64))?;
                fd.write_all(contents)?;
            }
        }
    }
    // In order to finish with a hole, we need to resize the file.
    let last = &sections[sections.len() - 1];
    match last.tp {
        SectionType::Hole => {
            fd.set_len(offset as u64)?;
        }
        SectionType::Data(_) => {}
    }

    fd.sync_all()?;
    Ok(())
}
