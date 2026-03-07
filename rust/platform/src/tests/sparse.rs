//! Helpers for sparse files testing.
use std::borrow::Cow;

#[cfg(windows)]
mod win;
#[cfg(windows)]
use win as helpers;

pub use helpers::read_sections;
pub use helpers::write_sections;

// The size is larger than most block sizes and enough to match the NTFS compression
// block size, which is the minimal unit that will be use to free up sparse blocks.
pub const BLOCK_SIZE: usize = 65536;

#[derive(Debug, PartialEq)]
pub struct Section<'a> {
    tp: SectionType<'a>,
    blocks: u8,
}

#[derive(Debug, PartialEq)]
pub enum SectionType<'a> {
    Hole,
    Data(Data<'a>),
}

#[derive(PartialEq)]
pub struct Data<'a> {
    contents: Cow<'a, [u8]>,
}

impl<'a> std::fmt::Debug for Data<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Data")
            .field(
                "contents",
                &format!("{:?}: {} bytes", &self.contents[..10], self.contents.len()),
            )
            .finish()
    }
}

pub fn hole(blocks: u8) -> Section<'static> {
    Section {
        tp: SectionType::Hole,
        blocks,
    }
}

pub fn data<'a>(blocks: u8, contents: &'a [u8]) -> Section<'a> {
    Section {
        tp: SectionType::Data(Data {
            contents: Cow::Borrowed(contents),
        }),
        blocks,
    }
}

pub fn owned_data(blocks: u8, contents: Vec<u8>) -> Section<'static> {
    Section {
        tp: SectionType::Data(Data {
            contents: Cow::Owned(contents),
        }),
        blocks,
    }
}
