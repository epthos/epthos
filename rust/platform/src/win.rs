use crate::{
    Block, Sparse,
    shared::{LocalPathRepr, PlatformError},
};
use anyhow::{Context, anyhow};
use std::{
    ffi::c_void,
    fs::File,
    io::Seek,
    os::windows::{
        ffi::{OsStrExt, OsStringExt},
        io::AsRawHandle,
    },
    path::{Path, PathBuf},
};
use windows::{
    Win32::{
        Foundation::{ERROR_HANDLE_EOF, ERROR_MORE_DATA, HANDLE, HLOCAL},
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
            Authorization::{
                ConvertSidToStringSidA, ConvertStringSecurityDescriptorToSecurityDescriptorA,
                GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
            },
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetSecurityDescriptorDacl,
            GetTokenInformation, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES, TOKEN_QUERY,
            TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::CreateDirectoryW,
        System::{
            IO::DeviceIoControl,
            Ioctl::{FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES},
            Memory::LocalAlloc,
            SystemServices::ACCESS_ALLOWED_ACE_TYPE,
            Threading::{GetCurrentProcess, OpenProcessToken},
        },
    },
    core::{BOOL, HRESULT, Owned, PCSTR, PCWSTR, PSTR},
};

impl Sparse for File {
    fn next_block(&mut self, previous: Block) -> std::io::Result<Block> {
        let md = self.metadata()?;
        let end = md.len() as usize;

        if previous.offset >= end {
            return Ok(Block {
                skipped: 0,
                size: 0,
                offset: previous.offset,
            });
        }

        match get_next_allocated_range(self, previous.offset, end - previous.offset)? {
            None => {
                tracing::debug!("no allocated range found");
                Ok(Block {
                    skipped: end - previous.offset,
                    size: 0,
                    offset: end,
                })
            }
            Some(range) => {
                tracing::debug!("allocated range found: {:?}", &range);
                let start = range.FileOffset as usize;
                let length = range.Length as usize;

                // Compared to Linux's lseek(SEEK_HOLE), the call above did not change the
                // position in the file. We must ensure the caller can read the next bytes
                // when this call returns.
                self.seek(std::io::SeekFrom::Start(start as u64))?;
                Ok(Block {
                    skipped: start - previous.offset,
                    size: length,
                    offset: start + length,
                })
            }
        }
    }
}

// Buffer doesn't hold all the results.
const MORE_DATA: HRESULT = HRESULT::from_win32(ERROR_MORE_DATA.0);
// Reached End Of File.
const EOF: HRESULT = HRESULT::from_win32(ERROR_HANDLE_EOF.0);

// Returns the next allocated block in the provided range. Returns None if there is no
// allocated block at or after start (but there might still be a hole).
fn get_next_allocated_range(
    file: &File,
    start: usize,
    len: usize,
) -> std::io::Result<Option<FILE_ALLOCATED_RANGE_BUFFER>> {
    let handle = HANDLE(file.as_raw_handle() as *mut c_void);
    // Query which parts of the file actually have allocated data using
    // FSCTL_QUERY_ALLOCATED_RANGES IoControl call:
    // https://learn.microsoft.com/en-us/windows/win32/fileio/sparse-file-operations
    let mut query = FILE_ALLOCATED_RANGE_BUFFER {
        FileOffset: start as i64,
        Length: len as i64,
    };
    let mut output: [FILE_ALLOCATED_RANGE_BUFFER; 1] = [FILE_ALLOCATED_RANGE_BUFFER::default(); 1];
    let mut bytes_returned: u32 = 0;

    let result = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_ALLOCATED_RANGES,
            Some(&mut query as *mut _ as *const _),
            std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() as u32,
            Some(output.as_mut_ptr() as *mut _),
            std::mem::size_of_val(&output) as u32,
            Some(&mut bytes_returned),
            None,
        )
    };

    match &result {
        Ok(_) => {
            if bytes_returned == 0 {
                // No allocated ranges at or after previous.offset: it's a hole until EOF.
                return Ok(None);
            }
        }
        Err(e) => {
            let code = e.code();
            if code == EOF {
                // Treat EOF as a final hole.
                return Ok(None);
            }
            // MORE_DATA is ok, we read allocated blocks one at a time anyways.
            if code != MORE_DATA {
                return Err(std::io::Error::from_raw_os_error(code.0 as i32));
            }
        }
    };
    Ok(Some(output[0]))
}

// The data area passed to a system call is too small. (0x8007007A)
const TOO_SMALL: HRESULT = HRESULT::from_win32(0x8007007A);
// The file or directory already exists (0x800700B7)
const ALREADY_EXISTS: HRESULT = HRESULT::from_win32(0x800700B7);

/// Ensures path points to a safe config directory, possibly creating it.
pub fn private_directory(path: &std::path::Path) -> Result<(), PlatformError> {
    let u16_path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<u16>>();

    unsafe {
        let p_sid = get_user_psid().context("get_user_psid() failed")?;
        let sid = psid_to_sddl(*p_sid)
            .context("psid_to_sddl() failed")?
            .to_string()
            .context("to_string() of SDDL failed")?;

        // Create the security descriptor string: no inheritence, full file
        // permission granted to the current user.
        let str_sd = format!("D:P(A;;FA;;;{})", &sid);

        let b_str_sd = std::ffi::CString::new(str_sd.clone()).context("CString::new() failed")?;
        let p_str_sd = PCSTR::from_raw(b_str_sd.as_ptr() as *const u8);

        let mut p_sd = PSECURITY_DESCRIPTOR::default();
        ConvertStringSecurityDescriptorToSecurityDescriptorA(
            p_str_sd,
            SDDL_REVISION_1,
            &mut p_sd,
            None,
        )
        .context("ConvertStringSecurityDescriptorToSecurityDescriptorA() failed")?;
        // Ensure scoped deallocation.
        let p_sd = Tow::ptr_to_hlocal(p_sd);

        // Create security attributes
        let sa = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: p_sd.0,
            bInheritHandle: BOOL::from(false),
        };

        let p_u16_path = PCWSTR::from_raw(u16_path.as_ptr());
        let result = CreateDirectoryW(p_u16_path, Some(&sa));
        match &result {
            Ok(_) => {
                // The directory was created with the right permissions, we're good already.
                return Ok(());
            }
            Err(err) => {
                if err.code() != ALREADY_EXISTS {
                    return Err(PlatformError::DirectoryCreationError(
                        path.to_path_buf(),
                        result.context("CreateDirectoryW() failed").unwrap_err(),
                    ));
                }
            }
        }

        // High level check: if the existing target is not a directory, we'll fail to create it
        // going forward.
        if !path.is_dir() {
            return Err(PlatformError::DirectoryCreationError(
                path.to_path_buf(),
                anyhow!("target exists but is not a directory"),
            ));
        }

        // The directory exist. It's fine as long as it's with the right permissions.
        validate_permissions(path, p_u16_path, *p_sid)
            .map_err(|err| PlatformError::DirectoryPermissionError(path.to_path_buf(), err))
    }
}

impl<P: AsRef<Path>> From<P> for LocalPathRepr {
    fn from(path: P) -> LocalPathRepr {
        let wide: Vec<u16> = path.as_ref().as_os_str().encode_wide().collect();
        let mut canonical: Vec<u8> = Vec::with_capacity(wide.len() * 2);
        // The information is specific to the platform it originated from, so there is no need to handle endianness or
        // map between Unix & Windows, etc.
        for pair in wide {
            canonical.push((pair & 0xFF) as u8);
            canonical.push(((pair >> 8) & 0xFF) as u8);
        }
        LocalPathRepr::new(canonical)
    }
}

impl TryFrom<&LocalPathRepr> for PathBuf {
    type Error = PlatformError;
    fn try_from(path: &LocalPathRepr) -> Result<PathBuf, Self::Error> {
        let bytes = path.as_ref();
        if bytes.len() % 2 != 0 {
            return Err(PlatformError::PathConversionError(anyhow!(
                "expected an even number of bytes, got {0}",
                bytes.len()
            )));
        }
        let mut wide: Vec<u16> = Vec::with_capacity(bytes.len() / 2);
        for pair in bytes.chunks_exact(2) {
            wide.push((pair[0] as u16) | ((pair[1] as u16) << 8));
        }
        let str = std::ffi::OsString::from_wide(&wide);
        Ok(PathBuf::from(str))
    }
}

/// Returns Ok() iff the directory has p_sid as its only accessor.
fn validate_permissions(
    path: &std::path::Path,
    p_u16_path: PCWSTR,
    p_sid: PSID,
) -> anyhow::Result<()> {
    unsafe {
        let flags = DACL_SECURITY_INFORMATION;
        let mut p_actual = PSECURITY_DESCRIPTOR::default();
        let code = GetNamedSecurityInfoW(
            p_u16_path,
            SE_FILE_OBJECT,
            flags,
            None,
            None,
            None,
            None,
            &mut p_actual,
        );
        if code.is_err() {
            let err: windows::core::Error = code.into();
            Err(err).context(format!("GetNamedSecurityInfoW({:?}) failed", &path))?;
        }
        let p_actual = Tow::ptr_to_hlocal(p_actual);

        // Manually ensure that we got the right Dacl.
        let mut has_dacl = BOOL::from(false);
        let mut defaulted = BOOL::from(true);
        let mut p_dacls: *mut ACL = std::ptr::null_mut::<ACL>();
        GetSecurityDescriptorDacl(*p_actual, &mut has_dacl, &mut p_dacls, &mut defaulted)
            .context("GetSecurityDescriptorDacl() failed")?;
        if !has_dacl.as_bool() || defaulted.as_bool() {
            return Err(anyhow!("Expected directory to have an un-inherited DACL"));
        }
        if p_dacls.is_null() || (*p_dacls).AceCount != 1 {
            return Err(anyhow!("Dacl must have exactly one entry"));
        }
        let mut p_ace: *mut c_void = std::ptr::null_mut();
        GetAce(p_dacls, 0, &mut p_ace).context("GetAce() failed")?;

        let ace = &*(p_ace as *const ACE_HEADER);
        if ace.AceType as u32 != ACCESS_ALLOWED_ACE_TYPE {
            return Err(anyhow!("Only Dacl entry must be ACCESS_ALLOWED"));
        }

        let ace = &mut *(p_ace as *mut ACCESS_ALLOWED_ACE);
        let actual_sid = PSID(&mut ace.SidStart as *mut u32 as *mut _);
        EqualSid(actual_sid, p_sid).context("Unexpected SID access")?;
    }
    Ok(())
}

/// Get the current user's PSID.
fn get_user_psid() -> anyhow::Result<Tow<HLOCAL, PSID>> {
    unsafe {
        // Get the current process token
        let mut token_handle = HANDLE::default();
        OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token_handle)
            .context("OpenProcessToken failed")?;
        let token_handle = Owned::new(token_handle); // Ensure deletion.

        // Fetch the user information. Requires two calls to GetTokenInformation.
        let mut bytes_required = 0;
        let result = GetTokenInformation(*token_handle, TokenUser, None, 0, &mut bytes_required);
        match &result {
            Ok(_) => {
                return Err(anyhow!(
                    "initial GetTokenInformation() succeeded unexpectedly"
                ));
            }
            Err(err) => {
                // For some reason the first call always fails but fills in the desired size...
                if err.code() != TOO_SMALL {
                    result.context("initial GetTokenInformation() failed")?;
                }
            }
        };

        let b_token_user = Owned::new(
            LocalAlloc(
                windows::Win32::System::Memory::LPTR,
                bytes_required as usize,
            )
            .context("LocalAlloc of buffer failed")?,
        );
        let mut return_length = 0;
        GetTokenInformation(
            *token_handle,
            TokenUser,
            Some((*b_token_user).0 as *mut _),
            bytes_required,
            &mut return_length,
        )
        .context("GetTokenInformation failed")?;
        let p_token_user = &*((*b_token_user).0 as *const TOKEN_USER);
        // The PSID is inside the buffer (and is not castable to a SID).
        Ok(Tow::<HLOCAL, PSID>::ptr_and_buffer(
            p_token_user.User.Sid,
            b_token_user,
        ))
    }
}

/// Convert a PSID into its SDDL string representation.
fn psid_to_sddl(sid: PSID) -> anyhow::Result<Tow<HLOCAL, PSTR>> {
    unsafe {
        // Convert PSID to an ASCII string, so that we can quickly do
        // string formatting on it.
        let mut p_sid_string = PSTR::null();
        ConvertSidToStringSidA(sid, &mut p_sid_string).context("ConvertSidToStringSid failed")?;
        // Ensure scoped deallocation.
        Ok(Tow::ptr_to_hlocal(p_sid_string))
    }
}

// Helper trait that extracts the raw pointer from a windows type so we
// can wrap it into an HLOCAL or such as appropriate.
trait Castable {
    fn raw_ptr(&self) -> *mut c_void;
}

// Helper type which keeps both an owned buffer and the pointer type
// it's expected to be seen as.
struct Tow<O: windows_core::Free, P> {
    _owner: Owned<O>,
    pointer: P,
}

impl<P: Castable> Tow<HLOCAL, P> {
    // Build a Tow from a pointer _at the beginning_ of an HLOCAL.
    pub fn ptr_to_hlocal(pointer: P) -> Tow<HLOCAL, P> {
        Tow {
            _owner: unsafe { Owned::new(HLOCAL(pointer.raw_ptr())) },
            pointer,
        }
    }
}

impl<O: windows_core::Free, P> Tow<O, P> {
    /// Build a Tow from a pointer and a buffer. The pointer must be
    /// in the buffer, but not necessarily at the beginning.
    ///
    /// See Tow::ptr_to_hlocal() for the special case of a pointer at the
    /// start of an HLOCAL-allocated buffer.
    pub fn ptr_and_buffer(pointer: P, buffer: Owned<O>) -> Tow<O, P> {
        Tow {
            _owner: buffer,
            pointer,
        }
    }
}

impl<O: windows_core::Free, T> std::ops::Deref for Tow<O, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.pointer
    }
}

impl Castable for PSECURITY_DESCRIPTOR {
    fn raw_ptr(&self) -> *mut c_void {
        self.0
    }
}

impl Castable for PSID {
    fn raw_ptr(&self) -> *mut c_void {
        self.0
    }
}

impl Castable for PSTR {
    fn raw_ptr(&self) -> *mut c_void {
        self.as_ptr() as *mut _
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        borrow::Cow,
        fs,
        io::{Read, Seek, Write},
        os::windows::io::AsRawHandle,
    };
    use test_log::test;
    use windows::Win32::System::{
        IO::DeviceIoControl,
        Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_SPARSE, FSCTL_SET_ZERO_DATA},
    };

    #[test]
    fn iterate_over_all_sizes_and_shapes() -> anyhow::Result<()> {
        let payload = b"1234567890";
        let only_data = [data(1, payload)];
        let only_hole = [hole(2)];
        let hole_data = [hole(1), data(1, payload)];

        let mut data_block = vec![0_u8; BLOCK_SIZE];
        data_block[..payload.len()].copy_from_slice(&payload[..]);
        let data_hole = [data(1, data_block.as_slice()), hole(1)];

        for scenario in [
            &only_data[..],
            &only_hole[..],
            &data_hole[..],
            &hole_data[..],
        ] {
            // Each scenario is done in its own temp file.
            let parent = tempfile::TempDir::new()?;
            let file = parent.path().join("file");
            write_sections(&file, scenario)?;
            let metadata = fs::metadata(&file)?;
            tracing::info!("{:?} = {} for {:?}", &file, metadata.len(), &scenario);

            let sections = read_sections(&file)?;
            assert_eq!(scenario, sections.as_slice());
        }

        Ok(())
    }

    const BLOCK_SIZE: usize = 65536;

    #[derive(Debug, PartialEq)]
    struct Section<'a> {
        tp: SectionType<'a>,
        blocks: u8,
    }
    #[derive(Debug, PartialEq)]
    enum SectionType<'a> {
        Hole,
        Data(Data<'a>),
    }
    #[derive(PartialEq)]
    struct Data<'a> {
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

    fn hole(blocks: u8) -> Section<'static> {
        Section {
            tp: SectionType::Hole,
            blocks,
        }
    }

    fn data<'a>(blocks: u8, contents: &'a [u8]) -> Section<'a> {
        Section {
            tp: SectionType::Data(Data {
                contents: Cow::Borrowed(contents),
            }),
            blocks,
        }
    }

    fn owned_data(blocks: u8, contents: Vec<u8>) -> Section<'static> {
        Section {
            tp: SectionType::Data(Data {
                contents: Cow::Owned(contents),
            }),
            blocks,
        }
    }

    fn read_sections(file: &Path) -> anyhow::Result<Vec<Section<'static>>> {
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
            let block_count =
                next.size / BLOCK_SIZE + if next.size % BLOCK_SIZE > 0 { 1 } else { 0 };
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

    fn write_sections(file: &Path, sections: &[Section]) -> anyhow::Result<()> {
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
}
