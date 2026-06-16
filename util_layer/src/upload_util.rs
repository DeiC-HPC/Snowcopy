use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(unix)]
pub fn read_at_offset(file: &File, buf: &mut[u8], offset: u64) -> std::io::Result<usize> {
    file.read_at(buf, offset)
}

#[cfg(windows)]
pub fn read_at_offset(file: &File, buf: &mut[u8], offset: u64) -> std::io::Result<usize> {
    file.seek_read(buf, offset)
}
