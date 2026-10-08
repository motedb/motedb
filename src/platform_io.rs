//! 跨平台 positional read。
//!
//! `std::os::unix::fs::FileExt::read_exact_at` 是 unix-only trait 方法
//! (Windows 的 `FileExt` 只有 `seek_read`/`seek_write`),索引存储
//! (B+Tree / ioctree / leaf_store) 直接依赖它导致 crate 无法在 Windows
//! 上编译。本模块提供同名的 `PositionalRead` trait —— 调用点的
//! `file.read_exact_at(&mut buf, offset)` 方法语法保持不变,只需把
//! `use std::os::unix::fs::FileExt;` 换成
//! `use crate::platform_io::PositionalRead as _;`。
//!
//! - unix: `FileExt::read_at`(处理短读循环)
//! - windows: `FileExt::seek_read`(同语义的 positional read)
//! - 其它目标: try_clone + seek + read 兜底(每调用一次 clone,仅供
//!   冷门平台编译通过,不在支持矩阵内)

use std::io::ErrorKind;

/// Positional (pread-style) reads for [`std::fs::File`].
pub trait PositionalRead {
    /// Single positional read: fills at most `buf.len()` bytes starting at
    /// `offset`, returns bytes actually read (0 ⇒ EOF). Does not move the
    /// shared file cursor.
    fn pread_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;

    /// Positional read that loops until the buffer is full or EOF
    /// (errors with [`ErrorKind::UnexpectedEof`] on a short read).
    fn read_exact_at(&self, buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
        let mut done = 0usize;
        while done < buf.len() {
            let n = self.pread_at(&mut buf[done..], offset)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "read_exact_at hit EOF",
                ));
            }
            done += n;
            offset += n as u64;
        }
        Ok(())
    }
}

impl PositionalRead for std::fs::File {
    #[cfg(unix)]
    fn pread_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.read_at(buf, offset)
    }

    #[cfg(windows)]
    fn pread_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.seek_read(buf, offset)
    }

    #[cfg(not(any(unix, windows)))]
    fn pread_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        let mut clone = self.try_clone()?;
        clone.seek(SeekFrom::Start(offset))?;
        clone.read(buf)
    }
}
