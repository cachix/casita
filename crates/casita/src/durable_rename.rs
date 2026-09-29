//! Atomic file replacement whose new name survives power loss.

use std::io;
use std::path::Path;

/// Rename `from` over `to`, replacing any existing file.
///
/// The caller must flush the file's data first. On Windows the rename itself is
/// written through before this returns, since directories cannot be opened to
/// flush them. Unix has no per-rename flush: callers sync the parent directory.
pub(crate) fn rename_write_through(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        windows::move_file_write_through(from, to)
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(from, to)
    }
}

#[cfg(windows)]
mod windows {
    use std::ffi::OsString;
    use std::io;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::path::{Path, PathBuf};
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    pub(super) fn move_file_write_through(from: &Path, to: &Path) -> io::Result<()> {
        let from = wide(&verbatim(from)?);
        let to = wide(&verbatim(to)?);
        // The only unsafe call in the crate: Rust exposes no rename flags.
        #[allow(unsafe_code)]
        // SAFETY: both pointers are NUL-terminated UTF-16 buffers that outlive
        // the call, and MoveFileExW does not retain them.
        let moved = unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    /// The `\\?\` form, which lifts MAX_PATH as std does for its own calls.
    fn verbatim(path: &Path) -> io::Result<PathBuf> {
        let absolute: Vec<u16> = std::path::absolute(path)?
            .as_os_str()
            .encode_wide()
            .collect();
        let prefixed = if absolute.starts_with(&units(r"\\?\")) {
            absolute
        } else if let Some(share) = absolute.strip_prefix(units(r"\\").as_slice()) {
            [units(r"\\?\UNC\").as_slice(), share].concat()
        } else {
            [units(r"\\?\").as_slice(), &absolute].concat()
        };
        Ok(OsString::from_wide(&prefixed).into())
    }

    fn units(text: &str) -> Vec<u16> {
        text.encode_utf16().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_an_existing_destination() {
        let directory = tempfile::tempdir().unwrap();
        let from = directory.path().join("new");
        let to = directory.path().join("current");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"old").unwrap();
        rename_write_through(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
        assert!(!from.exists());
    }

    #[test]
    fn creates_a_missing_destination() {
        let directory = tempfile::tempdir().unwrap();
        let from = directory.path().join("new");
        let to = directory.path().join("absent");
        std::fs::write(&from, b"new").unwrap();
        rename_write_through(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }

    #[test]
    fn a_missing_source_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let error = rename_write_through(
            &directory.path().join("missing"),
            &directory.path().join("to"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    /// Paths past MAX_PATH need the verbatim form on Windows.
    #[test]
    fn handles_paths_longer_than_max_path() {
        let directory = tempfile::tempdir().unwrap();
        let mut deep = directory.path().to_path_buf();
        while deep.as_os_str().len() < 300 {
            deep.push("a".repeat(40));
        }
        std::fs::create_dir_all(&deep).unwrap();
        let from = deep.join("new");
        let to = deep.join("current");
        std::fs::write(&from, b"new").unwrap();
        rename_write_through(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }
}
