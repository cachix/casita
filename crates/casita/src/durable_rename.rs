//! Atomic file replacement whose new name survives power loss.

use std::io;
use std::path::Path;

/// Rename `from` over `to`, replacing any existing file.
///
/// The caller must flush the file's data first. Unix has no per-rename flush:
/// callers sync the parent directory. Windows cannot open directories to flush
/// them, so the renamed file is flushed instead. That relies on NTFS keeping
/// the new name in the file's own metadata, which the flush commits; Windows
/// documents no stronger rename durability. The rename uses POSIX semantics,
/// so it replaces a destination other processes still have open.
///
/// A failed Windows flush returns an error after `to` was already replaced,
/// the same outcome as a failed directory sync after the rename on Unix.
pub(crate) fn replace(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)?;
    #[cfg(windows)]
    std::fs::OpenOptions::new()
        .write(true)
        .open(to)?
        .sync_all()?;
    Ok(())
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
        replace(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
        assert!(!from.exists());
    }

    #[test]
    fn creates_a_missing_destination() {
        let directory = tempfile::tempdir().unwrap();
        let from = directory.path().join("new");
        let to = directory.path().join("absent");
        std::fs::write(&from, b"new").unwrap();
        replace(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }

    /// Concurrent processes replace shared marker files while others read
    /// them; Windows must not refuse a destination that is still open.
    #[test]
    fn replaces_a_destination_another_handle_has_open() {
        let directory = tempfile::tempdir().unwrap();
        let from = directory.path().join("new");
        let to = directory.path().join("current");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"old").unwrap();
        let reader = std::fs::File::open(&to).unwrap();
        replace(&from, &to).unwrap();
        drop(reader);
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }

    #[test]
    fn a_missing_source_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let error = replace(
            &directory.path().join("missing"),
            &directory.path().join("to"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    /// Paths past MAX_PATH must still work on Windows.
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
        replace(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }
}
