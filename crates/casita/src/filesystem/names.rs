//! Bridging stored byte names and platform filesystem strings.
//!
//! The data model stores names and symlink targets as raw bytes (see
//! [`crate::path`]); the filesystem speaks [`OsStr`]. On Unix the two coincide,
//! so both directions are free and lossless. On Windows filesystem names are
//! Unicode, so they cross as UTF-8 (a tree of UTF-8 names hashes to the same
//! digest on every platform) and non-Unicode names are refused: importing a
//! name with an unpaired surrogate, or materializing stored non-UTF-8 bytes,
//! is an error rather than a silent respelling that would change the digest.

use std::ffi::OsStr;

use crate::error::Error;

/// The stored-bytes form of a filesystem name.
#[cfg(unix)]
pub(crate) fn os_str_bytes(s: &OsStr) -> Result<&[u8], Error> {
    use std::os::unix::ffi::OsStrExt;
    Ok(s.as_bytes())
}

/// The stored-bytes form of a filesystem name.
#[cfg(not(unix))]
pub(crate) fn os_str_bytes(s: &OsStr) -> Result<&[u8], Error> {
    s.to_str().map(str::as_bytes).ok_or_else(|| {
        format!("file name {s:?} is not valid Unicode, which this platform cannot store").into()
    })
}

/// The filesystem-name form of stored bytes.
#[cfg(unix)]
pub(crate) fn os_str_from_bytes(b: &[u8]) -> Result<&OsStr, Error> {
    use std::os::unix::ffi::OsStrExt;
    Ok(OsStr::from_bytes(b))
}

/// The filesystem-name form of stored bytes.
#[cfg(not(unix))]
pub(crate) fn os_str_from_bytes(b: &[u8]) -> Result<&OsStr, Error> {
    std::str::from_utf8(b).map(OsStr::new).map_err(|_| {
        format!(
            "stored name {:?} is not valid UTF-8, which this platform cannot materialize",
            bstr::BStr::new(b)
        )
        .into()
    })
}

#[cfg(any(windows, test))]
const WINDOWS_RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Validate one name against Windows path and device-name rules.
#[cfg(any(windows, test))]
pub(crate) fn check_windows_name(name: &str) -> Result<(), String> {
    for character in name.chars() {
        match character {
            '\\' => return Err("`\\` is a path separator on Windows".into()),
            ':' => return Err("`:` opens a drive or NTFS stream".into()),
            '<' | '>' | '"' | '|' | '?' | '*' => {
                return Err(format!(
                    "`{character}` is not allowed in Windows file names"
                ));
            }
            '\0'..='\x1f' => {
                return Err(format!(
                    "control character {:#04x} is not allowed in Windows file names",
                    character as u32
                ));
            }
            _ => {}
        }
    }
    if name.ends_with('.') || name.ends_with(' ') {
        return Err("Windows strips a trailing dot or space".into());
    }
    let base = name.split('.').next().unwrap_or(name);
    if WINDOWS_RESERVED_NAMES
        .iter()
        .any(|reserved| base.eq_ignore_ascii_case(reserved))
    {
        return Err(format!("`{base}` is a reserved device name on Windows"));
    }
    Ok(())
}

/// Validate stored name bytes materialized below `directory` on Windows,
/// returning the name as text.
#[cfg(windows)]
pub(crate) fn check_windows_stored_name<'a>(
    name: &'a [u8],
    directory: &std::path::Path,
) -> Result<&'a str, Error> {
    let name = std::str::from_utf8(name).map_err(|_| -> Error {
        format!(
            "stored name {} is not valid UTF-8, which Windows cannot materialize",
            bstr::BStr::new(name)
        )
        .into()
    })?;
    check_windows_name(name).map_err(|reason| -> Error {
        format!(
            "cannot materialize `{name}` under {}: {reason}",
            directory.display()
        )
        .into()
    })?;
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_names_round_trip() {
        for name in ["a.txt", "with space", "ünîcøde"] {
            let bytes = os_str_bytes(OsStr::new(name)).unwrap();
            assert_eq!(bytes, name.as_bytes());
            assert_eq!(os_str_from_bytes(bytes).unwrap(), OsStr::new(name));
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_passes_non_utf8_through() {
        use std::os::unix::ffi::OsStrExt;
        let raw = b"caf\xff";
        let os = OsStr::from_bytes(raw);
        assert_eq!(os_str_bytes(os).unwrap(), raw);
        assert_eq!(os_str_from_bytes(raw).unwrap(), os);
    }

    #[cfg(not(unix))]
    #[test]
    fn non_utf8_bytes_are_refused() {
        assert!(os_str_from_bytes(b"caf\xff").is_err());
    }

    #[test]
    fn windows_name_rules() {
        for name in ["file.txt", "ünîcøde", "com10.txt"] {
            assert!(check_windows_name(name).is_ok(), "{name}");
        }
        for name in [
            "CON",
            "con.txt",
            "Lpt9.log",
            "file.",
            "file ",
            "dir\\file",
            "file:stream",
            "file?",
            "file\x01",
        ] {
            assert!(check_windows_name(name).is_err(), "{name}");
        }
    }
}
