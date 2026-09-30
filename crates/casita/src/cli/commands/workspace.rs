//! Portable attachment of one working tree to the user's global repository.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use casita::experimental::RootName;

use super::{Error, run::RunConfig};

pub(super) const MARKER: &str = ".casita";
const HEADER: &str = "casita-workspace-v1";

/// A project-local identity whose objects and roots live in the global store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Workspace {
    root: PathBuf,
    id: String,
    pub(super) run: RunConfig,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MarkerConfig {
    workspace: String,
    #[serde(default)]
    run: RunConfig,
}

impl Workspace {
    fn read(root: PathBuf) -> Result<Self, Error> {
        let marker = root.join(MARKER);
        let contents = std::fs::read_to_string(&marker)?;
        let Some((header, body)) = contents.split_once('\n') else {
            return Err(invalid_marker(&marker, "unknown format"));
        };
        if header.trim_end_matches('\r') != HEADER {
            return Err(invalid_marker(&marker, "unknown format"));
        }
        let config: MarkerConfig =
            toml::from_str(body).map_err(|error| invalid_marker(&marker, &error.to_string()))?;
        if !is_uuid(&config.workspace) {
            return Err(invalid_marker(&marker, "missing or invalid workspace UUID"));
        }
        config.run.validate()?;
        Ok(Self {
            root,
            id: config.workspace,
            run: config.run,
        })
    }

    /// Directory carrying this portable workspace attachment.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Stable UUID stored in the marker.
    pub(super) fn id(&self) -> &str {
        &self.id
    }

    /// Translate a user-visible root name into this workspace's global scope.
    pub(super) fn root_name(&self, name: &str) -> Result<RootName, Error> {
        Ok(RootName::try_from(format!(
            "workspaces/{}/{name}",
            self.id
        ))?)
    }

    /// Global prefix containing every root owned by this workspace.
    pub(super) fn root_prefix(&self) -> Result<RootName, Error> {
        Ok(RootName::try_from(format!("workspaces/{}", self.id))?)
    }

    /// Translate an optional user prefix, with an empty prefix selecting all
    /// roots in this workspace.
    pub(super) fn scoped_prefix(&self, prefix: &str) -> Result<RootName, Error> {
        if prefix.is_empty() {
            self.root_prefix()
        } else {
            self.root_name(prefix)
        }
    }

    /// Return the user-visible part of a root known to be inside this workspace.
    pub(super) fn display_name<'a>(&self, name: &'a RootName) -> Option<&'a str> {
        let prefix = format!("workspaces/{}/", self.id);
        name.as_str().strip_prefix(&prefix)
    }

    /// Make a workspace marker in `root`, or return the existing marker there.
    pub(super) fn create_at(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = std::fs::canonicalize(root)?;
        let marker = root.join(MARKER);
        if marker.exists() {
            return Self::read(root);
        }

        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)?;
        // A conventional UUID makes the marker convenient to inspect and copy
        // into bug reports, while the random bytes remain the identity source.
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let hex = data_encoding::HEXLOWER.encode(&bytes);
        let id = format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        );
        let contents = format!("{HEADER}\nworkspace = \"{id}\"\n");

        // `create_new` makes competing `init` calls resolve to the one marker
        // that won, rather than silently replacing an existing attachment.
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
        {
            Ok(mut file) => {
                if let Err(error) = (|| -> std::io::Result<()> {
                    file.write_all(contents.as_bytes())?;
                    file.sync_all()?;
                    // The marker is the workspace identity: without its
                    // directory entry, a power loss would orphan its roots.
                    crate::cli::sync_directory(&root)
                })() {
                    let _ = std::fs::remove_file(&marker);
                    return Err(error.into());
                }
                Ok(Self {
                    root,
                    id,
                    run: RunConfig::default(),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Self::read(root),
            Err(error) => Err(error.into()),
        }
    }
}

/// Find the closest enclosing workspace marker, starting at `directory`.
pub(super) fn discover_from(directory: impl AsRef<Path>) -> Result<Option<Workspace>, Error> {
    let mut current = std::fs::canonicalize(directory)?;
    loop {
        let marker = current.join(MARKER);
        if marker.exists() {
            return Workspace::read(current).map(Some);
        }
        if !current.pop() {
            return Ok(None);
        }
    }
}

/// Find the workspace enclosing the command's current directory.
pub(super) fn discover() -> Result<Option<Workspace>, Error> {
    discover_from(std::env::current_dir()?)
}

fn invalid_marker(path: &Path, reason: &str) -> Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "invalid Casita workspace marker {}: {reason}",
            path.display()
        ),
    )
    .into()
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_is_discovered_from_a_descendant_and_scopes_roots() {
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().join("project");
        let nested = project.join("src/nested");
        std::fs::create_dir_all(&nested).unwrap();
        let created = Workspace::create_at(&project).unwrap();
        let found = discover_from(&nested).unwrap().unwrap();

        assert_eq!(created, found);
        assert_eq!(
            found.root_name("main").unwrap().as_str(),
            format!("workspaces/{}/main", found.id())
        );
        assert_eq!(
            found.display_name(&found.root_name("main").unwrap()),
            Some("main")
        );
    }

    #[test]
    fn creation_reuses_an_existing_workspace_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let first = Workspace::create_at(temporary.path()).unwrap();
        let second = Workspace::create_at(temporary.path()).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn creation_flushes_the_directory_holding_the_new_marker() {
        let temporary = tempfile::tempdir().unwrap();
        let workspace = Workspace::create_at(temporary.path()).unwrap();
        assert!(crate::cli::was_synced(workspace.root()));
    }

    #[test]
    fn malformed_marker_is_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(temporary.path().join(MARKER), "not casita\n").unwrap();
        assert!(discover_from(temporary.path()).is_err());
    }
}
