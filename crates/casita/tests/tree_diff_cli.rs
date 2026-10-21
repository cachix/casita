#![cfg(feature = "cli")]

use std::process::{Command, Output};

use casita::experimental::{
    Directory, MetadataStore as _, Node, ObjectKey, PathComponent, Repository, RootChange,
    RootName, SymlinkTarget,
};
use serde_json::Value;

fn directory(entries: &[(&str, Node)]) -> Directory {
    Directory::try_from_iter(
        entries
            .iter()
            .map(|(name, node)| (PathComponent::try_from(*name).unwrap(), node.clone())),
    )
    .unwrap()
}

fn dir_node(directory: &Directory) -> Node {
    Node::Directory {
        digest: directory.digest(),
        size: directory.size(),
    }
}

fn symlink(target: &str) -> Node {
    Node::Symlink {
        target: SymlinkTarget::try_from(target).unwrap(),
    }
}

async fn fixture() -> (tempfile::TempDir, ObjectKey, ObjectKey) {
    let temp = tempfile::tempdir().unwrap();
    let repository = Repository::local(temp.path()).await.unwrap();
    let session = repository.mutation_session().await.unwrap();
    let first = session
        .stage_blob_reader(&mut &b"before"[..])
        .await
        .unwrap();
    let second = session.stage_blob_reader(&mut &b"after"[..]).await.unwrap();
    let first_file = Node::File {
        digest: first.record().payload(),
        size: first.record().payload_size(),
        executable: false,
    };
    let second_file = Node::File {
        digest: second.record().payload(),
        size: second.record().payload_size(),
        executable: false,
    };
    let mut executable = first_file.clone();
    if let Node::File { executable, .. } = &mut executable {
        *executable = true;
    }
    let empty = Directory::new();
    let populated = directory(&[("leaf", first_file.clone())]);
    let nested_old = directory(&[
        ("changed", first_file.clone()),
        ("removed", first_file.clone()),
    ]);
    let nested_new = directory(&[
        ("added", second_file.clone()),
        ("changed", second_file.clone()),
    ]);
    let mut old = directory(&[
        ("content", first_file.clone()),
        ("deleted", first_file.clone()),
        ("empty-removed", dir_node(&empty)),
        ("link", symlink("../old")),
        ("mode", first_file.clone()),
        ("nested", dir_node(&nested_old)),
        ("removed-dir", dir_node(&populated)),
        ("to-dir", first_file.clone()),
        ("to-file", dir_node(&populated)),
        ("to-link", first_file.clone()),
        ("unchanged", dir_node(&populated)),
    ]);
    let mut new = directory(&[
        ("added", second_file.clone()),
        ("content", second_file.clone()),
        ("empty-added", dir_node(&empty)),
        ("link", symlink("../new")),
        ("mode", executable),
        ("nested", dir_node(&nested_new)),
        ("new-dir", dir_node(&populated)),
        ("to-dir", dir_node(&populated)),
        ("to-file", second_file.clone()),
        ("to-link", symlink("../target")),
        ("unchanged", dir_node(&populated)),
    ]);
    // These names need no native filesystem support: a canonical tree carries
    // exact bytes on every platform, even when checkout could not create them.
    for name in [b"line\nbreak".as_slice(), b"raw-\xff", b"raw-\xfe"] {
        let name = PathComponent::try_from(bytes::Bytes::copy_from_slice(name)).unwrap();
        old.add(name.clone(), first_file.clone()).unwrap();
        new.add(name, second_file.clone()).unwrap();
    }
    let binary_target = Node::Symlink {
        target: SymlinkTarget::try_from(bytes::Bytes::from_static(b"../\xff")).unwrap(),
    };
    new.add(
        PathComponent::try_from("binary-link").unwrap(),
        binary_target,
    )
    .unwrap();
    let old_key = ObjectKey::directory(old.digest());
    let new_key = ObjectKey::directory(new.digest());
    let mut staged = vec![first, second];
    for tree in [&empty, &populated, &nested_old, &nested_new, &old, &new] {
        staged.push(session.stage_directory(tree).await.unwrap());
    }
    session
        .publish(
            staged,
            vec![
                RootChange::Set {
                    name: RootName::try_from("before").unwrap(),
                    target: old_key.clone(),
                },
                RootChange::Set {
                    name: RootName::try_from("after").unwrap(),
                    target: new_key.clone(),
                },
            ],
        )
        .await
        .unwrap();
    drop(session);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    (temp, old_key, new_key)
}

fn diff(repository: &std::path::Path, old: &str, new: &str, json: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_casita"));
    command
        .arg("--repository")
        .arg(repository)
        .args(["tree", "diff", old, new]);
    if json {
        command.arg("--json");
    }
    command.output().unwrap()
}

fn rows(output: &Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn recursive_diff_reports_content_modes_links_and_type_changes_without_mutation() {
    let (temp, old, new) = fixture().await;
    let repository = Repository::local(temp.path()).await.unwrap();
    let revision = repository.metadata().snapshot().await.unwrap().revision();
    let changes = rows(&diff(temp.path(), &old.to_string(), &new.to_string(), true));
    let actual: Vec<_> = changes
        .iter()
        .map(|row| (row["status"].as_str().unwrap(), row["path"].as_str()))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("A", Some("added")),
            ("A", Some("binary-link")),
            ("M", Some("content")),
            ("D", Some("deleted")),
            ("A", Some("empty-added")),
            ("D", Some("empty-removed")),
            ("M", Some("line\nbreak")),
            ("M", Some("link")),
            ("M", Some("mode")),
            ("A", Some("nested/added")),
            ("M", Some("nested/changed")),
            ("D", Some("nested/removed")),
            ("A", Some("new-dir")),
            ("A", Some("new-dir/leaf")),
            ("M", None),
            ("M", None),
            ("D", Some("removed-dir")),
            ("D", Some("removed-dir/leaf")),
            ("T", Some("to-dir")),
            ("A", Some("to-dir/leaf")),
            ("T", Some("to-file")),
            ("D", Some("to-file/leaf")),
            ("T", Some("to-link")),
        ]
    );
    for row in &changes {
        assert_eq!(row["schema"], "casita.tree-diff.v1");
        if let Some(path) = row["path"].as_str() {
            assert_eq!(row["path_bytes"], serde_json::json!(path.as_bytes()));
        }
    }
    let find = |path: &str| changes.iter().find(|row| row["path"] == path).unwrap();
    assert!(find("added")["before"].is_null());
    assert!(find("deleted")["after"].is_null());
    assert_ne!(
        find("content")["before"]["key"],
        find("content")["after"]["key"]
    );
    let mode = find("mode");
    assert_eq!(mode["before"]["key"], mode["after"]["key"]);
    assert_eq!(mode["before"]["executable"], false);
    assert_eq!(mode["after"]["executable"], true);
    assert_eq!(find("link")["before"]["target"], "../old");
    assert_eq!(find("link")["after"]["target"], "../new");
    assert!(find("binary-link")["after"]["target"].is_null());
    assert_eq!(
        find("binary-link")["after"]["target_bytes"],
        serde_json::json!(b"../\xff".as_slice())
    );
    assert_eq!(
        changes[14]["path_bytes"],
        serde_json::json!(b"raw-\xfe".as_slice())
    );
    assert_eq!(
        changes[15]["path_bytes"],
        serde_json::json!(b"raw-\xff".as_slice())
    );
    assert_eq!(
        repository.metadata().snapshot().await.unwrap().revision(),
        revision
    );
}

#[tokio::test]
async fn text_diff_escapes_names_and_reverse_diff_reverses_changes() {
    let (temp, old, new) = fixture().await;
    let forward = diff(temp.path(), &old.to_string(), &new.to_string(), false);
    assert!(forward.status.success());
    let text = String::from_utf8(forward.stdout).unwrap();
    assert_eq!(text.lines().count(), 23);
    assert!(text.contains("M\t\"line\\nbreak\"\n"));
    assert!(text.contains("M\t\"raw-\\xff\"\n"));
    assert!(text.contains("M\t\"raw-\\xfe\"\n"));
    assert!(!text.contains("unchanged"));
    let forward = rows(&diff(temp.path(), &old.to_string(), &new.to_string(), true));
    let reverse = rows(&diff(temp.path(), &new.to_string(), &old.to_string(), true));
    for (before, after) in forward.iter().zip(&reverse) {
        assert_eq!(before["path_bytes"], after["path_bytes"]);
        assert_eq!(before["before"], after["after"]);
        assert_eq!(before["after"], after["before"]);
        let expected = match before["status"].as_str().unwrap() {
            "A" => "D",
            "D" => "A",
            other => other,
        };
        assert_eq!(after["status"], expected);
    }
    assert_eq!(forward.len(), reverse.len());
}

#[tokio::test]
async fn identical_short_and_full_keys_are_empty_and_bad_inputs_fail() {
    let (temp, old, new) = fixture().await;
    let short = old.native_digest().unwrap().to_string();
    for json in [false, true] {
        let identical = diff(temp.path(), &short, &old.to_string(), json);
        assert!(identical.status.success());
        assert!(identical.stdout.is_empty());
        assert!(identical.stderr.is_empty());
    }
    let missing = ObjectKey::directory(casita::experimental::DirectoryId::new([99; 32].into()));
    for (before, after) in [
        (old.to_string(), missing.to_string()),
        (missing.to_string(), new.to_string()),
        (missing.to_string(), missing.to_string()),
        (
            ObjectKey::blob(casita::experimental::BlobId::new([1; 32].into())).to_string(),
            new.to_string(),
        ),
        (old.to_string(), "invalid".to_owned()),
    ] {
        let output = diff(temp.path(), &before, &after, true);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[tokio::test]
async fn identical_corrupt_tree_is_rejected_before_output() {
    let (temp, old, _) = fixture().await;
    let packs = temp.path().join("blobs/packs/b3");
    let mut corrupted = 0;
    for shard in std::fs::read_dir(packs).unwrap() {
        for entry in std::fs::read_dir(shard.unwrap().path()).unwrap() {
            let path = entry.unwrap().path();
            std::fs::write(path, b"corrupt").unwrap();
            corrupted += 1;
        }
    }
    assert!(
        corrupted > 0,
        "fixture must corrupt at least one chunk pack"
    );
    let output = diff(temp.path(), &old.to_string(), &old.to_string(), true);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}
