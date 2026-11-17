#![cfg(feature = "cli")]

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use casita::experimental::{MetadataStore, RepositoryRevision};
use casita::{ObjectKey, Repository, RootName, RootRetention};

struct Fixture {
    directory: tempfile::TempDir,
    path: PathBuf,
    keys: [String; 3],
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repository");
        let repository = Repository::local(&path).await.unwrap();
        let mut keys = Vec::new();
        for (index, bytes) in [b"first".as_slice(), b"second", b"third"]
            .iter()
            .enumerate()
        {
            keys.push(
                repository
                    .import(casita::import::BlobImport::new(
                        *bytes,
                        RootName::try_from(format!("fixtures/{index}")).unwrap(),
                    ))
                    .await
                    .unwrap()
                    .to_string(),
            );
        }
        repository.flush().await.unwrap();
        drop(repository);
        Self {
            directory,
            path,
            keys: keys.try_into().unwrap(),
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_casita"));
        command
            .current_dir(self.directory.path())
            .env("RUST_LOG", "off")
            .arg("--repository")
            .arg(&self.path)
            .args(args);
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    async fn root(&self, name: &str) -> (Option<ObjectKey>, Option<RootRetention>) {
        let repository = Repository::local(&self.path).await.unwrap();
        let name = RootName::try_from(name).unwrap();
        let result = (
            repository.root(&name).await.unwrap(),
            repository.root_retention(&name).await.unwrap(),
        );
        repository.flush().await.unwrap();
        result
    }

    async fn revision(&self) -> RepositoryRevision {
        let repository = casita::experimental::Repository::local(&self.path)
            .await
            .unwrap();
        let revision = repository.metadata().snapshot().await.unwrap().revision();
        repository.flush().await.unwrap();
        revision
    }
}

fn success(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn conflict(output: Output) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("error[destination_conflict]"), "{error}");
    assert!(error.contains("nothing published"), "{error}");
}

#[tokio::test]
async fn conditional_create_and_replace_keep_or_atomically_change_retention() {
    let fixture = Fixture::new().await;
    let [first, second, third] = &fixture.keys;
    success(fixture.run(&[
        "root",
        "set",
        "build/current",
        first,
        "--if-absent",
        "--retention",
        "evictable",
    ]));
    assert_eq!(
        fixture.root("build/current").await,
        (Some(first.parse().unwrap()), Some(RootRetention::Evictable))
    );
    success(fixture.run(&["root", "set", "build/current", second, "--if-target", first]));
    assert_eq!(
        fixture.root("build/current").await,
        (
            Some(second.parse().unwrap()),
            Some(RootRetention::Evictable)
        )
    );
    success(fixture.run(&[
        "root",
        "set",
        "build/current",
        third,
        "--if-target",
        second,
        "--retention",
        "permanent",
    ]));
    assert_eq!(
        fixture.root("build/current").await,
        (Some(third.parse().unwrap()), Some(RootRetention::Permanent))
    );
    // Unconditional replacement remains available and keeps the policy.
    success(fixture.run(&["root", "set", "build/current", first]));
    assert_eq!(
        fixture.root("build/current").await.0,
        Some(first.parse().unwrap())
    );
}

#[tokio::test]
async fn mismatches_and_invalid_inputs_leave_the_root_policy_and_revision_unchanged() {
    let fixture = Fixture::new().await;
    let [first, second, third] = &fixture.keys;
    success(fixture.run(&[
        "root",
        "set",
        "build/current",
        first,
        "--retention",
        "evictable",
    ]));
    let before = fixture.revision().await;
    conflict(fixture.run(&[
        "root",
        "set",
        "build/current",
        second,
        "--if-absent",
        "--retention",
        "permanent",
    ]));
    conflict(fixture.run(&[
        "root",
        "set",
        "build/current",
        second,
        "--if-target",
        third,
        "--retention",
        "permanent",
    ]));
    conflict(fixture.run(&["root", "set", "missing", second, "--if-target", first]));
    let missing =
        ObjectKey::blob(casita::BlobId::new(casita::Digest::hash(b"not imported"))).to_string();
    for output in [
        fixture.run(&[
            "root",
            "set",
            "build/current",
            second,
            "--if-target",
            "invalid",
        ]),
        fixture.run(&[
            "root",
            "set",
            "build/current",
            &missing,
            "--if-target",
            first,
        ]),
        fixture.run(&[
            "root",
            "set",
            "build/current",
            second,
            "--if-target",
            first,
            "--if-absent",
        ]),
    ] {
        assert!(!output.status.success());
    }
    assert_eq!(fixture.revision().await, before);
    assert_eq!(
        fixture.root("build/current").await,
        (Some(first.parse().unwrap()), Some(RootRetention::Evictable))
    );
    assert_eq!(fixture.root("missing").await, (None, None));
}

#[tokio::test]
async fn competing_cli_publishers_have_one_winner_for_create_and_replace() {
    let fixture = Fixture::new().await;
    for expected in [None, Some(fixture.keys[0].as_str())] {
        if let Some(expected) = expected {
            success(fixture.run(&["root", "set", "race", expected]));
        }
        let mut children = Vec::new();
        for target in &fixture.keys[1..] {
            let mut args = vec!["root", "set", "race", target];
            match expected {
                Some(expected) => args.extend(["--if-target", expected]),
                None => args.push("--if-absent"),
            }
            children.push((
                target,
                fixture
                    .command(&args)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            ));
        }
        let mut winner = None;
        for (target, child) in children {
            let output = child.wait_with_output().unwrap();
            if output.status.success() {
                assert!(winner.replace(target).is_none(), "both writers committed");
            } else {
                conflict(output);
            }
        }
        assert_eq!(
            fixture.root("race").await.0,
            Some(winner.unwrap().parse().unwrap())
        );
        success(fixture.run(&["root", "rm", "race"]));
    }
}
