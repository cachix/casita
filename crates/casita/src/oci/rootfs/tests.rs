use std::io::Cursor;

use ::tar::{Builder, EntryType, Header};
use tokio::io::AsyncWriteExt;

use super::*;
use crate::repository::Repository;
use crate::{BlobId, Digest, MemoryBlobStore, MemoryMetadataStore};

enum Item<'a> {
    File(&'a str, &'a [u8]),
    Directory(&'a str),
    Link(&'a str, &'a str),
    Symlink(&'a str, &'a str),
    Device(&'a str),
}

async fn tar(items: &[Item<'_>]) -> Vec<u8> {
    let mut builder = Builder::new(Vec::new());
    for item in items {
        let mut header = Header::new_ustar();
        header.set_mode(0o755);
        let (path, body, kind, target) = match item {
            Item::File(path, body) => (*path, *body, EntryType::Regular, None),
            Item::Directory(path) => (*path, &[][..], EntryType::Directory, None),
            Item::Link(path, target) => (*path, &[][..], EntryType::Link, Some(*target)),
            Item::Symlink(path, target) => (*path, &[][..], EntryType::Symlink, Some(*target)),
            Item::Device(path) => (*path, &[][..], EntryType::Char, None),
        };
        header.set_entry_type(kind);
        header.set_size(body.len() as u64);
        if let Some(target) = target {
            header.set_link_name(target).unwrap();
        }
        builder.append_data(&mut header, path, body).unwrap();
    }
    builder.into_inner().unwrap()
}

fn diff_id(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn file(tree: &Tree, name: &str, expected: &[u8]) {
    let Some(Entry::Leaf(Node::File {
        digest, executable, ..
    })) = tree.entries.get(&path(name.as_bytes(), 4096).unwrap())
    else {
        panic!("missing file {name}")
    };
    assert_eq!(*digest, BlobId::new(Digest::hash(expected)), "{name}");
    assert!(*executable, "{name}");
}

fn repository() -> Repository<MemoryBlobStore, MemoryMetadataStore> {
    Repository::memory().unwrap()
}

#[tokio::test]
async fn merges_whiteouts_replacements_and_links_without_following_symlinks() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let mut tree = Tree::default();
    let lower = tar(&[
        Item::Directory("./"),
        Item::Directory("empty"),
        Item::File("etc/old", b"old"),
        Item::File("cache/lower/deep", b"hidden"),
        Item::File("keep/sub/data", b"kept"),
        Item::File("replace/child", b"gone"),
        Item::File("flip", b"file becomes directory"),
        Item::File("deleted/tree/file", b"deleted"),
        Item::File("same", b"lower"),
        Item::File("original", b"original inode"),
        Item::Link("stable", "original"),
    ])
    .await;
    tree.apply(
        &mutation,
        Cursor::new(&lower),
        &diff_id(&lower),
        OciRootfsLimits::default(),
    )
    .await
    .unwrap();
    let upper = tar(&[
        Item::File("cache/new", b"new"),
        Item::File("same", b"upper"),
        Item::File("replace", b"directory becomes file"),
        Item::Directory("flip"),
        Item::File("flip/child", b"child"),
        Item::File("etc/old", b"changed"),
        Item::File("etc/.wh.old", b""),
        Item::File(".wh.deleted", b""),
        Item::File(".wh.same", b""),
        Item::File("cache/.wh..wh..opq", b""),
        Item::File(".wh.original", b""),
        Item::Link("from_lower", "keep/sub/data"),
        Item::Link("forward", "target"),
        Item::Link("chain", "forward"),
        Item::File("target", b"forward target"),
        Item::Symlink("absolute", "/etc/old"),
    ])
    .await;
    tree.apply(
        &mutation,
        Cursor::new(&upper),
        &diff_id(&upper),
        OciRootfsLimits::default(),
    )
    .await
    .unwrap();
    for name in ["cache/lower", "deleted", "replace/child", "original"] {
        assert!(
            !tree
                .entries
                .contains_key(&path(name.as_bytes(), 4096).unwrap()),
            "{name}"
        );
    }
    file(&tree, "cache/new", b"new");
    file(&tree, "same", b"upper");
    file(&tree, "etc/old", b"changed");
    file(&tree, "replace", b"directory becomes file");
    file(&tree, "stable", b"original inode");
    file(&tree, "from_lower", b"kept");
    file(&tree, "forward", b"forward target");
    file(&tree, "chain", b"forward target");
    assert!(
        matches!(tree.entries.get(&path(b"absolute", 4096).unwrap()), Some(Entry::Leaf(Node::Symlink { target })) if target.as_bytes() == b"/etc/old")
    );
    assert!(
        !tree
            .entries
            .keys()
            .any(|path| path.iter().any(|part| part.as_bytes().starts_with(b".wh.")))
    );
    let root = tree.finish(&mutation).await.unwrap();
    // Native Windows checkout rejects absolute symlink targets.
    #[cfg(unix)]
    {
        let output = tempfile::tempdir().unwrap();
        repository.checkout(&root, output.path()).await.unwrap();
        assert_eq!(
            std::fs::read(output.path().join("forward")).unwrap(),
            b"forward target"
        );
        assert!(output.path().join("empty").is_dir());
    }
    #[cfg(not(unix))]
    let _ = root;
}

#[tokio::test]
async fn root_opaque_whiteout_preserves_same_layer_entries() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let mut tree = Tree::default();
    for items in [
        vec![Item::File("old/deep", b"old")],
        vec![Item::File("new", b"new"), Item::File(".wh..wh..opq", b"")],
    ] {
        let bytes = tar(&items).await;
        tree.apply(
            &mutation,
            Cursor::new(&bytes),
            &diff_id(&bytes),
            OciRootfsLimits::default(),
        )
        .await
        .unwrap();
    }
    assert_eq!(tree.entries.len(), 1);
    file(&tree, "new", b"new");
}

#[tokio::test]
async fn rejects_invalid_layer_structure_and_uncompressed_digests() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    for items in [
        vec![Item::File("dup", b"a"), Item::File("dup", b"b")],
        vec![Item::File(".wh.bad", b"data")],
        vec![Item::File(".wh.", b"")],
        vec![Item::Device("device")],
        vec![
            Item::Symlink("parent", "/outside"),
            Item::File("parent/file", b"no"),
        ],
        vec![Item::Link("a", "b"), Item::Link("b", "a")],
        vec![Item::Link("a", "missing")],
    ] {
        let bytes = tar(&items).await;
        assert!(
            Tree::default()
                .apply(
                    &mutation,
                    Cursor::new(&bytes),
                    &diff_id(&bytes),
                    OciRootfsLimits::default()
                )
                .await
                .is_err()
        );
    }
    let mut unsafe_header = Header::new_ustar();
    unsafe_header.set_size(0);
    unsafe_header.set_mode(0o644);
    unsafe_header.as_mut_bytes()[..10].copy_from_slice(b"../outside");
    unsafe_header.set_cksum();
    let mut builder = Builder::new(Vec::new());
    builder.append(&unsafe_header, &[][..]).unwrap();
    let unsafe_tar = builder.into_inner().unwrap();
    assert!(
        Tree::default()
            .apply(
                &mutation,
                Cursor::new(&unsafe_tar),
                &diff_id(&unsafe_tar),
                OciRootfsLimits::default()
            )
            .await
            .is_err()
    );
    let valid = tar(&[Item::File("safe", b"body")]).await;
    let wrong = format!("sha256:{}", "0".repeat(64));
    assert!(
        Tree::default()
            .apply(
                &mutation,
                Cursor::new(&valid),
                &wrong,
                OciRootfsLimits::default()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn refuses_lower_symlink_traversal_and_hidden_hardlink_targets() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    for (lower_items, upper_items) in [
        (
            vec![Item::Symlink("parent", "/outside")],
            vec![Item::File("parent/file", b"data")],
        ),
        (
            vec![Item::File("original", b"old")],
            vec![
                Item::File(".wh.original", b""),
                Item::Link("link", "original"),
            ],
        ),
    ] {
        let mut tree = Tree::default();
        let lower = tar(&lower_items).await;
        let upper = tar(&upper_items).await;
        tree.apply(
            &mutation,
            Cursor::new(&lower),
            &diff_id(&lower),
            OciRootfsLimits::default(),
        )
        .await
        .unwrap();
        assert!(
            tree.apply(
                &mutation,
                Cursor::new(&upper),
                &diff_id(&upper),
                OciRootfsLimits::default()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn rejects_pax_sparse_files_instead_of_storing_sparse_payload_as_content() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let mut builder = Builder::new(Vec::new());
    let extension = b"22 GNU.sparse.map=0,1\n";
    let mut header = Header::new_ustar();
    header.set_entry_type(EntryType::XHeader);
    header.set_path("PaxHeaders/sparse").unwrap();
    header.set_size(extension.len() as u64);
    header.set_cksum();
    builder.append(&header, &extension[..]).unwrap();
    let mut header = Header::new_ustar();
    header.set_size(0);
    header.set_mode(0o644);
    builder.append_data(&mut header, "sparse", &[][..]).unwrap();
    let bytes = builder.into_inner().unwrap();
    let error = Tree::default()
        .apply(
            &mutation,
            Cursor::new(&bytes),
            &diff_id(&bytes),
            OciRootfsLimits::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, OciImportError::Unsupported(_)), "{error}");
}

#[tokio::test]
async fn enforces_decoded_file_and_tree_bounds_at_the_boundary() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let bytes = tar(&[Item::File("parent/file", b"1234")]).await;
    let exact = OciRootfsLimits {
        max_layer_bytes: bytes.len() as u64,
        max_total_archive_bytes: bytes.len() as u64,
        max_entries: 1,
        max_tree_entries: 2,
        max_file_bytes: 4,
        max_total_file_bytes: 4,
        ..Default::default()
    };
    Tree::default()
        .apply(&mutation, Cursor::new(&bytes), &diff_id(&bytes), exact)
        .await
        .unwrap();
    for limits in [
        OciRootfsLimits {
            max_layer_bytes: bytes.len() as u64 - 1,
            ..exact
        },
        OciRootfsLimits {
            max_total_archive_bytes: bytes.len() as u64 - 1,
            ..exact
        },
        OciRootfsLimits {
            max_tree_entries: 1,
            ..exact
        },
        OciRootfsLimits {
            max_file_bytes: 3,
            ..exact
        },
        OciRootfsLimits {
            max_total_file_bytes: 3,
            ..exact
        },
    ] {
        assert!(
            Tree::default()
                .apply(&mutation, Cursor::new(&bytes), &diff_id(&bytes), limits)
                .await
                .is_err()
        );
    }
    let mut tree = Tree::default();
    tree.apply(&mutation, Cursor::new(&bytes), &diff_id(&bytes), exact)
        .await
        .unwrap();
    assert!(
        tree.apply(&mutation, Cursor::new(&bytes), &diff_id(&bytes), exact)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn decodes_gzip_zstd_and_raw_layers_and_checks_decoder_eof() {
    let repository = repository();
    let mutation = repository.mutation_session().await.unwrap();
    let raw = tar(&[Item::File("file", b"streamed")]).await;
    let mut gzip = async_compression::tokio::write::GzipEncoder::new(Vec::new());
    gzip.write_all(&raw).await.unwrap();
    gzip.shutdown().await.unwrap();
    let gzip = gzip.into_inner();
    let mut zstd = async_compression::tokio::write::ZstdEncoder::new(Vec::new());
    zstd.write_all(&raw).await.unwrap();
    zstd.shutdown().await.unwrap();
    let zstd = zstd.into_inner();
    for (bytes, media) in [
        (raw.clone(), "application/vnd.oci.image.layer.v1.tar"),
        (gzip.clone(), "application/vnd.oci.image.layer.v1.tar+gzip"),
        (zstd, "application/vnd.oci.image.layer.v1.tar+zstd"),
    ] {
        let reader = decode(Box::new(Cursor::new(bytes)), media).unwrap();
        let mut tree = Tree::default();
        tree.apply(
            &mutation,
            reader,
            &diff_id(&raw),
            OciRootfsLimits::default(),
        )
        .await
        .unwrap();
        file(&tree, "file", b"streamed");
    }
    let mut corrupt = gzip;
    *corrupt.last_mut().unwrap() ^= 1;
    let reader = decode(
        Box::new(Cursor::new(corrupt)),
        "application/vnd.oci.image.layer.v1.tar+gzip",
    )
    .unwrap();
    assert!(
        Tree::default()
            .apply(
                &mutation,
                reader,
                &diff_id(&raw),
                OciRootfsLimits::default()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn publishes_files_and_directories_with_one_object_batches() {
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        crate::FormatRegistry::builtin(),
        crate::FormatLimits {
            max_batch_objects: 1,
            ..Default::default()
        },
    );
    let mutation = repository.mutation_session().await.unwrap();
    let bytes = tar(&[
        Item::File("one/two/file", b"one"),
        Item::File("other/file", b"two"),
    ])
    .await;
    let mut tree = Tree::default();
    tree.apply(
        &mutation,
        Cursor::new(&bytes),
        &diff_id(&bytes),
        OciRootfsLimits::default(),
    )
    .await
    .unwrap();
    let root = tree.finish(&mutation).await.unwrap();
    let output = tempfile::tempdir().unwrap();
    repository.checkout(&root, output.path()).await.unwrap();
    assert_eq!(
        std::fs::read(output.path().join("one/two/file")).unwrap(),
        b"one"
    );
    assert_eq!(
        std::fs::read(output.path().join("other/file")).unwrap(),
        b"two"
    );
}
