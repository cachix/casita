#![cfg(feature = "oci")]

use std::collections::HashMap;
use std::sync::Arc;

use casita::{Directory, Node, ObjectKey, Repository, RootName, import::OciImport};
use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::{Client, Reference};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

struct Fixture {
    reference: Reference,
    client: Client,
    manifest_digest: String,
    layer_digest: String,
    server: tokio::task::JoinHandle<()>,
}

async fn fixture(corrupt_layer: bool) -> Fixture {
    fixture_with_options(corrupt_layer, false, false).await
}

async fn fixture_with_diff_id(corrupt_layer: bool, bad_diff_id: bool) -> Fixture {
    fixture_with_options(corrupt_layer, bad_diff_id, false).await
}

async fn fixture_with_options(
    corrupt_layer: bool,
    bad_diff_id: bool,
    repeated_arm_layer: bool,
) -> Fixture {
    let layer = vec![0u8; 1024];
    let layer_digest = sha256(&layer);
    let config = serde_json::to_vec(&serde_json::json!({
        "architecture": "amd64", "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": [layer_digest]}
    }))
    .unwrap();
    let config_digest = sha256(&config);
    let manifest = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layer_digest, "size": layer.len()}]
    })).unwrap();
    let manifest_digest = sha256(&manifest);
    let mut builder = ::tar::Builder::new(Vec::new());
    let mut header = ::tar::Header::new_ustar();
    header.set_size(8);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "etc/message", &b"arm root"[..])
        .unwrap();
    let mut bulk = vec![0u8; 256 * 1024];
    let mut state = 0x9e37_79b9u32;
    for byte in &mut bulk {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        *byte = state as u8;
    }
    let mut header = ::tar::Header::new_ustar();
    header.set_size(bulk.len() as u64);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "data/bulk", &bulk[..])
        .unwrap();
    let arm_raw = builder.into_inner().unwrap();
    let mut gzip = async_compression::tokio::write::GzipEncoder::new(Vec::new());
    gzip.write_all(&arm_raw).await.unwrap();
    gzip.shutdown().await.unwrap();
    let arm_layer = gzip.into_inner();
    assert!(arm_layer.len() > 64 * 1024);
    let arm_layer_digest = sha256(&arm_layer);
    let arm_diff_id = if bad_diff_id {
        sha256(b"wrong decoded content")
    } else {
        sha256(&arm_raw)
    };
    let arm_diff_ids = vec![arm_diff_id; if repeated_arm_layer { 2 } else { 1 }];
    let arm_config = serde_json::to_vec(&serde_json::json!({
        "architecture": "arm64", "os": "linux",
        "rootfs": {"type": "layers", "diff_ids": arm_diff_ids}
    }))
    .unwrap();
    let arm_config_digest = sha256(&arm_config);
    let arm_layers = vec![
        serde_json::json!({
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": arm_layer_digest, "size": arm_layer.len()
        });
        if repeated_arm_layer { 2 } else { 1 }
    ];
    let arm_manifest = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": arm_config_digest, "size": arm_config.len()},
        "layers": arm_layers
    })).unwrap();
    let arm_manifest_digest = sha256(&arm_manifest);
    let index = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "digest": manifest_digest, "size": manifest.len(),
             "platform": {"os": "linux", "architecture": "amd64"}},
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "digest": arm_manifest_digest, "size": arm_manifest.len(),
             "platform": {"os": "linux", "architecture": "arm64"}}
        ]
    }))
    .unwrap();
    let mut routes = HashMap::new();
    routes.insert(
        "/v2/demo/manifests/latest".to_owned(),
        (
            index.clone(),
            sha256(&index),
            "application/vnd.oci.image.index.v1+json",
        ),
    );
    routes.insert(
        format!("/v2/demo/manifests/{manifest_digest}"),
        (
            manifest,
            manifest_digest.clone(),
            "application/vnd.oci.image.manifest.v1+json",
        ),
    );
    routes.insert(
        format!("/v2/demo/manifests/{arm_manifest_digest}"),
        (
            arm_manifest,
            arm_manifest_digest.clone(),
            "application/vnd.oci.image.manifest.v1+json",
        ),
    );
    routes.insert(
        format!("/v2/demo/blobs/{config_digest}"),
        (config, config_digest, "application/octet-stream"),
    );
    routes.insert(
        format!("/v2/demo/blobs/{layer_digest}"),
        (
            if corrupt_layer {
                vec![1u8; 1024]
            } else {
                layer
            },
            layer_digest.clone(),
            "application/octet-stream",
        ),
    );
    routes.insert(
        format!("/v2/demo/blobs/{arm_config_digest}"),
        (arm_config, arm_config_digest, "application/octet-stream"),
    );
    routes.insert(
        format!("/v2/demo/blobs/{arm_layer_digest}"),
        (
            arm_layer,
            arm_layer_digest.clone(),
            "application/octet-stream",
        ),
    );
    let routes = Arc::new(routes);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let routes = routes.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0u8; 2048];
                loop {
                    let Ok(count) = stream.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split_whitespace().nth(1).unwrap_or("");
                let response = routes.get(path);
                let (status, body, digest, media_type) = match response {
                    Some((body, digest, media_type)) => {
                        ("200 OK", body.as_slice(), digest.as_str(), *media_type)
                    }
                    None => ("404 Not Found", b"missing".as_slice(), "", "text/plain"),
                };
                let header = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {media_type}\r\nContent-Length: {}\r\nDocker-Content-Digest: {digest}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes()).await;
                let _ = stream.write_all(body).await;
            });
        }
    });
    let config = ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    };
    Fixture {
        reference: format!("127.0.0.1:{}/demo:latest", addr.port())
            .parse()
            .unwrap(),
        client: Client::new(config),
        manifest_digest: arm_manifest_digest,
        layer_digest: arm_layer_digest,
        server,
    }
}

async fn read(repository: &Repository, key: &ObjectKey) -> Vec<u8> {
    let mut reader = repository.open(key).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    bytes
}

#[tokio::test]
async fn imports_streamed_image_as_a_complete_oci_layout() {
    let fixture = fixture(false).await;
    let repository = Repository::memory().unwrap();
    let root: RootName = "image".try_into().unwrap();
    let report = repository
        .import(
            OciImport::new(fixture.reference, root.clone())
                .with_client(fixture.client)
                .with_platform("linux/arm64"),
        )
        .await
        .unwrap();
    assert_eq!(report.manifest_digest, fixture.manifest_digest);
    assert!(report.source_index_digest.is_some());
    assert_eq!(report.layers, 1);
    assert_eq!(
        repository.root(&root).await.unwrap(),
        Some(report.root.clone())
    );
    let top = Directory::decode(&read(&repository, &report.root).await).unwrap();
    assert!(top.get("oci-layout").is_some());
    let Node::File { digest, .. } = top.get("index.json").unwrap() else {
        panic!("index file")
    };
    let index: serde_json::Value =
        serde_json::from_slice(&read(&repository, &ObjectKey::blob(*digest)).await).unwrap();
    assert_eq!(
        index["manifests"][0]["digest"].as_str(),
        Some(fixture.manifest_digest.as_str())
    );
    let Node::Directory { digest, .. } = top.get("blobs").unwrap() else {
        panic!("blobs directory")
    };
    let blobs =
        Directory::decode(&read(&repository, &ObjectKey::directory(*digest)).await).unwrap();
    let Node::Directory { digest, .. } = blobs.get("sha256").unwrap() else {
        panic!("sha256 directory")
    };
    let sha = Directory::decode(&read(&repository, &ObjectKey::directory(*digest)).await).unwrap();
    assert!(
        sha.get(fixture.manifest_digest.strip_prefix("sha256:").unwrap())
            .is_some()
    );
    assert!(
        sha.get(fixture.layer_digest.strip_prefix("sha256:").unwrap())
            .is_some()
    );
    fixture.server.abort();
}

#[tokio::test]
async fn direct_manifest_reference_imports_without_a_source_index() {
    let fixture = fixture(false).await;
    let reference = fixture
        .reference
        .clone_with_digest(fixture.manifest_digest.clone());
    let repository = Repository::memory().unwrap();
    let root: RootName = "direct-image".try_into().unwrap();
    let report = repository
        .import(OciImport::new(reference, root.clone()).with_client(fixture.client))
        .await
        .unwrap();
    assert_eq!(report.manifest_digest, fixture.manifest_digest);
    assert_eq!(report.source_index_digest, None);
    assert_eq!(repository.root(&root).await.unwrap(), Some(report.root));
    fixture.server.abort();
}

#[tokio::test]
async fn corrupt_layer_does_not_replace_existing_root() {
    let fixture = fixture(true).await;
    let repository = Repository::memory().unwrap();
    let root: RootName = "image".try_into().unwrap();
    let previous = repository
        .import(casita::import::BlobImport::new(
            &b"previous"[..],
            root.clone(),
        ))
        .await
        .unwrap();
    assert!(
        repository
            .import(
                OciImport::new(fixture.reference, root.clone())
                    .with_client(fixture.client)
                    .with_platform("linux/amd64"),
            )
            .await
            .is_err()
    );
    assert_eq!(repository.root(&root).await.unwrap(), Some(previous));
    fixture.server.abort();
}

#[tokio::test]
async fn imports_layout_and_checkoutable_filesystem_together() {
    let fixture = fixture(false).await;
    let repository = Repository::memory().unwrap();
    let layout: RootName = "image".try_into().unwrap();
    let filesystem: RootName = "filesystem".try_into().unwrap();
    let report = repository
        .import(
            OciImport::new(fixture.reference, layout.clone())
                .with_client(fixture.client)
                .with_platform("linux/arm64")
                .with_rootfs(filesystem.clone()),
        )
        .await
        .unwrap();
    let rootfs = report.rootfs.unwrap();
    assert_eq!(repository.root(&layout).await.unwrap(), Some(report.root));
    assert_eq!(
        repository.root(&filesystem).await.unwrap(),
        Some(rootfs.clone())
    );
    let output = tempfile::tempdir().unwrap();
    repository.checkout(&rootfs, output.path()).await.unwrap();
    assert_eq!(
        std::fs::read(output.path().join("etc/message")).unwrap(),
        b"arm root"
    );
    assert_eq!(
        std::fs::read(output.path().join("data/bulk"))
            .unwrap()
            .len(),
        256 * 1024
    );
    fixture.server.abort();
}

#[tokio::test]
async fn repeated_layer_is_applied_twice_during_registry_transfer() {
    let fixture = fixture_with_options(false, false, true).await;
    let repository = Repository::memory().unwrap();
    let filesystem: RootName = "filesystem".try_into().unwrap();
    let report = repository
        .import(
            OciImport::new(fixture.reference, "layout".try_into().unwrap())
                .with_client(fixture.client)
                .with_platform("linux/arm64")
                .with_rootfs(filesystem.clone()),
        )
        .await
        .unwrap();
    assert_eq!(report.layers, 2);
    let rootfs = report.rootfs.unwrap();
    assert_eq!(
        repository.root(&filesystem).await.unwrap(),
        Some(rootfs.clone())
    );
    let output = tempfile::tempdir().unwrap();
    repository.checkout(&rootfs, output.path()).await.unwrap();
    assert_eq!(
        std::fs::read(output.path().join("etc/message")).unwrap(),
        b"arm root"
    );
    fixture.server.abort();
}

#[tokio::test]
async fn invalid_diff_id_does_not_replace_either_existing_root() {
    let fixture = fixture_with_diff_id(false, true).await;
    let repository = Repository::memory().unwrap();
    let layout: RootName = "image".try_into().unwrap();
    let filesystem: RootName = "filesystem".try_into().unwrap();
    let previous_layout = repository
        .import(casita::import::BlobImport::new(
            &b"previous layout"[..],
            layout.clone(),
        ))
        .await
        .unwrap();
    let previous_filesystem = repository
        .import(casita::import::BlobImport::new(
            &b"previous filesystem"[..],
            filesystem.clone(),
        ))
        .await
        .unwrap();
    let error = repository
        .import(
            OciImport::new(fixture.reference, layout.clone())
                .with_client(fixture.client)
                .with_platform("linux/arm64")
                .with_rootfs(filesystem.clone()),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("uncompressed layer digest mismatch"),
        "{error}"
    );
    assert_eq!(
        repository.root(&layout).await.unwrap(),
        Some(previous_layout)
    );
    assert_eq!(
        repository.root(&filesystem).await.unwrap(),
        Some(previous_filesystem)
    );
    fixture.server.abort();
}
