use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

use casita::experimental::{MetadataStore, Repository};
use casita::{CasitarStreamLimits, RootName};

#[tokio::test]
async fn frame_limit_is_enforced_before_newline_or_eof() {
    for negotiated in [false, true] {
        let repository = Arc::new(Repository::memory().unwrap());
        let (client, server) = tokio::io::duplex(super::MAX_FRAME_BYTES + 2);
        let task = tokio::spawn(super::server::serve_connection(repository, server));
        let mut client = BufReader::new(client);
        let limit = if negotiated {
            let response = call(
                &mut client,
                "rpc.initialize",
                json!({
                    "versions": [1], "max_frame_bytes": super::MIN_FRAME_BYTES
                }),
            )
            .await;
            assert_eq!(
                response["result"]["max_frame_bytes"],
                super::MIN_FRAME_BYTES
            );
            super::MIN_FRAME_BYTES
        } else {
            super::MAX_FRAME_BYTES
        };
        client
            .get_mut()
            .write_all(&vec![b'x'; limit + 1])
            .await
            .unwrap();
        // Keep the peer open: rejection must not depend on EOF or a newline.
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("oversized IPC frame")
        );
    }
}

#[tokio::test]
async fn exact_limit_and_pipelined_frames_preserve_framing() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    call(
        &mut client,
        "rpc.initialize",
        json!({
            "versions": [1], "max_frame_bytes": super::MIN_FRAME_BYTES
        }),
    )
    .await;
    let mut frame = br#"{"jsonrpc":"2.0","id":2,"method":"unknown"}"#.to_vec();
    frame.resize(super::MIN_FRAME_BYTES, b' ');
    frame.extend_from_slice(b"\n{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"rpc.shutdown\"}\n");
    client.get_mut().write_all(&frame).await.unwrap();
    for id in [2, 3] {
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["id"], id);
    }
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn frame_deadline_bounds_idle_and_trickling_clients() {
    for trickle in [false, true] {
        let repository = Arc::new(Repository::memory().unwrap());
        let (mut client, server) = tokio::io::duplex(64);
        let options = super::IpcOptions {
            frame_timeout: std::time::Duration::from_secs(10),
            ..Default::default()
        };
        let task = tokio::spawn(super::server::serve_connection_with_options(
            repository, server, options,
        ));
        tokio::task::yield_now().await;
        if trickle {
            for _ in 0..3 {
                tokio::time::advance(std::time::Duration::from_secs(3)).await;
                client.write_all(b" ").await.unwrap();
                tokio::task::yield_now().await;
            }
        }
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        assert!(
            task.await
                .unwrap()
                .unwrap_err()
                .is::<tokio::time::error::Elapsed>()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn response_deadline_bounds_clients_that_stop_reading() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (mut client, server) = tokio::io::duplex(128);
    let options = super::IpcOptions {
        response_timeout: std::time::Duration::from_secs(5),
        ..Default::default()
    };
    let task = tokio::spawn(super::server::serve_connection_with_options(
        repository, server, options,
    ));
    client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"rpc.initialize\",\"params\":{\"versions\":[1]}}\n").await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .is::<tokio::time::error::Elapsed>()
    );
}

#[tokio::test(start_paused = true)]
async fn connection_limit_rejects_excess_and_releases_slots() {
    use tokio::io::AsyncReadExt;
    let repository = Arc::new(Repository::memory().unwrap());
    let mut connections = super::server::Connections::new(super::IpcOptions {
        max_connections: 1,
        ..Default::default()
    });
    let (client, server) = tokio::io::duplex(1024);
    assert!(connections.admit(repository.clone(), server));
    let (rejected, server) = tokio::io::duplex(1024);
    assert!(!connections.admit(repository.clone(), server));
    // A rejected client learns why before the daemon closes its connection.
    let mut rejected = BufReader::new(rejected);
    let mut frame = String::new();
    rejected.read_line(&mut frame).await.unwrap();
    assert_eq!(frame, super::server::REJECTED_FRAME);
    let error: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(error["error"]["code"], -32000);
    assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    call(&mut client, "rpc.shutdown", Value::Null).await;
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    let (mut client, server) = tokio::io::duplex(1024);
    assert!(connections.admit(repository.clone(), server));
    // An idle timeout must release the same permit as a normal shutdown.
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    let (mut client, server) = tokio::io::duplex(1024);
    assert!(connections.admit(repository, server));
    drop(connections);
    // Dropping the listener's task set also terminates accepted connections.
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
}

#[test]
fn accept_errors_from_exhausted_resources_do_not_stop_the_listener() {
    use std::io::{Error, ErrorKind};
    for kind in [ErrorKind::ConnectionAborted, ErrorKind::Interrupted] {
        assert!(super::server::accept_error_is_transient(&Error::from(kind)));
    }
    #[cfg(unix)]
    for code in [libc::EMFILE, libc::ENFILE] {
        assert!(super::server::accept_error_is_transient(
            &Error::from_raw_os_error(code)
        ));
    }
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::NotFound,
        ErrorKind::Other,
    ] {
        assert!(!super::server::accept_error_is_transient(&Error::from(
            kind
        )));
    }
}

async fn call(client: &mut BufReader<DuplexStream>, method: &str, params: Value) -> Value {
    let mut request = json!({"jsonrpc":"2.0", "id":1, "method":method});
    if !params.is_null() {
        request["params"] = params;
    }
    let line = format!("{request}\n");
    client.get_mut().write_all(line.as_bytes()).await.unwrap();
    let mut response = String::new();
    client.read_line(&mut response).await.unwrap();
    let response: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["id"], 1);
    response
}

async fn initialize(client: &mut BufReader<DuplexStream>) {
    let response = call(client, "rpc.initialize", json!({"versions":[1]})).await;
    assert_eq!(
        response["result"]["importers"],
        json!(super::import::IMPORTERS)
    );
}

#[tokio::test]
async fn filesystem_wire_options_legacy_requests_and_validation() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("keep"), b"content").unwrap();
    std::fs::write(source.join("skip"), b"control").unwrap();
    let legacy = json!({"path":source, "root":"legacy"});
    assert_eq!(
        call(&mut client, "artifact.import", legacy.clone()).await["error"]["code"],
        -32600
    );
    initialize(&mut client).await;
    let original = call(&mut client, "artifact.import", legacy).await;
    assert!(original["result"]["object"].is_string(), "{original}");
    let filtered = call(
        &mut client,
        "artifact.import",
        json!({
            "importer":"filesystem", "path":source, "root":"filtered",
            "options":{"reread":false,"exclude":"skip"}
        }),
    )
    .await;
    assert!(filtered["result"]["object"].is_string(), "{filtered}");
    assert_ne!(original["result"], filtered["result"]);
    let destination = work.path().join("checkout");
    let checkout = call(
        &mut client,
        "artifact.checkout",
        json!({"root":"filtered","path":destination}),
    )
    .await;
    assert_eq!(checkout["result"]["present"], true, "{checkout}");
    assert_eq!(std::fs::read(destination.join("keep")).unwrap(), b"content");
    assert!(!destination.join("skip").exists());
    for params in [
        json!({"importer":"unknown","path":source,"root":"bad"}),
        json!({"path":source,"root":"bad","options":{"rereed":false}}),
        json!({"path":source,"root":"bad","options":{"reread":"false"}}),
        json!({"importer":"tar","path":source,"root":"bad","options":{"exclude":"skip"}}),
        json!({"importer":"tar","path":source,"root":"bad","options":{"limits":{"max_entries":-1}}}),
        json!({"importer":"tar","path":source,"root":"bad","options":{"limits":{"max_entry":1}}}),
        json!({"importer":"casitar","path":source,"destinations":[]}),
        json!({"path":source,"root":"bad","view":"unexpected"}),
    ] {
        let response = call(&mut client, "artifact.import", params).await;
        assert_eq!(response["error"]["code"], -32602, "{response}");
    }
    #[cfg(not(feature = "git"))]
    assert_eq!(
        call(
            &mut client,
            "artifact.import",
            json!({"importer":"git","path":source,"view":"test"})
        )
        .await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(&mut client, "rpc.shutdown", Value::Null).await["result"],
        Value::Null
    );
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn archive_wire_imports_enforce_limits_and_conflicts() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(
        Arc::clone(&repository),
        server,
    ));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let work = tempfile::tempdir().unwrap();
    let mut builder = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(7);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "hello", &b"content"[..])
        .await
        .unwrap();
    let path = work.path().join("input.tar");
    std::fs::write(&path, builder.into_inner().await.unwrap()).unwrap();
    let imported = call(
        &mut client,
        "artifact.import",
        json!({"importer":"tar","path":path,"root":"tar","options":{"limits":{"max_in_flight_files":1}}}),
    )
    .await;
    assert_eq!(imported["result"]["files"], 1, "{imported}");
    assert_eq!(imported["result"]["file_bytes"], 7);
    let failed = call(&mut client, "artifact.import", json!({
        "importer":"tar","path":path,"root":"too-large","options":{"limits":{"max_archive_bytes":1}}
    })).await;
    assert_eq!(failed["error"]["code"], -32008, "{failed}");
    let (archive, _) = repository
        .export_casitar(
            [RootName::try_from("tar").unwrap()],
            Vec::new(),
            CasitarStreamLimits::default(),
        )
        .await
        .unwrap();
    let path = work.path().join("input.casitar");
    std::fs::write(&path, archive).unwrap();
    let params = json!({"importer":"casitar","path":path,"destinations":["restored"]});
    let restored = call(&mut client, "artifact.import", params.clone()).await;
    assert_eq!(
        restored["result"]["mappings"][0]["root"], imported["result"]["object"],
        "{restored}"
    );
    assert_eq!(restored["result"]["mappings"][0]["name"], "restored");
    assert_eq!(
        call(&mut client, "artifact.import", params.clone()).await["error"]["code"],
        -32008
    );
    let mut replacement = params.clone();
    replacement["options"] = json!({"conflict_policy":"replace_if_unchanged"});
    let replaced = call(&mut client, "artifact.import", replacement).await;
    assert_eq!(
        replaced["result"]["mappings"], restored["result"]["mappings"],
        "{replaced}"
    );
    let mut bounded = params;
    bounded["destinations"] = json!(["bounded"]);
    bounded["options"] = json!({"limits":{"max_archive_bytes":1}});
    assert_eq!(
        call(&mut client, "artifact.import", bounded).await["error"]["code"],
        -32008
    );
    drop(client);
    task.await.unwrap().unwrap();
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_wire_import_selects_refs() {
    let source = tempfile::tempdir().unwrap();
    for args in [
        vec!["init", "--initial-branch=main"],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    ] {
        let output = std::process::Command::new("git")
            .current_dir(source.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let mut params = json!({"importer":"git","path":source.path(),"view":"upstream",
        "options":{"refs":["refs/heads/main"],"max_cached_pack_bytes":0,"concurrency":4,"max_buffered_bytes":65536}});
    let response = call(&mut client, "artifact.import", params.clone()).await;
    assert!(response["result"]["view"].is_string(), "{response}");
    assert!(response["result"]["objects"].as_u64().unwrap() > 0);
    for field in ["concurrency", "max_buffered_bytes"] {
        let mut invalid = params.clone();
        invalid["options"][field] = json!(0);
        assert_eq!(
            call(&mut client, "artifact.import", invalid).await["error"]["code"],
            -32602
        );
    }
    params["options"]["refs"] = json!(["refs/heads/missing"]);
    assert_eq!(
        call(&mut client, "artifact.import", params.clone()).await["error"]["code"],
        -32008
    );
    params["options"]["refs"] = json!(["invalid ref"]);
    assert_eq!(
        call(&mut client, "artifact.import", params).await["error"]["code"],
        -32602
    );
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsupported_imports_are_classified_before_any_mutation() {
    use casita::experimental::MetadataStore;
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let before = repository.metadata().snapshot().await.unwrap().revision();
    for (params, category, importer, option) in [
        (
            json!({"importer":"future"}),
            "unsupported_importer",
            "future",
            Value::Null,
        ),
        (
            json!({"importer":"blob","path":"/missing","root":"x","options":{"future":true}}),
            "unsupported_option",
            "blob",
            json!("future"),
        ),
        (
            json!({"importer":"tar","path":"/missing","root":"x","options":{"compression":"zstd"}}),
            "unsupported_option",
            "tar",
            json!("compression"),
        ),
        (
            json!({"importer":"tar","path":"/missing","root":"x","options":{"limits":{"future":1}}}),
            "unsupported_option",
            "tar",
            json!("limits.future"),
        ),
        (
            json!({"importer":"nar","path":"/missing"}),
            "invalid_parameters",
            "nar",
            Value::Null,
        ),
        (
            json!({"importer":"filesystem_nar","path":"/missing","root":22}),
            "invalid_parameters",
            "filesystem_nar",
            Value::Null,
        ),
    ] {
        let response = call(&mut client, "artifact.import", params).await;
        assert_eq!(
            response["error"]["data"],
            json!({"category":category,"importer":importer,"option":option}),
            "{response}"
        );
        assert_eq!(
            repository.metadata().snapshot().await.unwrap().revision(),
            before
        );
    }
    drop(client);
    task.await.unwrap().unwrap();
}

async fn tar_fixture() -> Vec<u8> {
    let mut builder = tokio_tar::Builder::new(Vec::new());
    let mut header = tokio_tar::Header::new_ustar();
    header.set_size(7);
    header.set_mode(0o755);
    builder
        .append_data(&mut header, "hello", &b"content"[..])
        .await
        .unwrap();
    builder.into_inner().await.unwrap()
}

async fn gzip_fixture(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = async_compression::tokio::write::GzipEncoder::new(Vec::new());
    encoder.write_all(bytes).await.unwrap();
    encoder.shutdown().await.unwrap();
    encoder.into_inner()
}

#[tokio::test]
async fn gzip_integrity_and_both_limits_gate_publication() {
    use casita::experimental::MetadataStore;
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let work = tempfile::tempdir().unwrap();
    let path = work.path().join("archive");
    let raw = tar_fixture().await;
    let compressed = gzip_fixture(&raw).await;
    std::fs::write(&path, &compressed).unwrap();
    let params = json!({"importer":"tar","path":path,"root":"protected","options":{"compression":"gzip","max_compressed_bytes":compressed.len(),"limits":{"max_archive_bytes":raw.len()}}});
    let response = call(&mut client, "artifact.import", params.clone()).await;
    assert!(response["result"]["object"].is_string(), "{response}");
    assert_eq!(response["result"]["archive_bytes"], raw.len());
    let root = RootName::try_from("protected").unwrap();
    let original = repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .root(&root)
        .await
        .unwrap();
    let mut corrupt = compressed.clone();
    let trailer = corrupt.len() - 8;
    corrupt[trailer] ^= 1;
    let mut appended = compressed.clone();
    appended.extend_from_slice(b"garbage");
    let mut corrupt_tar = raw.clone();
    corrupt_tar[0] ^= 1;
    let corrupt_tar = gzip_fixture(&corrupt_tar).await;
    for (bytes, options) in [
        (corrupt, json!({"compression":"gzip"})),
        (
            compressed[..compressed.len() - 1].to_vec(),
            json!({"compression":"gzip"}),
        ),
        (appended, json!({"compression":"gzip"})),
        (corrupt_tar, json!({"compression":"gzip"})),
        (
            compressed.clone(),
            json!({"compression":"gzip","max_compressed_bytes":compressed.len()-1}),
        ),
        (
            compressed.clone(),
            json!({"compression":"gzip","limits":{"max_archive_bytes":raw.len()-1}}),
        ),
        (
            compressed,
            json!({"compression":"gzip","limits":{"max_file_bytes":6}}),
        ),
    ] {
        std::fs::write(&path, bytes).unwrap();
        let mut request = params.clone();
        request["options"] = options;
        let response = call(&mut client, "artifact.import", request).await;
        assert_eq!(
            response["error"]["data"]["category"], "execution_failure",
            "{response}"
        );
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root)
                .await
                .unwrap(),
            original
        );
    }
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn all_importers_restore_after_source_removal_restart_and_collection() {
    let work = tempfile::tempdir().unwrap();
    let inputs = work.path().join("inputs");
    std::fs::create_dir(&inputs).unwrap();
    let tree = inputs.join("tree");
    std::fs::create_dir(&tree).unwrap();
    std::fs::write(tree.join("hello"), b"content").unwrap();
    let tar = inputs.join("tree.tar");
    let raw = tar_fixture().await;
    std::fs::write(&tar, &raw).unwrap();
    let gzip = inputs.join("tree.gz");
    std::fs::write(&gzip, gzip_fixture(&raw).await).unwrap();
    let nar = inputs.join("file.nar");
    let mut nar_bytes = Vec::new();
    nix_archive::nar::encode_tree(
        &mut nar_bytes,
        &nix_archive::nar::Node::Regular {
            executable: true,
            contents: b"content",
        },
    )
    .unwrap();
    std::fs::write(&nar, &nar_bytes).unwrap();
    let source_path = inputs.join("repository");
    let source = Repository::local(&source_path).await.unwrap();
    source
        .import(casita::import::FilesystemImport::new(
            &tree,
            RootName::try_from("source").unwrap(),
        ))
        .await
        .unwrap();
    let (archive, _) = source
        .export_casitar(
            [RootName::try_from("source").unwrap()],
            Vec::new(),
            CasitarStreamLimits::default(),
        )
        .await
        .unwrap();
    let casitar = inputs.join("tree.casitar");
    std::fs::write(&casitar, archive).unwrap();
    drop(source);
    let repository_path = work.path().join("repository");
    let repository = Arc::new(Repository::local(&repository_path).await.unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let import_requests = vec![
        json!({"path":tree,"root":"filesystem"}),
        json!({"importer":"blob","parameters":{"path":tree.join("hello"),"root":"blob"},"options":{}}),
        json!({"importer":"copy","path":source_path,"source_root":"source","root":"copy"}),
        json!({"importer":"tar","path":tar,"root":"tar"}),
        json!({"importer":"tar","path":gzip,"root":"gzip","options":{"compression":"gzip"}}),
        json!({"importer":"casitar","path":casitar,"destinations":["casitar"]}),
        json!({"importer":"nar","path":nar,"root":"nar"}),
        json!({"importer":"filesystem_nar","path":tree,"root":"filesystem_nar"}),
        json!({"importer":"filesystem_nar","path":tree.join("hello"),"root":"filesystem_nar_file"}),
    ];
    let process_repository = work.path().join("process-repository");
    let process_imports: Vec<_> = import_requests
        .iter()
        .map(|params| json!({"method":"artifact.import","params":params}))
        .collect();
    restore_in_new_process(&process_repository, json!(process_imports));
    for request in import_requests {
        let response = call(&mut client, "artifact.import", request).await;
        assert!(response.get("error").is_none(), "{response}");
    }
    // All failed replacements must retain the previous durable root.
    for importer in ["filesystem", "blob", "copy", "tar", "nar", "filesystem_nar"] {
        use casita::experimental::MetadataStore;
        let root = RootName::try_from(importer).unwrap();
        let before = repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&root)
            .await
            .unwrap();
        let mut request = json!({"importer":importer,"path":inputs.join("absent"),"root":importer});
        if importer == "copy" {
            request["source_root"] = json!("source");
        }
        let response = call(&mut client, "artifact.import", request).await;
        assert_eq!(
            response["error"]["data"]["category"], "execution_failure",
            "{response}"
        );
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root)
                .await
                .unwrap(),
            before
        );
    }
    for (importer, archive) in [("nar", &nar), ("tar", &tar), ("casitar", &casitar)] {
        use casita::experimental::MetadataStore;
        let name = RootName::try_from(importer).unwrap();
        let before = repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .root(&name)
            .await
            .unwrap();
        let bytes = std::fs::read(archive).unwrap();
        std::fs::write(archive, &bytes[..100.min(bytes.len() / 3)]).unwrap();
        let request = if importer == "casitar" {
            json!({"importer":importer,"path":archive,"destinations":[importer],"options":{"conflict_policy":"replace_if_unchanged"}})
        } else {
            json!({"importer":importer,"path":archive,"root":importer})
        };
        let response = call(&mut client, "artifact.import", request).await;
        assert_eq!(
            response["error"]["data"]["category"], "execution_failure",
            "{response}"
        );
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&name)
                .await
                .unwrap(),
            before
        );
    }
    call(&mut client, "rpc.shutdown", Value::Null).await;
    task.await.unwrap().unwrap();
    drop(client);
    drop(repository);
    std::fs::remove_dir_all(&inputs).unwrap();
    let mut restored = Vec::new();
    for root in [
        "filesystem",
        "blob",
        "copy",
        "tar",
        "gzip",
        "casitar",
        "nar",
        "filesystem_nar",
        "filesystem_nar_file",
    ] {
        let importer = match root {
            "gzip" => "tar",
            "filesystem_nar_file" => "filesystem_nar",
            other => other,
        };
        restored.push(json!({"importer":importer,"root":root,"path":work.path().join(format!("process-{root}"))}));
    }
    restore_in_new_process(&process_repository, json!(restored));
    for root in [
        "filesystem",
        "blob",
        "copy",
        "tar",
        "gzip",
        "casitar",
        "nar",
        "filesystem_nar",
        "filesystem_nar_file",
    ] {
        let output = work.path().join(format!("process-{root}"));
        let file = if ["blob", "nar", "filesystem_nar_file"].contains(&root) {
            output
        } else {
            output.join("hello")
        };
        assert_eq!(std::fs::read(file).unwrap(), b"content");
    }

    let repository = Arc::new(Repository::local(&repository_path).await.unwrap());
    repository.collect().await.unwrap();
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    for root in [
        "filesystem",
        "blob",
        "copy",
        "tar",
        "gzip",
        "casitar",
        "nar",
        "filesystem_nar",
        "filesystem_nar_file",
    ] {
        let path = work.path().join(format!("restore-{root}"));
        let importer = match root {
            "gzip" => "tar",
            "filesystem_nar_file" => "filesystem_nar",
            other => other,
        };
        let request = json!({"importer":importer,"root":root,"path":path});
        let response = call(&mut client, "artifact.restore", request.clone()).await;
        assert_eq!(response["result"]["present"], true, "{root}: {response}");
        let file = if ["blob", "nar", "filesystem_nar_file"].contains(&root) {
            path.clone()
        } else {
            path.join("hello")
        };
        assert_eq!(std::fs::read(&file).unwrap(), b"content");
        #[cfg(unix)]
        if root == "nar" {
            use std::os::unix::fs::PermissionsExt;
            assert_ne!(
                std::fs::metadata(&file).unwrap().permissions().mode() & 0o111,
                0
            );
        }
        let conflict = call(&mut client, "artifact.restore", request).await;
        assert_eq!(
            conflict["error"]["data"]["category"], "execution_failure",
            "{conflict}"
        );
        assert_eq!(std::fs::read(&file).unwrap(), b"content");
    }
    // A wrong restoration kind fails after staging and leaves no destination.
    let wrong = work.path().join("wrong");
    let response = call(
        &mut client,
        "artifact.restore",
        json!({"importer":"nar","root":"filesystem","path":wrong}),
    )
    .await;
    assert!(response.get("error").is_some());
    assert!(!wrong.exists());
    assert!(!std::fs::read_dir(work.path()).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".casita-restore-")
    }));
    drop(client);
    task.await.unwrap().unwrap();
}

#[cfg(feature = "git")]
#[tokio::test]
async fn git_restores_fetched_refs_and_pins_offline_after_restart_and_gc() {
    fn git(path: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    std::fs::create_dir(&source).unwrap();
    git(&source, &["init", "--initial-branch=main"]);
    git(
        &source,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "main",
        ],
    );
    let main = git(&source, &["rev-parse", "HEAD"]);
    git(&source, &["update-ref", "refs/remotes/origin/main", &main]);
    git(&source, &["checkout", "--orphan", "orphan"]);
    std::fs::write(source.join("pinned-file"), b"pinned content").unwrap();
    git(&source, &["add", "pinned-file"]);
    git(
        &source,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "pinned",
        ],
    );
    let pin = git(&source, &["rev-parse", "HEAD"]);
    git(&source, &["checkout", "main"]);
    git(&source, &["branch", "-D", "orphan"]);
    let repository_path = work.path().join("repository");
    let repository = Arc::new(Repository::local(&repository_path).await.unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let request = json!({"importer":"git","path":source,"view":"offline","options":{"refs":["refs/heads/main","refs/remotes/origin/main"],"revisions":[pin],"max_cached_pack_bytes":0}});
    let process_repository = work.path().join("process-repository");
    restore_in_new_process(
        &process_repository,
        json!([{"method":"artifact.import","params":request}]),
    );
    let response = call(&mut client, "artifact.import", request.clone()).await;
    assert!(response.get("error").is_none(), "{response}");
    let mut failure = request;
    failure["options"]["refs"] = json!(["refs/heads/absent"]);
    assert_eq!(
        call(&mut client, "artifact.import", failure).await["error"]["data"]["category"],
        "execution_failure"
    );
    drop(client);
    task.await.unwrap().unwrap();
    drop(repository);
    std::fs::remove_dir_all(&source).unwrap();
    let process_output = work.path().join("process.git");
    restore_in_new_process(
        &process_repository,
        json!([{"importer":"git","root":"git/offline","path":process_output}]),
    );
    git(&process_output, &["fsck", "--full", "--strict"]);
    assert_eq!(
        git(&process_output, &["rev-parse", "refs/remotes/origin/main"]),
        main
    );
    assert_eq!(
        git(
            &process_output,
            &["rev-parse", &format!("refs/casita/pins/{pin}")]
        ),
        pin
    );

    let repository = Arc::new(Repository::local(&repository_path).await.unwrap());
    repository.collect().await.unwrap();
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let destination = work.path().join("restored.git");
    let request = json!({"importer":"git","root":"git/offline","path":destination});
    let response = call(&mut client, "artifact.restore", request.clone()).await;
    assert_eq!(response["result"]["present"], true, "{response}");
    assert_eq!(
        git(&destination, &["rev-parse", "--is-bare-repository"]),
        "true"
    );
    git(&destination, &["fsck", "--full", "--strict"]);
    assert_eq!(git(&destination, &["rev-parse", "HEAD"]), main);
    assert_eq!(
        git(&destination, &["rev-parse", "refs/remotes/origin/main"]),
        main
    );
    assert_eq!(
        git(
            &destination,
            &["rev-parse", &format!("refs/casita/pins/{pin}")]
        ),
        pin
    );
    assert_eq!(
        git(&destination, &["show", &format!("{pin}:pinned-file")]),
        "pinned content"
    );
    assert!(
        call(&mut client, "artifact.restore", request)
            .await
            .get("error")
            .is_some()
    );
    git(&destination, &["fsck", "--full", "--strict"]);
    drop(client);
    task.await.unwrap().unwrap();
}

// Run the restarted service in another process so no connection, retention
// hold, repository cache or source handle can survive into offline restore.
fn restore_in_new_process(repository: &std::path::Path, requests: Value) {
    let work = tempfile::tempdir().unwrap();
    let input = work.path().join("requests.json");
    std::fs::write(&input, serde_json::to_vec(&requests).unwrap()).unwrap();
    // A shared Cargo cache may unlink an artifact while its tests still run.
    // On Linux the live executable remains addressable through procfs.
    #[cfg(target_os = "linux")]
    let executable = std::path::PathBuf::from(format!("/proc/{}/exe", std::process::id()));
    #[cfg(not(target_os = "linux"))]
    let executable = std::env::current_exe().unwrap();
    let output = std::process::Command::new(executable)
        .args([
            "--exact",
            "cli::ipc::tests::offline_restore_child",
            "--ignored",
            "--nocapture",
        ])
        .env("CASITA_IPC_RESTART_REPOSITORY", repository)
        .env("CASITA_IPC_RESTART_REQUESTS", &input)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "restarted service failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "subprocess helper for restart coverage"]
fn offline_restore_child() {
    let Some(repository) = std::env::var_os("CASITA_IPC_RESTART_REPOSITORY") else {
        return;
    };
    let requests: Vec<Value> = serde_json::from_slice(
        &std::fs::read(std::env::var_os("CASITA_IPC_RESTART_REQUESTS").unwrap()).unwrap(),
    )
    .unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let repository = Arc::new(Repository::local(repository).await.unwrap());
            repository.collect().await.unwrap();
            let (client, server) = tokio::io::duplex(65536);
            let task = tokio::spawn(super::server::serve_connection(repository, server));
            let mut client = BufReader::new(client);
            initialize(&mut client).await;
            for request in requests {
                let response = if let Some(method) = request.get("method").and_then(Value::as_str) {
                    call(&mut client, method, request["params"].clone()).await
                } else {
                    let response = call(&mut client, "artifact.restore", request).await;
                    assert_eq!(response["result"]["present"], true, "{response}");
                    response
                };
                assert!(response.get("error").is_none(), "{response}");
            }
            drop(client);
            task.await.unwrap().unwrap();
        });
}

#[cfg(unix)]
#[tokio::test]
async fn nar_symlink_roots_survive_collection_and_restore_without_following_links() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source-link");
    std::os::unix::fs::symlink("missing-target", &source).unwrap();
    let mut bytes = Vec::new();
    nix_archive::nar::encode_tree(
        &mut bytes,
        &nix_archive::nar::Node::Symlink {
            target: b"missing-target",
        },
    )
    .unwrap();
    let archive = work.path().join("link.nar");
    std::fs::write(&archive, bytes).unwrap();
    let repository_path = work.path().join("repository");
    let repository = Arc::new(Repository::local(&repository_path).await.unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    for (importer, path) in [("nar", &archive), ("filesystem_nar", &source)] {
        let response = call(
            &mut client,
            "artifact.import",
            json!({"importer":importer,"path":path,"root":importer}),
        )
        .await;
        assert!(response.get("error").is_none(), "{response}");
    }
    drop(client);
    task.await.unwrap().unwrap();
    drop(repository);
    std::fs::remove_file(source).unwrap();
    std::fs::remove_file(archive).unwrap();
    restore_in_new_process(
        &repository_path,
        json!([
            {"importer":"nar","root":"nar","path":work.path().join("nar")},
            {"importer":"filesystem_nar","root":"filesystem_nar","path":work.path().join("filesystem_nar")}
        ]),
    );
    for root in ["nar", "filesystem_nar"] {
        assert_eq!(
            std::fs::read_link(work.path().join(root)).unwrap(),
            std::path::Path::new("missing-target")
        );
    }
}

#[tokio::test]
async fn batch_import_is_one_commit_and_failure_publishes_nothing() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository.clone(), server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let work = tempfile::tempdir().unwrap();
    let blob = work.path().join("blob");
    std::fs::write(&blob, b"blob content").unwrap();
    let filesystem = work.path().join("filesystem");
    std::fs::create_dir(&filesystem).unwrap();
    std::fs::write(filesystem.join("file"), b"filesystem content").unwrap();
    let tar = work.path().join("archive.tar.gz");
    std::fs::write(&tar, gzip_fixture(&tar_fixture().await).await).unwrap();
    let requests = json!([
        {"importer":"blob","path":blob,"root":"batch/blob"},
        {"path":filesystem,"root":"batch/filesystem","options":{"reread":false}},
        {"importer":"tar","path":tar,"root":"batch/tar","options":{"compression":"gzip"}}
    ]);
    let before = repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .generation()
        .unwrap();
    let response = call(&mut client, "artifact.import", requests.clone()).await;
    assert!(response["result"].is_array(), "{response}");
    assert_eq!(response["result"].as_array().unwrap().len(), 3);
    assert_eq!(response["result"][2]["files"], 1);
    let after = repository
        .metadata()
        .snapshot()
        .await
        .unwrap()
        .generation()
        .unwrap();
    assert_eq!(after, before + 1);
    let reader = repository.retained_reader().await.unwrap();
    let original = reader
        .root(&"batch/blob".parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    drop(reader);
    std::fs::write(&blob, b"replacement").unwrap();
    let mut broken = requests.as_array().unwrap().clone();
    broken[2]["options"]["limits"] = json!({"max_file_bytes": 1});
    let response = call(&mut client, "artifact.import", json!({"requests":broken})).await;
    assert_eq!(
        response["error"]["data"]["category"], "execution_failure",
        "{response}"
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .generation()
            .unwrap(),
        after
    );
    let reader = repository.retained_reader().await.unwrap();
    assert_eq!(
        reader.root(&"batch/blob".parse().unwrap()).await.unwrap(),
        Some(original)
    );
    let replacement =
        casita::ObjectKey::blob(casita::BlobId::new(blake3::hash(b"replacement").into()));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&replacement)
            .await
            .unwrap()
            .is_none()
    );
    drop(reader);
    // A gzip trailer failure happens after tar payloads have been staged.
    let mut corrupt = std::fs::read(&tar).unwrap();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    std::fs::write(&tar, corrupt).unwrap();
    let response = call(&mut client, "artifact.import", requests.clone()).await;
    assert_eq!(
        response["error"]["data"]["category"], "execution_failure",
        "{response}"
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .generation()
            .unwrap(),
        after
    );
    for invalid in [
        json!([{ "path":filesystem,"root":"same" }, {"path":filesystem,"root":"same"}]),
        json!([{ "path":filesystem,"root":"valid" }, {"path":filesystem,"root":"../invalid"}]),
        json!([{ "path":filesystem,"root":"valid" }, {"importer":"copy","path":"/missing","source_root":"old","root":"new"}]),
        json!([42]),
        json!({"requests":[],"extra":true}),
    ] {
        assert!(call(&mut client, "artifact.import", invalid).await["error"].is_object());
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .generation()
                .unwrap(),
            after
        );
    }
    assert_eq!(
        call(&mut client, "artifact.import", json!([])).await["result"],
        json!([])
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .generation()
            .unwrap(),
        after
    );
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn batch_checkout_and_restore_return_ordered_results() {
    let repository = Arc::new(Repository::memory().unwrap());
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(super::server::serve_connection(repository, server));
    let mut client = BufReader::new(client);
    initialize(&mut client).await;
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("file"), b"contents").unwrap();
    let imports = call(
        &mut client,
        "artifact.import",
        json!([
            {"path":source,"root":"tree"},
            {"importer":"blob","path":source.join("file"),"root":"blob"}
        ]),
    )
    .await;
    assert!(imports["result"].is_array(), "{imports}");
    let checkout = work.path().join("checkout");
    let miss = work.path().join("miss");
    let result = call(
        &mut client,
        "artifact.checkout",
        json!({"requests":[
            {"root":"tree","path":checkout}, {"root":"missing","path":miss}
        ]}),
    )
    .await;
    assert_eq!(
        result["result"],
        json!([{"present":true},{"present":false}]),
        "{result}"
    );
    assert_eq!(std::fs::read(checkout.join("file")).unwrap(), b"contents");
    assert!(miss.is_dir());
    let restored_blob = work.path().join("restored-blob");
    let restored_tree = work.path().join("restored-tree");
    let absent = work.path().join("absent");
    let result = call(
        &mut client,
        "artifact.restore",
        json!([
            {"importer":"blob","root":"blob","path":restored_blob},
            {"root":"tree","path":restored_tree},
            {"root":"missing","path":absent}
        ]),
    )
    .await;
    assert_eq!(
        result["result"][0]["object"], imports["result"][1]["object"],
        "{result}"
    );
    assert_eq!(
        result["result"][1]["object"],
        imports["result"][0]["object"]
    );
    assert_eq!(result["result"][2], json!({"present":false}));
    assert_eq!(std::fs::read(restored_blob).unwrap(), b"contents");
    assert_eq!(
        std::fs::read(restored_tree.join("file")).unwrap(),
        b"contents"
    );
    assert!(!absent.exists());
    for method in ["artifact.checkout", "artifact.restore"] {
        assert_eq!(
            call(&mut client, method, json!([])).await["result"],
            json!([])
        );
        let valid = work.path().join(format!("{method}-valid"));
        let result = call(
            &mut client,
            method,
            json!([
                {"root":"tree","path":valid}, {"root":"tree"}
            ]),
        )
        .await;
        assert!(result["error"].is_object());
        assert!(!valid.exists());
    }
    drop(client);
    task.await.unwrap().unwrap();
}
