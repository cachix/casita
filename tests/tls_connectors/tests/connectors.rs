//! Exercise the WAL and payload HTTPS connectors without external services.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_http_client::tls::{self, TlsContext, TrustStore};
use object_store::ObjectStore;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

struct Endpoint {
    url: String,
    ca_pem: String,
    server: std::thread::JoinHandle<bool>,
}

impl Endpoint {
    fn start(hostname: &str) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "Casita test CA");
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec![hostname.into()])
            .unwrap()
            .signed_by(&key, &ca, &ca_key)
            .unwrap();
        assert_ne!(
            ca.params().distinguished_name,
            leaf.params().distinguished_name,
            "the server leaf must not appear self-issued to platform verifiers"
        );
        let config = rustls::ServerConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "TLS client never connected"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => panic!("accept: {e}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, socket);
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                if let Err(error) = stream.read_exact(&mut byte) {
                    eprintln!("fixture TLS handshake/read: {error:?}");
                    return false;
                }
                request.push(byte[0]);
                assert!(request.len() < 32768);
            }
            let body = b"<?xml version=\"1.0\"?><ListBucketResult><Name>test</Name><IsTruncated>false</IsTruncated></ListBucketResult>";
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            stream.write_all(body).unwrap();
            stream.conn.send_close_notify();
            stream.flush().unwrap();
            // Drain the peer before closing the socket. Closing with unread
            // TLS records can reset the connection on supported platforms.
            let mut drain = [0; 1024];
            while stream.read(&mut drain).is_ok_and(|read| read != 0) {}
            true
        });
        Self {
            url,
            ca_pem: ca.pem(),
            server,
        }
    }
}

async fn aws_request(endpoint: &Endpoint, trust_ca: bool) -> Result<(), String> {
    let trust = if trust_ca {
        TrustStore::empty().with_pem_certificate(endpoint.ca_pem.as_bytes())
    } else {
        TrustStore::default()
    };
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .tls_context(
            TlsContext::builder()
                .with_trust_store(trust)
                .build()
                .unwrap(),
        )
        .build_https();
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(&endpoint.url)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "test", "test", None, None, "tls-test",
        ))
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .behavior_version_latest()
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
        .http_client(http)
        .build();
    aws_sdk_s3::Client::from_conf(config)
        .list_objects_v2()
        .bucket("test")
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

async fn payload_request(endpoint: &Endpoint, trust_ca: bool) -> Result<(), String> {
    let mut options = object_store::ClientOptions::new().with_timeout(Duration::from_secs(5));
    if trust_ca {
        options = options.with_root_certificate(
            object_store::Certificate::from_pem(endpoint.ca_pem.as_bytes()).unwrap(),
        );
    }
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&endpoint.url)
        .with_bucket_name("test")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_client_options(options)
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap();
    store
        .list_with_delimiter(None)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

#[tokio::test]
async fn wal_and_payload_tls_authenticate_issuer_and_hostname() {
    for aws in [true, false] {
        for (hostname, trust_ca, expected) in [
            ("127.0.0.1", true, true),
            ("127.0.0.1", false, false),
            ("another.example", true, false),
        ] {
            let endpoint = Endpoint::start(hostname);
            let result = tokio::time::timeout(Duration::from_secs(8), async {
                if aws {
                    aws_request(&endpoint, trust_ca).await
                } else {
                    payload_request(&endpoint, trust_ca).await
                }
            })
            .await
            .expect("bounded TLS request");
            let saw_http = endpoint.server.join().unwrap();
            assert_eq!(
                result.is_ok(),
                expected,
                "aws={aws}, host={hostname}, trust={trust_ca}: {result:?}"
            );
            assert_eq!(
                saw_http, expected,
                "rejected peers must not receive HTTP credentials"
            );
            if !expected {
                let error = result.unwrap_err().to_lowercase();
                assert!(
                    error.contains("cert"),
                    "expected certificate rejection: {error}"
                );
            }
        }
    }
}
