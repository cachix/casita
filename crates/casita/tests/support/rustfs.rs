//! A private S3 fixture; application tests never construct Casita backends.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const BUCKET: &str = "casita-application-test";

pub struct Rustfs {
    child: Child,
    address: SocketAddr,
    data: tempfile::TempDir,
}

impl Rustfs {
    pub fn start() -> Self {
        let data = tempfile::tempdir().unwrap();
        std::fs::create_dir(data.path().join("objects")).unwrap();
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let console = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let log = std::fs::File::create(data.path().join("rustfs.log")).unwrap();
        let child = Command::new("rustfs")
            .arg("server")
            .arg(data.path().join("objects"))
            .arg("--address")
            .arg(address.to_string())
            .arg("--console-address")
            .arg(console.to_string())
            .env("RUSTFS_ACCESS_KEY", "minio")
            .env("RUSTFS_SECRET_KEY", "minio123")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("rustfs from devenv.nix must start");
        let mut fixture = Self {
            child,
            address,
            data,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if fixture.ready() {
                return fixture;
            }
            if fixture.child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!(
            "RustFS did not become ready: {}",
            std::fs::read_to_string(fixture.data.path().join("rustfs.log")).unwrap()
        );
    }

    /// The health endpoint reports ready while S3 requests still get 503
    /// (`x-rustfs-readiness-pending: startup_finalization`), so also require
    /// an S3 request to be served. Unauthenticated, that is 403.
    fn ready(&self) -> bool {
        self.status("/minio/health/ready")
            .is_some_and(|status| status == 200)
            && self.status("/").is_some_and(|status| status != 503)
    }

    fn status(&self, path: &str) -> Option<u16> {
        let mut stream =
            TcpStream::connect_timeout(&self.address, Duration::from_millis(100)).ok()?;
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .ok()?;
        let mut response = [0; 32];
        let read = stream.read(&mut response).ok()?;
        std::str::from_utf8(response[..read].strip_prefix(b"HTTP/1.1 ")?.get(..3)?)
            .ok()?
            .parse()
            .ok()
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub async fn create_bucket(&self) {
        let config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(self.endpoint())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "minio",
                "minio123",
                None,
                None,
                "application-test",
            ))
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(config)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await
            .unwrap();
    }

    pub fn configure(&self, command: &mut Command) {
        // Set credentials only on child processes, avoiding process-global
        // environment mutation in a parallel Rust test binary.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("AWS_") {
                command.env_remove(name);
            }
        }
        let config = self.data.path().join("empty-aws-config");
        std::fs::write(&config, "").unwrap();
        command
            .env("AWS_ACCESS_KEY_ID", "minio")
            .env("AWS_SECRET_ACCESS_KEY", "minio123")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", self.endpoint())
            .env("AWS_ALLOW_HTTP", "true")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_CONFIG_FILE", &config)
            .env("AWS_SHARED_CREDENTIALS_FILE", &config)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost");
    }
}

impl Drop for Rustfs {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
