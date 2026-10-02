//! Read an existing blob completely; EOF verifies Casita's whole-blob digest.
use casita::{ObjectKey, Repository};
use std::{error::Error, path::Path};
use tokio::{
    io::AsyncReadExt,
    time::{Duration, timeout},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: packed_archive_read REPOSITORY OBJECT_KEY EXPECTED_BYTES".into());
    }
    let expected: u64 = args[3].parse()?;
    let repository = Repository::local(Path::new(&args[1])).await?;
    let key: ObjectKey = args[2].parse()?;
    let mut reader = repository.open(&key).await?.ok_or("blob missing")?;
    let mut buffer = vec![0_u8; 128 * 1024];
    let mut total = 0_u64;
    loop {
        let count = timeout(Duration::from_secs(30), reader.read(&mut buffer)).await??;
        if count == 0 {
            break;
        }
        total += count as u64;
    }
    if total != expected {
        return Err(format!("expected {expected} bytes, read {total}").into());
    }
    println!("verified_bytes {total}");
    Ok(())
}
