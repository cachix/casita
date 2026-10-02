//! Link settings for the `casita` binary.

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    // Windows gives the main thread 1 MiB of stack, where Linux gives 8 MiB.
    // The CLI runs its commands on the main thread, and unoptimized builds
    // overflowed 1 MiB during archive verification, so match Linux.
    const STACK_BYTES: u32 = 8 * 1024 * 1024;
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }
    match std::env::var("CARGO_CFG_TARGET_ENV").as_deref() {
        Ok("msvc") => println!("cargo::rustc-link-arg-bins=/STACK:{STACK_BYTES}"),
        Ok("gnu") => println!("cargo::rustc-link-arg-bins=-Wl,--stack,{STACK_BYTES}"),
        _ => {}
    }
}
