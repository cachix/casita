//! Command-line application setup and dispatch.

use std::process::ExitCode;

use clap::Parser;

mod args;
mod commands;
mod error;
mod ipc;
mod logging;

use args::*;
use error::{
    ApplicationExit, Error, UsageError, error_exit_code, stable_error_category, usage_error,
};
use logging::init_tracing;

pub(crate) async fn run() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = init_tracing(cli.log_filter.as_deref(), cli.log_format) {
        eprintln!("error: {error}");
        return ExitCode::from(2);
    }
    let result = commands::run(cli).await;
    let released = casita::experimental::flush_repository_leases().await;
    let result = result.and_then(|()| released.map_err(|error| Box::new(error) as Error));
    match result {
        Ok(()) => {
            tracing::info!("CLI command completed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            if let Some(ApplicationExit(status)) = e.downcast_ref::<ApplicationExit>() {
                #[cfg(unix)]
                let code = {
                    use std::os::unix::process::ExitStatusExt;
                    status
                        .code()
                        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
                };
                #[cfg(not(unix))]
                let code = status.code().unwrap_or(1);
                // Execution has returned and temporary outputs and repository
                // leases have been released. Preserve full Windows exit codes.
                std::process::exit(code);
            }
            tracing::error!(
                category = stable_error_category(e.as_ref()).as_str(),
                "CLI command failed"
            );
            if e.is::<UsageError>() {
                eprintln!("error: {e}");
            } else {
                eprintln!("error[{}]: {e}", stable_error_category(e.as_ref()).as_str());
            }
            ExitCode::from(error_exit_code(&e))
        }
    }
}

/// Flush `directory` after publishing a file in it, so a power loss cannot
/// drop the new name once the command has reported success.
fn sync_directory(directory: &std::path::Path) -> std::io::Result<()> {
    casita::experimental::sync_directory(directory)?;
    #[cfg(test)]
    SYNCED_DIRECTORIES
        .lock()
        .unwrap()
        .push(directory.to_path_buf());
    Ok(())
}

/// Every directory [`sync_directory`] has flushed in this test process.
#[cfg(test)]
static SYNCED_DIRECTORIES: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn was_synced(directory: &std::path::Path) -> bool {
    SYNCED_DIRECTORIES
        .lock()
        .unwrap()
        .iter()
        .any(|synced| synced == directory)
}

#[cfg(test)]
mod tests;
