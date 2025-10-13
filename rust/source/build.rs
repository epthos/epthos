use chrono::SubsecRound;
use std::{path::PathBuf, process::Command};

// Generate static build information for introspection.
fn main() -> anyhow::Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    let timestamp = chrono::Utc::now().round_subsecs(0).to_rfc3339();
    let sha = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()?
            .stdout,
    )?;
    let short_sha = sha.trim();

    // Build a friendly release string from the metadata.
    let release = format!("EpthosSource v{version} (built on {timestamp} from SHA:{short_sha})");
    let out_dir: PathBuf = std::env::var("OUT_DIR")?.into();
    std::fs::write(
        out_dir.join("built_info.rs"),
        format!(
            r#"
mod release_info {{
// Compile-time release identifier
pub const HUMAN_READABLE: &str = "{}";
}}
"#,
            release
        ),
    )?;
    Ok(())
}
