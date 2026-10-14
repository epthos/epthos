use std::{path::PathBuf, process::Command};

// Generate static build information for introspection.
fn main() -> anyhow::Result<()> {
    // Rebuild whenever the commit or branch changes, so RELEASE stays fresh.
    let git_dir = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "--git-dir"])
            .output()?
            .stdout,
    )?;
    let git_dir = git_dir.trim();
    println!("cargo:rerun-if-changed={git_dir}/HEAD");
    println!("cargo:rerun-if-changed={git_dir}/refs");

    let version = env!("CARGO_PKG_VERSION");
    let sha = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()?
            .stdout,
    )?;
    let short_sha = sha.trim();

    // Build a friendly release string from the metadata. RELEASE omits the
    // binary name, as --version already prefixes it. No timestamp, so the
    // build is repeatable given the same source and version.
    let release = format!("v{version} (SHA:{short_sha})");
    let out_dir: PathBuf = std::env::var("OUT_DIR")?.into();
    std::fs::write(
        out_dir.join("built_info.rs"),
        format!(
            r#"
mod release_info {{
// Compile-time release identifier
pub const RELEASE: &str = "{release}";
}}
"#
        ),
    )?;
    Ok(())
}
