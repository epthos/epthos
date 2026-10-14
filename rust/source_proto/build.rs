use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=source.proto");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin::path());
    tonic_prost_build::configure().compile_with_config(
        config,
        &[Path::new("source.proto")],
        &[Path::new("."), protoc_bin::include_dir()],
    )?;
    Ok(())
}
