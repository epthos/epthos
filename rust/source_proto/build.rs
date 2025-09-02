fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=source.proto");
    tonic_build::compile_protos("source.proto")?;
    Ok(())
}
