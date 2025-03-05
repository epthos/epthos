fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=layout.proto");
    tonic_build::compile_protos("layout.proto")?;
    Ok(())
}
