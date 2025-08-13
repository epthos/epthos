fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=data.proto");
    tonic_prost_build::compile_protos("data.proto")?;
    Ok(())
}
