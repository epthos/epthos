fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=sink.proto");
    tonic_build::compile_protos("sink.proto")?;
    Ok(())
}
