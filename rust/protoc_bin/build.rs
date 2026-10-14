use std::env;
use std::fs;
use std::path::PathBuf;

// Build scripts always run on the host machine (even when the crate depending on
// this one is being cross-compiled), so cfg(target_os) here reflects the host,
// which is exactly what we want when picking a protoc binary to *execute*.
#[cfg(target_os = "linux")]
const PROTOC_SRC: &str = "vendor/linux-x86_64/protoc";
#[cfg(target_os = "linux")]
const PROTOC_FILENAME: &str = "protoc";

#[cfg(target_os = "windows")]
const PROTOC_SRC: &str = "vendor/windows-x86_64/protoc.exe";
#[cfg(target_os = "windows")]
const PROTOC_FILENAME: &str = "protoc.exe";

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
compile_error!("protoc_bin only vendors protoc binaries for linux and windows hosts");

fn main() {
    println!("cargo:rerun-if-changed=vendor");

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR not set"));
    let protoc_path = out_dir.join(PROTOC_FILENAME);
    fs::copy(manifest_dir.join(PROTOC_SRC), &protoc_path)
        .expect("failed to copy vendored protoc to OUT_DIR");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&protoc_path, fs::Permissions::from_mode(0o755))
            .expect("failed to set executable bit on vendored protoc");
    }

    let include_dir = manifest_dir.join("vendor/include");

    println!("cargo:rustc-env=PROTOC_BIN_PATH={}", protoc_path.display());
    println!(
        "cargo:rustc-env=PROTOC_INCLUDE_PATH={}",
        include_dir.display()
    );
}
