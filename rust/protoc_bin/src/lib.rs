use std::path::Path;

/// Path to the vendored `protoc` binary for the current host, extracted to `OUT_DIR` at build
/// time.
pub fn path() -> &'static Path {
    Path::new(env!("PROTOC_BIN_PATH"))
}

/// Path to the vendored `google/protobuf/*.proto` well-known type definitions, for use as a
/// protoc include (`-I`) path.
pub fn include_dir() -> &'static Path {
    Path::new(env!("PROTOC_INCLUDE_PATH"))
}
