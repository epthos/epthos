# Overall structure

Rust packages are flat for simplicity. Related packages share a prefix. 

- ./docker: build & package the broker.
- ./scripts: generate test certificates.

- ./rust/data_proto: in-memory data representation of content.
- ./rust/layout_proto: on-disk representation of content.

- ./rust/platform: platform-specific logic.
- ./rust/crypto: encryption/decryption of content.
- ./rust/netutil: helper to identify the local host's IP, etc. Merge into platform?
- ./rust/rpcutil: helpers (exponential backoff, testing) for client/server code.
- ./rust/constants: project-wide constants (names, etc)

- ./rust/source: user service that tracks changes and backs them up.
- ./rust/source_settings: source settings that can be configured by the CLI.

- ./rust/cli: helper tool, could eventually become a TUI for power users.

- ./rust/sink: user service that manages data being backed up.
- ./rust/sink_proto
- ./rust/sink_client
- ./rust/sink_settings

- ./rust/storage: generally obsolete or used by experiments.
- ./rust/storage/filesystem: obsoleted by disk for low-level ops
- ./rust/storage/layout: part of experiments.

- ./rust/settings: parsing, writing & validation of settings.

- ./rust/broker: admin service which puts components in touch with each other.
- ./rust/broker_client
- ./rust/broker_proto

- ./rust/experiments
