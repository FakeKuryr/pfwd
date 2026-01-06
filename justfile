# keep commands consistent between developers
set shell := ["zsh", "-c"]

default: test

# Run the standard debug build.
build:
	cargo build

# Optimized release build for native target.
release:
	cargo build --release

# Build the static Linux binary used in deployments.
musl-release:
	cargo build --target x86_64-unknown-linux-musl --release

# Run the test suite.
test:
	cargo test

# Lint the code with clippy.
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Format the workspace.
fmt:
	cargo fmt --all

# Execute the binary with arbitrary CLI args, e.g. `just run -- --help`.
run *ARGS:
	cargo run -- {{ARGS}}
