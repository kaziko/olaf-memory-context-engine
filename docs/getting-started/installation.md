# Installation

Choose any of the three methods:

## Homebrew (macOS — recommended)

```sh
brew tap kaziko/olaf
brew install olaf
```

## cargo

```sh
cargo install olaf
```

Requires Rust toolchain. Install from [rustup.rs](https://rustup.rs) if needed.

## Pre-built binary

Download the binary for your platform from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases):

| Platform | Binary |
|-|-|
| macOS (Apple Silicon) | `olaf-aarch64-apple-darwin` |
| macOS (Intel) | `olaf-x86_64-apple-darwin` |
| Linux (x86_64) | `olaf-x86_64-unknown-linux-musl` |
| Linux (ARM64) | `olaf-aarch64-unknown-linux-musl` |

Linux binaries are fully static — no glibc dependency.

Rename to `olaf`, make executable (`chmod +x olaf`), and move to a directory in your PATH.
