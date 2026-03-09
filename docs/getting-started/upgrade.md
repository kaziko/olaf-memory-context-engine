# Upgrade

=== "Homebrew"

    ```sh
    brew update && brew upgrade olaf
    ```

=== "cargo"

    ```sh
    cargo install olaf --force
    ```

=== "Pre-built binary"

    Download the latest release from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases), replace the existing `olaf` binary in your PATH with the new one.

!!! tip
    After upgrading, run `olaf init` in each project to update hooks and tool preference rules. It's idempotent — safe to re-run.
