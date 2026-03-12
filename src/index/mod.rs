pub(crate) mod diff;
pub(crate) mod full;
pub(crate) mod incremental;

// Minimal public re-export for the binary crate (cli/index.rs).
// The `full` module itself stays pub(crate) — only the run entry-point and its
// return type cross the library/binary boundary.
pub use full::{IndexStats, run};
// Expose incremental run for integration tests and the MCP query path (Story 2.2).
// The module itself stays pub(crate) — callers use olaf::index::run_incremental().
pub use incremental::run as run_incremental;
// Expose single-file reindex and outcome types for the PostToolUse hook (Story 7.1).
pub use diff::StructuralDiff;
pub use incremental::{reindex_single_file, ReindexOutcome, SoftFailureReason};

pub(crate) use crate::sensitive::is_sensitive;
