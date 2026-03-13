pub(crate) mod diff;
pub(crate) mod full;
pub(crate) mod incremental;

// Minimal public re-export for the binary crate (cli/index.rs).
// The `full` module itself stays pub(crate) — only the run entry-point and its
// return type cross the library/binary boundary.
pub use full::{IndexStats, run};
// Re-exported for any caller that needs incremental re-indexing (changed files only, not a full rebuild).
// The module itself stays pub(crate) — callers use olaf::index::run_incremental().
pub use incremental::run as run_incremental;
// Re-exported for single-file reindex after edits; returns ReindexOutcome (changed with a
// structural diff, unchanged, or soft failure with a reason).
pub use diff::StructuralDiff;
pub use incremental::{reindex_single_file, ReindexOutcome, SoftFailureReason};

pub(crate) use crate::sensitive::is_sensitive;
