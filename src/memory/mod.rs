pub(crate) mod antipattern;
pub(crate) mod capture;
pub(crate) mod compression;
pub(crate) mod staleness;
pub(crate) mod store;

pub use capture::{HookPayload, PostToolUseResult, parse_post_tool_use};
pub use compression::{run_compression, DEFAULT_COMPRESSION_THRESHOLD_SECS};
pub use store::{
    SessionSummary, get_session_observations, insert_auto_observation, is_sensitive_path,
    list_sessions, upsert_session,
};
