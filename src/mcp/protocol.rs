use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Incoming JSON-RPC 2.0 message (request or notification).
/// Notifications have no `id` field — `serde(default)` maps absence to `None`.
#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

/// Outgoing JSON-RPC 2.0 response.
///
/// `id` is ALWAYS serialized — JSON-RPC 2.0 §5 requires `id: null` in error
/// responses when the request id could not be determined (e.g. parse failure).
/// Never use `skip_serializing_if` on `id`.
#[derive(Debug, Serialize)]
pub(crate) struct Response {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(flatten)]
    pub body: ResponseBody,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum ResponseBody {
    Result { result: Value },
    Error { error: ErrorObject },
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorObject {
    pub code: i32,
    pub message: String,
}

impl Response {
    /// Success response. `id` must be the request's id value (never Null for successes).
    pub(crate) fn ok(id: Value, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, body: ResponseBody::Result { result } }
    }

    /// Error response. Pass `Value::Null` when the request id is unknown (parse errors).
    pub(crate) fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            body: ResponseBody::Error { error: ErrorObject { code, message } },
        }
    }
}
