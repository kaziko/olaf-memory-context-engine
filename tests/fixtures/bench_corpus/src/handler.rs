use crate::auth::{AuthHandler, AuthError};

pub struct RequestHandler {
    auth: AuthHandler,
}

impl RequestHandler {
    pub fn new(auth: AuthHandler) -> Self {
        RequestHandler { auth }
    }

    pub fn process_request(&self, token: &str, payload: &str) -> Result<String, HandlerError> {
        let _user = self.auth.validate(token)
            .map_err(|_| HandlerError::Unauthorized)?;
        Ok(format!("processed: {}", payload))
    }

    pub fn middleware_chain(&self, request: &str) -> Result<String, HandlerError> {
        if request.is_empty() {
            return Err(HandlerError::BadRequest);
        }
        Ok(request.to_uppercase())
    }
}

pub enum HandlerError {
    Unauthorized,
    BadRequest,
    InternalError(String),
}

pub struct ResponseBuilder {
    status_code: u16,
    body: String,
}

impl ResponseBuilder {
    pub fn new(status_code: u16) -> Self {
        ResponseBuilder {
            status_code,
            body: String::new(),
        }
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    pub fn build_response(&self) -> String {
        format!("HTTP {} {}", self.status_code, self.body)
    }
}
