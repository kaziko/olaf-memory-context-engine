use std::collections::HashMap;

pub enum AuthError {
    InvalidCredentials,
    TokenExpired,
    PermissionDenied,
}

pub struct AuthHandler {
    sessions: HashMap<String, String>,
}

impl AuthHandler {
    pub fn new() -> Self {
        AuthHandler {
            sessions: HashMap::new(),
        }
    }

    pub fn validate(&self, token: &str) -> Result<String, AuthError> {
        self.sessions
            .get(token)
            .cloned()
            .ok_or(AuthError::InvalidCredentials)
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Result<String, AuthError> {
        if username.is_empty() || password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }
        Ok(format!("token_{}", username))
    }

    pub fn revoke_session(&mut self, token: &str) {
        self.sessions.remove(token);
    }
}

pub struct PermissionChecker {
    role_map: HashMap<String, Vec<String>>,
}

impl PermissionChecker {
    pub fn check_access(&self, user_id: &str, resource: &str) -> Result<bool, AuthError> {
        match self.role_map.get(user_id) {
            Some(roles) => Ok(roles.iter().any(|r| r == resource)),
            None => Err(AuthError::PermissionDenied),
        }
    }
}
