# Content Policy Filtering

If your project has modules that should never appear in AI context — internal billing logic, authentication secrets, proprietary algorithms — you can define content policies in `.olaf/policy.toml`.

## Configuration

Create `.olaf/policy.toml`:

```toml
# Completely exclude from all output — AI never sees these
[[deny]]
path = "internal/billing/**"
reason = "Contains PII processing logic"

[[deny]]
fqn_prefix = "src/crypto.rs::KeyManager"
reason = "Cryptographic internals"

# Show signatures for navigation, but strip implementation bodies
[[redact]]
path = "src/auth/**"
reason = "Auth internals — signatures visible, bodies hidden"
```

## Rule types

**Deny rules** silently exclude matching files and symbols from all MCP tool output — context briefs, impact analysis, file skeletons, trace flow, session history, and failure analysis. Denied symbols are indistinguishable from non-existent symbols (no "access denied" messages that would confirm their existence).

**Redact rules** preserve symbol signatures for navigation but replace implementation bodies with `[redacted by policy]`.

## Key behaviors

- Policy is additive to hardcoded sensitive-file rules (`.env`, `.pem`, `id_rsa`) — those are never bypassed
- Policy is loaded fresh on every tool call — create, edit, or delete the file and changes take effect immediately without restarting the server
- Malformed policy files are ignored with a warning — the server never crashes due to policy errors
- Deny takes precedence over redact when both match the same path
