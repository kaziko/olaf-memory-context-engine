use assert_cmd::Command;
use std::fs;

#[test]
fn test_status_uninitialized() {
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("status")
        .output()
        .unwrap();

    assert!(output.status.success(), "exit code must be 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("not initialized"),
        "stdout must mention uninitialized; got: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "stderr must be empty for uninitialized path; got: {stderr}");
}

#[test]
fn test_status_initialized() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("main.rs"), "pub fn main() {}").unwrap();

    #[allow(deprecated)]
    Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("index")
        .assert()
        .success();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("status")
        .output()
        .unwrap();

    assert!(output.status.success(), "exit code must be 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for label in ["Files indexed:", "Symbols:", "Edges:", "Observations:", "Last indexed:"] {
        assert!(
            stdout.contains(label),
            "stdout must contain '{label}'; got:\n{stdout}"
        );
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    for label in ["Files indexed:", "Symbols:", "Edges:", "Observations:", "Last indexed:"] {
        assert!(
            !stderr.contains(label),
            "status label '{label}' must not appear on stderr; got:\n{stderr}"
        );
    }
}

#[test]
fn test_status_shows_tool_preferences() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("main.rs"), "pub fn main() {}").unwrap();

    // Initialize to create rules file
    #[allow(deprecated)]
    Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("init")
        .assert()
        .success();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("status")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Tool preferences:"),
        "status must show Tool preferences line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("current"),
        "tool preferences must show 'current' after init; got:\n{stdout}"
    );
}

#[test]
fn test_status_tool_preferences_missing() {
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf")
        .unwrap()
        .current_dir(dir.path())
        .arg("status")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Tool preferences:") && stdout.contains("missing"),
        "status must show 'missing' tool preferences when no rules file exists; got:\n{stdout}"
    );
}
