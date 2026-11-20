use std::io::Write;
use std::process::{Command, Stdio};

const XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<i>
  <d p="1.0,1,25,16777215">A &amp; B</d>
  <d p="1.4,1,25,16777215">A &amp; B</d>
</i>"#;

fn run_cli(args: &[&str], input: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_danmaku-renderer"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI starts");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(input.as_bytes())
        .expect("input is accepted");
    child.wait_with_output().expect("CLI exits")
}

#[test]
fn stdin_renders_decoded_and_grouped_comment_text_to_stdout() {
    let output = run_cli(&["-"], XML);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ass = String::from_utf8(output.stdout).expect("ASS is UTF-8");
    assert!(
        ass.contains("A & B"),
        "decoded XML text missing from ASS: {ass}"
    );
    assert!(ass.contains("(2)"), "grouped count missing from ASS: {ass}");
    assert_eq!(
        ass.matches("Dialogue:").count(),
        1,
        "comments should share one event"
    );
}

#[test]
fn malformed_xml_fails_without_writing_partial_ass() {
    let output = run_cli(&["-"], "<i><d p=\"1,1,25,0\">broken</i>");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to render XML danmaku"),
        "unexpected error: {stderr}"
    );
    assert!(
        !stderr.contains("broken"),
        "error output must not echo source text: {stderr}"
    );
}

#[test]
fn font_scale_option_is_applied() {
    let default = run_cli(&["-"], XML);
    let scaled = run_cli(&["--font-scale", "2", "-"], XML);

    assert!(default.status.success());
    assert!(scaled.status.success());
    assert_ne!(
        default.stdout, scaled.stdout,
        "font scale should affect the rendered ASS"
    );
}
