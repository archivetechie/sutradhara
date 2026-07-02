//! Fixture tests for the Rust receive-core M2 implementation.
//!
//! These tests read the Python-derived corpus committed in M1. The Rust crate is
//! therefore checked against the same public contract that will gate the later
//! write-side, binary, and PyO3 wheel migration stages.

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime};
use sutradhara_receive::{
    BAG_PROFILE, BAGIT_TEXT, CANONICALIZATION_VERSION, PACKAGE_GLOBS, PACKAGE_PROFILE_HASH,
    PACKAGE_PROFILE_VERSION, RECEIVE_PACKAGE, RECEIVE_VERSION, ReceiveOptions, bag_info_text,
    bagit_manifest_text, build_package_tar, canonicalize_manifest_path,
    canonicalize_raw_path_components, escape_member_name, hash_payload_tree,
    hash_payload_tree_with_policy, manifest_mismatch, read_manifest_sha256, receive_source,
    sha256_file, tagmanifest_text, unescape_member_name, validate_bag,
};

#[test]
fn string_fixtures_match_python_contract() {
    let fixture = read_fixture("strings.json");

    for case in fixture["escape_member_name"].as_array().unwrap() {
        let raw = parse_hex(case["raw_hex"].as_str().unwrap());
        let escaped = case["escaped"].as_str().unwrap();
        assert_eq!(escape_member_name(&raw), escaped, "{case:#?}");
        assert_eq!(
            hex_lower(&unescape_member_name(escaped).unwrap()),
            case["roundtrip_hex"].as_str().unwrap(),
            "{case:#?}"
        );
    }

    for case in fixture["canonicalize_filesystem_path"].as_array().unwrap() {
        let components: Vec<Vec<u8>> = case["components_hex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| parse_hex(item.as_str().unwrap()))
            .collect();
        assert_eq!(
            canonicalize_raw_path_components(&components).unwrap(),
            case["canonical"].as_str().unwrap(),
            "{case:#?}"
        );
    }

    for case in fixture["canonicalize_manifest_path"].as_array().unwrap() {
        assert_eq!(
            canonicalize_manifest_path(case["raw"].as_str().unwrap()).unwrap(),
            case["canonical"].as_str().unwrap(),
            "{case:#?}"
        );
    }

    for case in fixture["canonicalize_manifest_rejections"]
        .as_array()
        .unwrap()
    {
        let error = canonicalize_manifest_path(case["raw"].as_str().unwrap())
            .unwrap_err()
            .to_string();
        assert_eq!(error, case["error"].as_str().unwrap(), "{case:#?}");
    }
}

#[test]
fn writer_text_fixtures_match_python_contract() {
    let fixture = read_fixture("writer_outputs.json");
    let constants = &fixture["constants"];
    assert_eq!(
        RECEIVE_VERSION,
        constants["RECEIVE_VERSION"].as_str().unwrap()
    );
    assert_eq!(
        CANONICALIZATION_VERSION,
        constants["CANONICALIZATION_VERSION"].as_str().unwrap()
    );
    assert_eq!(
        PACKAGE_PROFILE_VERSION,
        constants["PACKAGE_PROFILE_VERSION"].as_str().unwrap()
    );
    assert_eq!(
        PACKAGE_PROFILE_HASH,
        constants["PACKAGE_PROFILE_HASH"].as_str().unwrap()
    );
    assert_eq!(BAG_PROFILE, constants["BAG_PROFILE"].as_str().unwrap());
    assert_eq!(
        RECEIVE_PACKAGE,
        constants["RECEIVE_PACKAGE"].as_str().unwrap()
    );
    assert_eq!(
        PACKAGE_GLOBS,
        constants["PACKAGE_GLOBS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );

    let writer = &fixture["bagit_writer"];
    assert_eq!(BAGIT_TEXT, writer["bagit.txt"].as_str().unwrap());

    let mut manifest_entries = BTreeMap::new();
    manifest_entries.insert(
        "clip%.mov".to_string(),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_string(),
    );
    assert_eq!(
        bagit_manifest_text(&manifest_entries).unwrap(),
        writer["manifest-sha256.txt"].as_str().unwrap()
    );

    assert_eq!(
        bag_info_text(&bag_info_metadata()),
        writer["bag-info.txt"].as_str().unwrap()
    );

    let temp = tempfile::tempdir().unwrap();
    for name in ["bagit.txt", "bag-info.txt", "manifest-sha256.txt"] {
        fs::write(temp.path().join(name), writer[name].as_str().unwrap()).unwrap();
    }
    let tag_files = vec![
        "bagit.txt".to_string(),
        "bag-info.txt".to_string(),
        "manifest-sha256.txt".to_string(),
    ];
    assert_eq!(
        tagmanifest_text(temp.path(), &tag_files).unwrap(),
        writer["tagmanifest-sha256.txt"].as_str().unwrap()
    );
}

#[cfg(unix)]
#[test]
fn package_tar_fixture_matches_python_contract() {
    let fixture = read_fixture("writer_outputs.json");
    let package_fixture = &fixture["package_tar"];
    let temp = tempfile::tempdir().unwrap();
    let package_root = temp.path().join("GOLDEN.fcpbundle");
    write_package_fixture(&package_root);
    let tar_path = temp.path().join("GOLDEN.fcpbundle.tar");

    let result = build_package_tar(&package_root, &tar_path, "GOLDEN.fcpbundle").unwrap();

    assert_eq!(result.digest, package_fixture["sha256"].as_str().unwrap());
    assert_eq!(result.size_bytes, package_fixture["size"].as_u64().unwrap());
    assert_eq!(
        fs::metadata(&tar_path).unwrap().len(),
        package_fixture["size"].as_u64().unwrap()
    );
    assert_eq!(
        serde_json::to_value(&result.members).unwrap(),
        package_fixture["package_index"]["packages"][0]["members"]
    );
}

#[cfg(unix)]
#[test]
fn receive_bag_fixtures_match_python_contract() {
    let fixture = read_fixture("receive_bags.json");
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let landing = temp.path().join("landing");
        match name {
            "mixed-files" => write_mixed_source(&source),
            "package-with-symlink" => write_package_fixture(&source.join("A001.photoslibrary")),
            other => panic!("unhandled receive fixture {other}"),
        }

        let result = receive_source(&source, &landing, &receive_options(name)).unwrap();

        assert_eq!(receive_result_payload(&result), case["result"]);
        assert_eq!(snapshot_bag_files(&result.intake_dir), case["files"]);
    }
}

#[cfg(unix)]
#[test]
fn cli_matrix_fixtures_match_rust_binary() {
    let fixture = read_fixture("cli_matrix.json");
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let case_root = temp.path().join(name);
        let source = case_root.join("source");
        let landing = case_root.join("landing");
        if matches!(
            name,
            "bare-fake-source-json"
                | "explicit-run-json"
                | "confirm-timeout-exit-3"
                | "source-and-fake-source-usage"
        ) {
            fs::create_dir_all(&source).unwrap();
            fs::write(source.join("clip.mov"), b"video").unwrap();
        }
        if matches!(name, "sweep-json" | "sweep-orphans-json") {
            write_sweep_fixture(&landing);
        }

        let argv = argv_from_fixture(case, temp.path(), &source, &landing);
        let output = Command::new(env!("CARGO_BIN_EXE_sutra-receive"))
            .args(&argv)
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        let intake_id = json_intake_id(&stdout);

        assert_eq!(
            output.status.code().unwrap(),
            case["exit_code"].as_i64().unwrap() as i32,
            "{name}"
        );
        assert_eq!(
            normalize_cli_text(
                &stdout,
                temp.path(),
                &source,
                &landing,
                intake_id.as_deref()
            ),
            case["stdout"].as_str().unwrap(),
            "{name} stdout"
        );
        assert_eq!(
            normalize_cli_text(
                &stderr,
                temp.path(),
                &source,
                &landing,
                intake_id.as_deref()
            ),
            case["stderr"].as_str().unwrap(),
            "{name} stderr"
        );
        let expected_json = &case["stdout_json"];
        if expected_json.is_null() {
            assert!(stdout.trim().is_empty(), "{name}");
        } else {
            let normalized_stdout = normalize_cli_text(
                &stdout,
                temp.path(),
                &source,
                &landing,
                intake_id.as_deref(),
            );
            let actual_json: Value = serde_json::from_str(&normalized_stdout).unwrap();
            assert_eq!(&actual_json, expected_json, "{name} stdout_json");
        }
    }
}

#[test]
fn validate_mismatch_fixtures_match_python_contract() {
    let fixture = read_fixture("validate_mismatch.json");

    for case in fixture["manifest_mismatch"].as_array().unwrap() {
        let actual = json_object_to_map(&case["actual"]);
        let expected = json_object_to_map(&case["expected"]);
        assert_eq!(
            manifest_mismatch(&actual, &expected).unwrap(),
            case["result"],
            "{}",
            case["name"].as_str().unwrap()
        );
    }

    for case in fixture["validate_bag"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let bag = simple_received_bag(temp.path(), name);
        match name {
            "missing-payload" => {
                fs::remove_file(bag.join("data").join("clip.mov")).unwrap();
            }
            "corrupt-payload" => {
                fs::write(bag.join("data").join("clip.mov"), b"corrupt").unwrap();
            }
            "unsupported-package" => {
                rewrite_receive_package(&bag, "sutradhara-receive/999.0.0");
            }
            other => panic!("unhandled validate fixture {other}"),
        }

        assert_eq!(validation_snapshot(name, &validate_bag(&bag)), *case);
    }
}

#[test]
fn manifest_reader_rejects_non_ascii_digest_without_panic() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = temp.path().join("manifest-sha256.txt");
    fs::write(
        &manifest,
        format!("{}{}  data/clip.mov\n", "a".repeat(63), "\u{e9}"),
    )
    .unwrap();

    let error = read_manifest_sha256(&manifest).unwrap_err().to_string();
    assert!(error.contains("invalid sha256 at line 1"));
}

#[test]
fn tagmanifest_absolute_path_is_reported_unsafe() {
    let temp = tempfile::tempdir().unwrap();
    let bag = simple_received_bag(temp.path(), "absolute-tagmanifest");
    let digest = sha256_file(&bag.join("bag-info.txt")).unwrap();
    fs::write(
        bag.join("tagmanifest-sha256.txt"),
        format!("{digest}  /bag-info.txt\n"),
    )
    .unwrap();

    let validation = validate_bag(&bag);
    let tag_mismatches = json!(validation.tag_mismatched);
    assert!(tag_mismatches.as_array().unwrap().contains(&json!({
        "actual": "unsafe path",
        "expected": digest,
        "path": "/bag-info.txt",
    })));
}

#[test]
fn hash_payload_tree_rejects_native_packages_only_when_requested() {
    let temp = tempfile::tempdir().unwrap();
    let payload = temp.path().join("data");
    let package = payload.join("A001.photoslibrary");
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("asset.mov"), b"video").unwrap();

    let receipts = hash_payload_tree(&payload).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].relpath, "A001.photoslibrary/asset.mov");

    let error = hash_payload_tree_with_policy(&payload, true)
        .unwrap_err()
        .to_string();
    assert!(error.contains("un-normalized package directory"));
}

#[test]
fn tagmanifest_dot_path_is_reported_unsafe() {
    let temp = tempfile::tempdir().unwrap();
    let bag = simple_received_bag(temp.path(), "dot-tagmanifest");
    fs::write(
        bag.join("tagmanifest-sha256.txt"),
        format!("{}  .\n", "0".repeat(64)),
    )
    .unwrap();

    let validation = validate_bag(&bag);
    let tag_mismatches = json!(validation.tag_mismatched);
    assert!(tag_mismatches.as_array().unwrap().contains(&json!({
        "actual": "unsafe path",
        "expected": "0".repeat(64),
        "path": ".",
    })));
}

fn read_fixture(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn json_object_to_map(value: &Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_string()))
        .collect()
}

fn simple_received_bag(root: &Path, name: &str) -> std::path::PathBuf {
    let source = root.join("source").join(name);
    let landing = root.join("landing").join(name);
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("clip.mov"), b"video").unwrap();
    receive_source(&source, &landing, &receive_options(name))
        .unwrap()
        .intake_dir
}

fn rewrite_receive_package(bag: &Path, receive_package: &str) {
    let bag_info_path = bag.join("bag-info.txt");
    let rewritten = fs::read_to_string(&bag_info_path)
        .unwrap()
        .lines()
        .map(|line| {
            if line.starts_with("Receive-Package: ") {
                format!("Receive-Package: {receive_package}")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(&bag_info_path, rewritten).unwrap();
    let tag_files = vec![
        "bagit.txt".to_string(),
        "bag-info.txt".to_string(),
        "manifest-sha256.txt".to_string(),
    ];
    fs::write(
        bag.join("tagmanifest-sha256.txt"),
        tagmanifest_text(bag, &tag_files).unwrap(),
    )
    .unwrap();
}

fn validation_snapshot(name: &str, validation: &sutradhara_receive::BagValidationResult) -> Value {
    json!({
        "complete": validation.complete(),
        "details": validation.details(),
        "errors": validation.errors,
        "extra": validation.extra,
        "mismatched": validation.mismatched,
        "missing": validation.missing,
        "name": name,
        "tag_mismatched": validation.tag_mismatched,
        "valid": validation.valid(),
    })
}

#[cfg(unix)]
fn argv_from_fixture(case: &Value, root: &Path, source: &Path, landing: &Path) -> Vec<String> {
    case["argv"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap()
                .replace("<FIXTURE-ROOT>", root.to_str().unwrap())
                .replace("<SOURCE>", source.to_str().unwrap())
                .replace("<LANDING>", landing.to_str().unwrap())
        })
        .collect()
}

#[cfg(unix)]
fn json_intake_id(stdout: &str) -> Option<String> {
    serde_json::from_str::<Value>(stdout)
        .ok()
        .and_then(|payload| payload["intake_id"].as_str().map(ToString::to_string))
}

#[cfg(unix)]
fn normalize_cli_text(
    value: &str,
    root: &Path,
    source: &Path,
    landing: &Path,
    intake_id: Option<&str>,
) -> String {
    let mut normalized = value
        .replace(source.to_str().unwrap(), "<SOURCE>")
        .replace(landing.to_str().unwrap(), "<LANDING>")
        .replace(root.to_str().unwrap(), "<FIXTURE-ROOT>");
    if let Some(intake_id) = intake_id {
        normalized = normalized.replace(intake_id, "<INTAKE-ID>");
    }
    normalized
}

#[cfg(unix)]
fn write_sweep_fixture(landing: &Path) {
    use filetime::{FileTime, set_file_mtime};

    for name in ["stale", "fresh", "complete"] {
        let path = landing.join(name);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(".receiving.json"), "{}").unwrap();
    }
    fs::write(landing.join("complete").join("intake.json"), "{}").unwrap();
    let old = FileTime::from_system_time(SystemTime::now() - Duration::from_secs(48 * 3600));
    set_file_mtime(landing.join("stale").join(".receiving.json"), old).unwrap();
}

#[cfg(unix)]
fn write_mixed_source(source: &Path) {
    fs::create_dir_all(source).unwrap();
    fs::write(source.join("clip.mov"), b"video").unwrap();
    fs::write(source.join("Cafe\u{301}.mov"), b"cafe").unwrap();
}

#[cfg(unix)]
fn write_package_fixture(package_root: &Path) {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::symlink;

    let render = package_root.join("Render");
    fs::create_dir_all(&render).unwrap();
    fs::write(render.join("clip01.mov"), b"clip-one").unwrap();
    fs::write(render.join("clip02.mov"), b"clip-two").unwrap();
    fs::write(
        render.join(Path::new(OsStr::from_bytes(b"\xfelegacy.dat"))),
        b"legacy",
    )
    .unwrap();
    fs::write(package_root.join("._meta"), b"appledouble").unwrap();
    fs::write(package_root.join("library.plist"), b"plist").unwrap();
    symlink("Render/clip01.mov", package_root.join("clip-link.mov")).unwrap();
}

#[cfg(unix)]
fn receive_options(name: &str) -> ReceiveOptions {
    ReceiveOptions {
        intake_id: format!("fixture-{name}"),
        created_at: "2026-06-18T12:34:56+00:00".to_string(),
        bagging_date: "2026-06-18".to_string(),
        source_kind: "card".to_string(),
        operator: "Op Name".to_string(),
        source_ref: Some("SRC".to_string()),
        artifactclass: "camera-original".to_string(),
        label: Some("fixture".to_string()),
    }
}

#[cfg(unix)]
fn receive_result_payload(result: &sutradhara_receive::ReceiveSourceResult) -> Value {
    json!({
        "bag_info_path": "<LANDING>/<INTAKE-ID>/bag-info.txt",
        "bag_profile": result.bag_profile,
        "file_count": result.file_count,
        "intake_dir": "<LANDING>/<INTAKE-ID>",
        "intake_id": "<INTAKE-ID>",
        "manifest_path": "<LANDING>/<INTAKE-ID>/manifest-sha256.txt",
        "sentinel_path": "<LANDING>/<INTAKE-ID>/intake.json",
        "skipped_count": result.skipped_count,
        "tagmanifest_path": "<LANDING>/<INTAKE-ID>/tagmanifest-sha256.txt",
        "total_bytes": result.total_bytes,
    })
}

#[cfg(unix)]
fn snapshot_bag_files(intake_dir: &Path) -> Value {
    let mut paths = collect_files(intake_dir);
    paths.sort();
    let mut records = Vec::new();
    for path in paths {
        let relpath = path
            .strip_prefix(intake_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if relpath == "receive.log" {
            continue;
        }
        if matches!(
            relpath.as_str(),
            "bagit.txt"
                | "bag-info.txt"
                | "manifest-sha256.txt"
                | "tagmanifest-sha256.txt"
                | "package-index.json"
                | "intake.json"
        ) {
            records.push(json!({
                "kind": "text",
                "path": relpath,
                "text": normalize_intake_id(&fs::read_to_string(&path).unwrap()),
            }));
        } else {
            records.push(json!({
                "kind": "bytes",
                "path": relpath,
                "sha256": sha256_file(&path).unwrap(),
                "size": fs::metadata(&path).unwrap().len(),
            }));
        }
    }
    Value::Array(records)
}

#[cfg(unix)]
fn collect_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(collect_files(&path));
        } else {
            result.push(path);
        }
    }
    result
}

#[cfg(unix)]
fn normalize_intake_id(value: &str) -> String {
    value
        .replace("fixture-mixed-files", "<INTAKE-ID>")
        .replace("fixture-package-with-symlink", "<INTAKE-ID>")
}

fn bag_info_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("Bagging-Date".to_string(), "2026-06-18".to_string()),
        ("Payload-Oxum".to_string(), "5.1".to_string()),
        (
            "Bag-Software-Agent".to_string(),
            "sutradhara-receive/receive-v2".to_string(),
        ),
        (
            "Receive-Package".to_string(),
            "sutradhara-receive/0.0.1".to_string(),
        ),
        ("Intake-Id".to_string(), "bag-001".to_string()),
        ("Operator".to_string(), "op".to_string()),
        ("Source-Kind".to_string(), "card".to_string()),
        ("Source-Ref".to_string(), "A001".to_string()),
        ("Artifactclass".to_string(), "camera-original".to_string()),
        ("Label".to_string(), "shoot".to_string()),
        (
            "Canonicalization-Version".to_string(),
            "receive-bagit-path-v2".to_string(),
        ),
        (
            "Package-Profile-Version".to_string(),
            "package-tar-v1".to_string(),
        ),
        (
            "Package-Profile-Hash".to_string(),
            "fc87e5e8ad47962fa800b2d2e7fac6ae1da148f142319a4c32efca1ed392ef3c".to_string(),
        ),
        ("Skipped-Count".to_string(), "0".to_string()),
    ])
}

fn parse_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
        .collect()
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    output
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!(),
    }
}
