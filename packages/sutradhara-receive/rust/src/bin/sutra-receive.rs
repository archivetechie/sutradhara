//! Standalone Rust edge CLI for the Sutradhara receive contract.
//!
//! This binary is the M3 command surface over the Rust receive-core library. It
//! intentionally starts with the fixture-pinned behavior from `cli_matrix.json`:
//! receive/run normalization, JSON output, fail-safe confirmation timeout,
//! source/`--fake-source` exclusivity, and orphan sweeping.

use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use sutradhara_receive::{
    BAG_PROFILE, ReceiveOptions, ReceiveSourceResult, resume_receive_source, slug_operator,
    sweep_orphans,
};
use time::OffsetDateTime;
use uuid::Uuid;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(args) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(1)
        }
    }
}

fn run(args: Vec<String>) -> Result<u8, String> {
    let normalized = normalize_argv(args);
    match normalized.first().map(String::as_str) {
        Some("run") => run_receive(&normalized[1..]),
        Some("sweep") | Some("sweep-orphans") => run_sweep(&normalized[1..]),
        Some("-h" | "--help") => {
            print_help();
            Ok(0)
        }
        _ => {
            print_help();
            Ok(2)
        }
    }
}

fn normalize_argv(args: Vec<String>) -> Vec<String> {
    if args.is_empty() {
        return vec!["run".to_string()];
    }
    match args[0].as_str() {
        "run" | "sweep" | "sweep-orphans" | "-h" | "--help" => args,
        _ => {
            let mut normalized = Vec::with_capacity(args.len() + 1);
            normalized.push("run".to_string());
            normalized.extend(args);
            normalized
        }
    }
}

fn run_receive(args: &[String]) -> Result<u8, String> {
    let mut source: Option<PathBuf> = None;
    let mut landing: Option<PathBuf> = None;
    let mut source_kind: Option<String> = None;
    let mut operator: Option<String> = None;
    let mut source_ref: Option<String> = None;
    let mut artifactclass = "default".to_string();
    let mut label: Option<String> = None;
    let mut fake_source: Option<PathBuf> = None;
    let mut resume: Option<String> = None;
    let mut confirm_timeout: Option<f64> = None;
    let mut confirm_interval = 1.0_f64;
    let mut as_json = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--landing" => {
                landing = Some(PathBuf::from(require_value(args, &mut index, "--landing")?));
            }
            "--source-kind" => {
                source_kind = Some(require_value(args, &mut index, "--source-kind")?);
            }
            "--operator" => {
                operator = Some(require_value(args, &mut index, "--operator")?);
            }
            "--source-ref" => {
                source_ref = Some(require_value(args, &mut index, "--source-ref")?);
            }
            "--artifactclass" => {
                artifactclass = require_value(args, &mut index, "--artifactclass")?;
            }
            "--label" => {
                label = Some(require_value(args, &mut index, "--label")?);
            }
            "--fake-source" => {
                fake_source = Some(PathBuf::from(require_value(
                    args,
                    &mut index,
                    "--fake-source",
                )?));
            }
            "--resume" => {
                resume = Some(require_value(args, &mut index, "--resume")?);
            }
            "--confirm-timeout" => {
                confirm_timeout = Some(
                    require_value(args, &mut index, "--confirm-timeout")?
                        .parse::<f64>()
                        .map_err(|_| "--confirm-timeout must be a number".to_string())?,
                );
            }
            "--confirm-interval" => {
                confirm_interval = require_value(args, &mut index, "--confirm-interval")?
                    .parse::<f64>()
                    .map_err(|_| "--confirm-interval must be a number".to_string())?;
            }
            "--json" => {
                as_json = true;
            }
            value if value.starts_with('-') => {
                return usage_error(format!("unrecognized argument: {value}"));
            }
            value => {
                if source.is_some() {
                    return usage_error(format!("unexpected argument: {value}"));
                }
                source = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }

    if fake_source.is_some() && source.is_some() {
        return usage_error("pass either SOURCE or --fake-source, not both");
    }
    let selected_source = fake_source.or(source);
    if selected_source.is_none() && resume.is_none() {
        return usage_error("SOURCE is required unless --resume is used");
    };
    let Some(landing) = landing else {
        return usage_error("--landing is required");
    };
    let Some(source_kind) = source_kind else {
        return usage_error("--source-kind is required");
    };

    let now = OffsetDateTime::now_utc();
    let operator = operator.unwrap_or_else(default_operator);
    let options = ReceiveOptions {
        intake_id: mint_intake_id(&operator, now),
        created_at: iso_utc(now),
        bagging_date: bagging_date(now),
        source_kind,
        operator,
        source_ref,
        artifactclass,
        label,
    };
    let result = if let Some(resume) = resume {
        resume_receive_source(&landing, &resume, &options).map_err(|error| error.to_string())?
    } else {
        let selected_source = selected_source.expect("source checked above");
        sutradhara_receive::receive_source(&selected_source, &landing, &options)
            .map_err(|error| error.to_string())?
    };
    let confirmation = confirm_timeout.map(|timeout| {
        wait_for_confirmation(
            &result.intake_dir,
            Duration::from_secs_f64(timeout.max(0.0)),
            Duration::from_secs_f64(confirm_interval.max(0.001)),
        )
    });

    if as_json {
        print_json(&receive_payload(&result, confirmation.as_ref()))?;
    } else {
        print_receive_text(&result, confirmation.as_ref());
    }
    if confirmation
        .as_ref()
        .and_then(|payload| payload.get("release_ok"))
        .and_then(Value::as_bool)
        == Some(false)
    {
        Ok(3)
    } else {
        Ok(0)
    }
}

fn run_sweep(args: &[String]) -> Result<u8, String> {
    let mut landing: Option<PathBuf> = None;
    let mut older_than_hours = 24.0_f64;
    let mut as_json = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--landing" => {
                landing = Some(PathBuf::from(require_value(args, &mut index, "--landing")?));
            }
            "--older-than-hours" => {
                older_than_hours = require_value(args, &mut index, "--older-than-hours")?
                    .parse::<f64>()
                    .map_err(|_| "--older-than-hours must be a number".to_string())?;
            }
            "--json" => as_json = true,
            value => return usage_error(format!("unrecognized argument: {value}")),
        }
        index += 1;
    }
    let Some(landing) = landing else {
        return usage_error("--landing is required");
    };
    let older_than = Duration::from_secs_f64(older_than_hours * 3600.0);
    let result = sweep_orphans(&landing, older_than, SystemTime::now())
        .map_err(|error| error.to_string())?;
    if as_json {
        print_json(&json!({
            "removed": result.removed.iter().map(|path| path_to_string(path)).collect::<Vec<_>>(),
        }))?;
    } else if result.removed.is_empty() {
        println!("(no stale receives)");
    } else {
        for path in result.removed {
            println!("removed {}", path.display());
        }
    }
    Ok(0)
}

fn require_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn usage_error(message: impl Into<String>) -> Result<u8, String> {
    eprintln!("error: {}", message.into());
    Ok(2)
}

fn receive_payload(result: &ReceiveSourceResult, confirmation: Option<&Value>) -> Value {
    let mut payload = json!({
        "bag_info_path": path_to_string(&result.bag_info_path),
        "bag_profile": result.bag_profile,
        "file_count": result.file_count,
        "intake_dir": path_to_string(&result.intake_dir),
        "intake_id": result.intake_id,
        "manifest_path": path_to_string(&result.manifest_path),
        "sentinel_path": path_to_string(&result.sentinel_path),
        "skipped_count": result.skipped_count,
        "tagmanifest_path": path_to_string(&result.tagmanifest_path),
        "total_bytes": result.total_bytes,
    });
    if let Some(confirmation) = confirmation {
        payload["confirmation"] = confirmation.clone();
    }
    payload
}

fn wait_for_confirmation(intake_dir: &Path, timeout: Duration, interval: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(payload) =
            marker_payload(intake_dir, "intake.discrepancy.json", "discrepancy", false)
        {
            return payload;
        }
        if let Some(payload) =
            marker_payload(intake_dir, "intake.quarantined.json", "quarantined", false)
        {
            return payload;
        }
        let verified = intake_dir.join("intake.verified.json");
        if verified.exists() {
            let detail = read_json_or_null(&verified);
            return json!({
                "detail": detail,
                "marker_path": path_to_string(&verified),
                "release_ok": !detail.is_null(),
                "status": if detail.is_null() { "pending" } else { "verified" },
            });
        }
        if Instant::now() >= deadline {
            return timeout_payload();
        }
        thread::sleep(interval);
    }
}

fn marker_payload(
    intake_dir: &Path,
    filename: &str,
    status: &str,
    release_ok: bool,
) -> Option<Value> {
    let marker = intake_dir.join(filename);
    if !marker.exists() {
        return None;
    }
    Some(json!({
        "detail": read_json_or_null(&marker),
        "marker_path": path_to_string(&marker),
        "release_ok": release_ok,
        "status": status,
    }))
}

fn read_json_or_null(path: &Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null)
}

fn timeout_payload() -> Value {
    json!({
        "detail": null,
        "marker_path": null,
        "release_ok": false,
        "status": "timeout",
    })
}

fn print_receive_text(result: &ReceiveSourceResult, confirmation: Option<&Value>) {
    println!(
        "{}: received {} file(s), {} byte(s), skipped={}",
        result.intake_id, result.file_count, result.total_bytes, result.skipped_count
    );
    println!("sentinel: {}", result.sentinel_path.display());
    println!("bag profile: {}", BAG_PROFILE);
    println!("manifest: {}", result.manifest_path.display());
    println!("tagmanifest: {}", result.tagmanifest_path.display());
    if let Some(confirmation) = confirmation {
        let status = confirmation["status"].as_str().unwrap_or("timeout");
        if status == "verified" {
            println!("server confirmation: verified; source release allowed");
        } else {
            eprintln!("server confirmation: {status}; do not release source");
        }
    }
}

fn print_json(value: &Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn mint_intake_id(operator: &str, now: OffsetDateTime) -> String {
    format!(
        "{:04}{:02}{:02}-{}-{}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        slug_operator(operator),
        Uuid::new_v4().simple()
    )
}

fn iso_utc(now: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+00:00",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

fn bagging_date(now: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    )
}

fn default_operator() -> String {
    env::var("USER").unwrap_or_else(|_| "operator".to_string())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn print_help() {
    eprintln!("usage: sutra-receive [run] [SOURCE] --landing LANDING --source-kind KIND [--json]");
    eprintln!("       sutra-receive sweep --landing LANDING [--json]");
}
