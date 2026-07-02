//! Rust receive-core primitives for the Sutradhara receive contract.
//!
//! The current migration slices cover the fixture-pinned stratum-1 primitives
//! plus the M3 write-side core: reversible member-name escaping, receive-path
//! canonicalization, BagIt tag text builders, SHA-256 hashing, package tar
//! normalization, deterministic receive-bag writing, the edge CLI, read-side
//! validation, and the first PyO3 binding layer for the in-place wheel flip.

#[cfg(feature = "python")]
mod python;

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};
use unicode_normalization::UnicodeNormalization;

pub const RECEIVE_VERSION: &str = "receive-v2";
pub const RECEIVE_PACKAGE_NAME: &str = "sutradhara-receive";
pub const RECEIVE_PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RECEIVE_PACKAGE: &str = concat!("sutradhara-receive/", env!("CARGO_PKG_VERSION"));
pub const CANONICALIZATION_VERSION: &str = "receive-bagit-path-v2";
pub const PACKAGE_PROFILE_VERSION: &str = "package-tar-v1";
pub const PACKAGE_PROFILE_HASH: &str =
    "fc87e5e8ad47962fa800b2d2e7fac6ae1da148f142319a4c32efca1ed392ef3c";
pub const PACKAGE_GLOBS: &[&str] = &["*.fcpbundle", "*.photoslibrary", "*.imovielibrary", "*.app"];
pub const BAG_PROFILE: &str = "bagit-1.0";
pub const BAGIT_TEXT: &str = "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
pub const DATA_DIR_NAME: &str = "data";
pub const COPY_BUFFER_BYTES: usize = 1024 * 1024;
pub const PACKAGE_FILE_MODE: u32 = 0o644;
pub const PACKAGE_DIR_MODE: u32 = 0o755;
pub const PACKAGE_SYMLINK_MODE: u32 = 0o777;
pub const PACKAGE_MTIME: u64 = 0;
pub const TAR_BLOCK_BYTES: u64 = 512;
pub const TAR_RECORD_BYTES: u64 = 10240;

const BAG_INFO_ORDER: &[&str] = &[
    "Bagging-Date",
    "Payload-Oxum",
    "Bag-Software-Agent",
    "Receive-Package",
    "Intake-Id",
    "Operator",
    "Source-Kind",
    "Source-Ref",
    "Artifactclass",
    "Label",
    "Canonicalization-Version",
    "Package-Profile-Version",
    "Package-Profile-Hash",
    "Skipped-Count",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveError {
    message: String,
}

impl ReceiveError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ReceiveError {}

impl From<io::Error> for ReceiveError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<serde_json::Error> for ReceiveError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

pub type ReceiveResult<T> = Result<T, ReceiveError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveOptions {
    pub intake_id: String,
    pub created_at: String,
    pub bagging_date: String,
    pub source_kind: String,
    pub operator: String,
    pub source_ref: Option<String>,
    pub artifactclass: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiveSourceResult {
    pub intake_id: String,
    pub intake_dir: PathBuf,
    pub manifest_path: PathBuf,
    pub bag_info_path: PathBuf,
    pub tagmanifest_path: PathBuf,
    pub sentinel_path: PathBuf,
    pub file_count: usize,
    pub total_bytes: u64,
    pub skipped_count: usize,
    pub bag_profile: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrphanSweepResult {
    pub removed: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BagWriteResult {
    pub manifest_path: PathBuf,
    pub bag_info_path: PathBuf,
    pub tagmanifest_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayloadPlan {
    pub units: Vec<PayloadUnit>,
    pub rejected: Vec<RejectedEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayloadUnit {
    pub source_path: PathBuf,
    pub relpath: String,
    pub entry_type: String,
    pub logical_relpath: Option<String>,
    pub hint_size: u64,
    pub plan_size: u64,
    pub mtime_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RejectedEntry {
    pub relpath: String,
    pub source_path: PathBuf,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HashReceipt {
    pub relpath: String,
    pub sha256_hex: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestDigestMismatch {
    pub path: String,
    pub expected: String,
    pub actual: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TagManifestMismatch {
    pub path: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BagValidationResult {
    #[serde(skip_serializing)]
    pub bag_root: PathBuf,
    #[serde(skip_serializing)]
    pub data_root: PathBuf,
    pub metadata: BTreeMap<String, String>,
    pub manifest: BTreeMap<String, String>,
    pub actual: BTreeMap<String, String>,
    pub actual_records: Vec<HashReceipt>,
    pub missing: Vec<String>,
    pub extra: Vec<String>,
    pub mismatched: Vec<ManifestDigestMismatch>,
    pub tag_mismatched: Vec<TagManifestMismatch>,
    pub errors: Vec<String>,
}

impl BagValidationResult {
    pub fn complete(&self) -> bool {
        self.missing.is_empty()
            && self.extra.is_empty()
            && !self
                .errors
                .iter()
                .any(|error| error.starts_with("complete:"))
    }

    pub fn valid(&self) -> bool {
        self.complete()
            && self.mismatched.is_empty()
            && self.tag_mismatched.is_empty()
            && self.errors.is_empty()
    }

    pub fn details(&self) -> Value {
        let mut payload = serde_json::Map::new();
        if !self.missing.is_empty() {
            payload.insert("missing".to_string(), json!(self.missing));
        }
        if !self.extra.is_empty() {
            payload.insert("extra".to_string(), json!(self.extra));
        }
        if !self.mismatched.is_empty() {
            payload.insert("mismatched".to_string(), json!(self.mismatched));
        }
        if !self.tag_mismatched.is_empty() {
            payload.insert("tag_mismatched".to_string(), json!(self.tag_mismatched));
        }
        if !self.errors.is_empty() {
            payload.insert("errors".to_string(), json!(self.errors));
        }
        Value::Object(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceEntry {
    source_path: PathBuf,
    relpath: String,
    entry_type: SourceEntryType,
    logical_relpath: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceEntryType {
    File,
    Package,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileReceipt {
    relpath: String,
    destination_path: PathBuf,
    sha256_hex: String,
    size_bytes: u64,
    logical_relpath: Option<String>,
    stored_relpath: Option<String>,
    package_members: Vec<PackageMemberRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceStatSnapshot {
    size: u64,
    modified: std::time::SystemTime,
    inode: Option<u64>,
    device: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PackageTarResult {
    pub digest: String,
    pub size_bytes: u64,
    pub members: Vec<PackageMemberRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PackageMemberRecord {
    pub member: String,
    #[serde(rename = "type")]
    pub member_type: String,
    pub length: u64,
    pub sha256: Option<String>,
    pub data_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linkname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackageMember {
    source_path: PathBuf,
    member_name: String,
    member_type: PackageMemberType,
    mode: u32,
    size: u64,
    linkname: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackageMemberType {
    File,
    Directory,
    Symlink,
}

pub fn escape_member_name(raw: &[u8]) -> String {
    let mut output = String::new();
    let mut index = 0;
    while index < raw.len() {
        let byte = raw[index];
        if byte == b'\\' {
            output.push_str(r"\\");
            index += 1;
            continue;
        }
        if must_escape_byte(byte) {
            push_hex_escape(&mut output, byte);
            index += 1;
            continue;
        }
        let sequence_len = utf8_sequence_len(byte);
        if sequence_len == 0 {
            push_hex_escape(&mut output, byte);
            index += 1;
            continue;
        }
        if sequence_len == 1 {
            output.push(byte as char);
            index += 1;
            continue;
        }
        if index + sequence_len <= raw.len() {
            let candidate = &raw[index..index + sequence_len];
            if let Ok(text) = std::str::from_utf8(candidate) {
                output.push_str(text);
                index += sequence_len;
                continue;
            }
        }
        push_hex_escape(&mut output, byte);
        index += 1;
    }
    output
}

pub fn unescape_member_name(text: &str) -> ReceiveResult<Vec<u8>> {
    let chars: Vec<char> = text.chars().collect();
    let mut output = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            if index + 1 >= chars.len() {
                return Err(ReceiveError::new("member name ends with a bare backslash"));
            }
            let marker = chars[index + 1];
            if marker == '\\' {
                output.push(b'\\');
                index += 2;
                continue;
            }
            if marker == 'x' && index + 3 < chars.len() {
                let high = chars[index + 2];
                let low = chars[index + 3];
                if let (Some(high), Some(low)) = (lower_hex_value(high), lower_hex_value(low)) {
                    output.push((high << 4) | low);
                    index += 4;
                    continue;
                }
            }
            return Err(ReceiveError::new(format!(
                "invalid escape at character {index}"
            )));
        }
        if current < '\u{20}' || current == '\u{7f}' {
            return Err(ReceiveError::new(
                "member name contains an unescaped control character",
            ));
        }
        let mut buffer = [0_u8; 4];
        output.extend_from_slice(current.encode_utf8(&mut buffer).as_bytes());
        index += 1;
    }
    Ok(output)
}

pub fn canonicalize_manifest_path(raw: &str) -> ReceiveResult<String> {
    let mut value = raw;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    value = value.trim_start_matches('/');
    if let Some(stripped) = value.strip_prefix("data/") {
        value = stripped;
    }
    canonicalize_manifest_components(value.split('/'))
}

pub fn canonicalize_raw_path_components<I, C>(components: I) -> ReceiveResult<String>
where
    I: IntoIterator<Item = C>,
    C: AsRef<[u8]>,
{
    let mut output = Vec::new();
    for component in components {
        let raw = component.as_ref();
        if raw.is_empty() || raw == b"." {
            continue;
        }
        if raw == b".." {
            return Err(ReceiveError::new("relative paths must not contain '..'"));
        }
        output.push(canonical_component_from_bytes(raw));
    }
    join_canonical_parts(output)
}

pub fn canonicalize_filesystem_path(path: &Path, root: &Path) -> ReceiveResult<String> {
    let relative = path.strip_prefix(root).map_err(|_| {
        ReceiveError::new(format!(
            "{} is not under source root {}",
            path.display(),
            root.display()
        ))
    })?;
    let mut output = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                output.push(canonical_component_from_bytes(&os_str_bytes(value)));
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(ReceiveError::new("relative paths must not contain '..'"));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ReceiveError::new(format!(
                    "{} is not under source root {}",
                    path.display(),
                    root.display()
                )));
            }
        }
    }
    join_canonical_parts(output)
}

pub fn bagit_manifest_text(entries: &BTreeMap<String, String>) -> ReceiveResult<String> {
    let mut lines = Vec::new();
    for (relpath, digest) in entries {
        let digest = digest.to_ascii_lowercase();
        if !is_sha256_hex(&digest) {
            return Err(ReceiveError::new(format!(
                "invalid sha256 for {relpath:?}: {digest:?}"
            )));
        }
        let canonical = canonicalize_manifest_path(relpath)?;
        lines.push(format!(
            "{digest}  {}",
            encode_bagit_path(&format!("{DATA_DIR_NAME}/{canonical}"))
        ));
    }
    Ok(join_lines(lines))
}

pub fn bag_info_text(metadata: &BTreeMap<String, String>) -> String {
    let mut lines = Vec::new();
    for key in BAG_INFO_ORDER {
        if let Some(value) = metadata.get(*key) {
            lines.push(format!("{key}: {}", bag_info_value(value)));
        }
    }
    for (key, value) in metadata {
        if !BAG_INFO_ORDER.contains(&key.as_str()) {
            lines.push(format!("{key}: {}", bag_info_value(value)));
        }
    }
    format!("{}\n", lines.join("\n"))
}

pub fn tagmanifest_text(bag_root: &Path, tag_files: &[String]) -> ReceiveResult<String> {
    let mut relpaths = tag_files.to_vec();
    relpaths.sort();
    let mut lines = Vec::new();
    for relpath in relpaths {
        let path = bag_root.join(&relpath);
        lines.push(format!(
            "{}  {}",
            sha256_file(&path)?,
            encode_bagit_path(&relpath)
        ));
    }
    Ok(join_lines(lines))
}

pub fn sha256_file(path: &Path) -> ReceiveResult<String> {
    let mut digest = Sha256::new();
    let mut handle = File::open(path)?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = handle.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_lower(&digest.finalize()))
}

pub fn build_package_tar(
    package_root: &Path,
    destination: &Path,
    logical_relpath: &str,
) -> ReceiveResult<PackageTarResult> {
    let root_metadata = fs::symlink_metadata(package_root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(ReceiveError::new(format!(
            "package boundary is not a directory: {}",
            package_root.display()
        )));
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    let members = package_members(package_root, logical_relpath)?;
    let handle = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut writer = TarHashingWriter::new(handle);
    let mut records = BTreeMap::new();

    for member in members {
        let record = match member.member_type {
            PackageMemberType::File => {
                let header = tar_header(
                    &member.member_name,
                    member.mode,
                    member.size,
                    b'0',
                    "",
                    false,
                )?;
                writer.write_all(&header)?;
                let data_offset = writer.position();
                let file_digest = copy_file_to_tar(&member.source_path, &mut writer)?;
                pad_tar_data(&mut writer, member.size)?;
                PackageMemberRecord {
                    member: member.member_name,
                    member_type: "file".to_string(),
                    length: member.size,
                    sha256: Some(file_digest),
                    data_offset: Some(data_offset),
                    linkname: None,
                }
            }
            PackageMemberType::Directory => {
                let header = tar_header(&member.member_name, member.mode, 0, b'5', "", true)?;
                writer.write_all(&header)?;
                PackageMemberRecord {
                    member: member.member_name,
                    member_type: "directory".to_string(),
                    length: 0,
                    sha256: None,
                    data_offset: None,
                    linkname: None,
                }
            }
            PackageMemberType::Symlink => {
                let linkname = member.linkname.unwrap_or_default();
                let header =
                    tar_header(&member.member_name, member.mode, 0, b'2', &linkname, false)?;
                writer.write_all(&header)?;
                PackageMemberRecord {
                    member: member.member_name,
                    member_type: "symlink".to_string(),
                    length: 0,
                    sha256: None,
                    data_offset: None,
                    linkname: Some(linkname),
                }
            }
        };
        records.insert(record.member.clone(), record);
    }

    writer.write_all(&[0_u8; TAR_BLOCK_BYTES as usize])?;
    writer.write_all(&[0_u8; TAR_BLOCK_BYTES as usize])?;
    while writer.position() % TAR_RECORD_BYTES != 0 {
        writer.write_all(&[0_u8; TAR_BLOCK_BYTES as usize])?;
    }
    writer.flush()?;
    let size_bytes = writer.position();
    let digest = writer.hexdigest();
    Ok(PackageTarResult {
        digest,
        size_bytes,
        members: records.into_values().collect(),
    })
}

pub fn receive_source(
    source: &Path,
    landing: &Path,
    options: &ReceiveOptions,
) -> ReceiveResult<ReceiveSourceResult> {
    let source_root = fs::canonicalize(source)?;
    validate_source_root(&source_root)?;
    fs::create_dir_all(landing)?;
    let landing_root = fs::canonicalize(landing)?;
    validate_landing_relationship(&source_root, &landing_root)?;
    let intake_dir = landing_root.join(&options.intake_id);
    fs::create_dir(&intake_dir)?;
    let receiving_path = intake_dir.join(".receiving.json");
    write_json_file(
        &receiving_path,
        &json!({
            "artifactclass": options.artifactclass,
            "canonicalization_version": CANONICALIZATION_VERSION,
            "intake_id": options.intake_id,
            "label": options.label,
            "landing": landing_root,
            "operator": options.operator,
            "receive_version": RECEIVE_VERSION,
            "source": source_root,
            "source_kind": options.source_kind,
            "source_ref": options.source_ref,
            "started_at": options.created_at,
        }),
    )?;
    finish_receive(
        &source_root,
        &landing_root,
        &intake_dir,
        &receiving_path,
        options,
        false,
    )
}

pub fn resume_receive_source(
    landing: &Path,
    intake_id: &str,
    options: &ReceiveOptions,
) -> ReceiveResult<ReceiveSourceResult> {
    fs::create_dir_all(landing)?;
    let landing_root = fs::canonicalize(landing)?;
    let intake_dir = landing_root.join(intake_id);
    if intake_dir.join("intake.json").exists() {
        return Err(ReceiveError::new(format!(
            "intake {intake_id:?} is already complete"
        )));
    }
    let receiving_path = intake_dir.join(".receiving.json");
    let receiving: Value = serde_json::from_str(&fs::read_to_string(&receiving_path)?)?;
    let source_text = required_json_string(&receiving, "source")?;
    let source_root = fs::canonicalize(PathBuf::from(source_text))?;
    validate_source_root(&source_root)?;
    validate_landing_relationship(&source_root, &landing_root)?;
    let resume_options = ReceiveOptions {
        intake_id: intake_id.to_string(),
        created_at: options.created_at.clone(),
        bagging_date: options.bagging_date.clone(),
        source_kind: optional_json_string(&receiving, "source_kind")
            .unwrap_or_else(|| options.source_kind.clone()),
        operator: optional_json_string(&receiving, "operator")
            .unwrap_or_else(|| options.operator.clone()),
        source_ref: optional_json_string(&receiving, "source_ref")
            .or_else(|| options.source_ref.clone()),
        artifactclass: optional_json_string(&receiving, "artifactclass")
            .unwrap_or_else(|| options.artifactclass.clone()),
        label: optional_json_string(&receiving, "label").or_else(|| options.label.clone()),
    };
    finish_receive(
        &source_root,
        &landing_root,
        &intake_dir,
        &receiving_path,
        &resume_options,
        true,
    )
}

fn finish_receive(
    source_root: &Path,
    _landing_root: &Path,
    intake_dir: &Path,
    receiving_path: &Path,
    options: &ReceiveOptions,
    resume: bool,
) -> ReceiveResult<ReceiveSourceResult> {
    let (entries, rejected) = scan_source(source_root)?;
    check_collisions(&entries)?;
    let data_root = intake_dir.join(DATA_DIR_NAME);
    fs::create_dir_all(&data_root)?;
    if resume {
        let desired_relpaths = entries
            .iter()
            .map(|entry| entry.relpath.clone())
            .collect::<std::collections::BTreeSet<_>>();
        prune_stale_payload_files(&data_root, &desired_relpaths)?;
    }

    let mut receipts = Vec::new();
    for entry in &entries {
        receipts.push(copy_or_package_entry(entry, &data_root)?);
    }
    verify_destination_files(&receipts)?;

    let manifest_entries: BTreeMap<String, String> = receipts
        .iter()
        .map(|receipt| (receipt.relpath.clone(), receipt.sha256_hex.clone()))
        .collect();
    let package_index = package_index_payload(&receipts);
    let mut extra_tag_files = Vec::new();
    if let Some(package_index) = package_index {
        write_json_file(&intake_dir.join("package-index.json"), &package_index)?;
        extra_tag_files.push("package-index.json".to_string());
    } else {
        match fs::remove_file(intake_dir.join("package-index.json")) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    let total_bytes = receipts.iter().map(|receipt| receipt.size_bytes).sum();
    let metadata = bag_info_metadata(options, receipts.len(), total_bytes, rejected.len());
    write_bagit_files(intake_dir, &manifest_entries, &metadata, &extra_tag_files)?;
    write_json_file(
        &intake_dir.join("intake.json"),
        &json!({
            "bag_profile": BAG_PROFILE,
            "created_at": options.created_at,
            "intake_id": options.intake_id,
            "status": "complete",
        }),
    )?;
    match fs::remove_file(receiving_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    Ok(ReceiveSourceResult {
        intake_id: options.intake_id.clone(),
        intake_dir: intake_dir.to_path_buf(),
        manifest_path: intake_dir.join("manifest-sha256.txt"),
        bag_info_path: intake_dir.join("bag-info.txt"),
        tagmanifest_path: intake_dir.join("tagmanifest-sha256.txt"),
        sentinel_path: intake_dir.join("intake.json"),
        file_count: receipts.len(),
        total_bytes,
        skipped_count: rejected.len(),
        bag_profile: BAG_PROFILE.to_string(),
    })
}

pub fn plan_payload_units(source: &Path) -> ReceiveResult<PayloadPlan> {
    let source_root = fs::canonicalize(source)?;
    validate_source_root(&source_root)?;
    let (entries, rejected) = scan_source(&source_root)?;
    check_collisions(&entries)?;
    let units = entries
        .iter()
        .map(payload_unit_from_entry)
        .collect::<ReceiveResult<Vec<_>>>()?;
    Ok(PayloadPlan { units, rejected })
}

pub fn write_bagit_files(
    bag_root: &Path,
    entries: &BTreeMap<String, String>,
    metadata: &BTreeMap<String, String>,
    extra_tag_files: &[String],
) -> ReceiveResult<BagWriteResult> {
    let mut noop = |_temp_path: &Path, _final_path: &Path| Ok(());
    write_bagit_files_with_observer(bag_root, entries, metadata, extra_tag_files, &mut noop)
}

pub fn write_bagit_files_with_observer<F>(
    bag_root: &Path,
    entries: &BTreeMap<String, String>,
    metadata: &BTreeMap<String, String>,
    extra_tag_files: &[String],
    observer: &mut F,
) -> ReceiveResult<BagWriteResult>
where
    F: FnMut(&Path, &Path) -> ReceiveResult<()>,
{
    let manifest_path = bag_root.join("manifest-sha256.txt");
    let bag_info_path = bag_root.join("bag-info.txt");
    let bagit_path = bag_root.join("bagit.txt");
    let tagmanifest_path = bag_root.join("tagmanifest-sha256.txt");

    atomic_write_text(&bagit_path, BAGIT_TEXT, observer)?;
    atomic_write_text(&bag_info_path, &bag_info_text(metadata), observer)?;
    atomic_write_text(&manifest_path, &bagit_manifest_text(entries)?, observer)?;

    let mut tag_files = vec![
        "bagit.txt".to_string(),
        "bag-info.txt".to_string(),
        "manifest-sha256.txt".to_string(),
    ];
    tag_files.extend_from_slice(extra_tag_files);
    atomic_write_text(
        &tagmanifest_path,
        &tagmanifest_text(bag_root, &tag_files)?,
        observer,
    )?;

    Ok(BagWriteResult {
        manifest_path,
        bag_info_path,
        tagmanifest_path,
    })
}

pub fn sweep_orphans(
    landing: &Path,
    older_than: std::time::Duration,
    now: std::time::SystemTime,
) -> ReceiveResult<OrphanSweepResult> {
    let mut removed = Vec::new();
    if !landing.exists() {
        return Ok(OrphanSweepResult { removed });
    }
    let mut children = Vec::new();
    for entry in fs::read_dir(landing)? {
        let path = entry?.path();
        if path.is_dir() {
            children.push(path);
        }
    }
    children.sort();
    for child in children {
        let receiving = child.join(".receiving.json");
        if !receiving.exists() || child.join("intake.json").exists() {
            continue;
        }
        let modified = fs::metadata(&receiving)?.modified()?;
        let age = now
            .duration_since(modified)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0));
        if age >= older_than {
            fs::remove_dir_all(&child)?;
            removed.push(child);
        }
    }
    Ok(OrphanSweepResult { removed })
}

pub fn hash_payload_tree(payload_root: &Path) -> ReceiveResult<Vec<HashReceipt>> {
    hash_payload_tree_with_policy(payload_root, false)
}

pub fn hash_payload_tree_with_policy(
    payload_root: &Path,
    reject_native_packages: bool,
) -> ReceiveResult<Vec<HashReceipt>> {
    let mut receipts = Vec::new();
    collect_payload_hashes(
        payload_root,
        payload_root,
        reject_native_packages,
        &mut receipts,
    )?;
    receipts.sort_by(|left, right| left.relpath.cmp(&right.relpath));
    Ok(receipts)
}

pub fn read_bag_info(path: &Path) -> ReceiveResult<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let mut current_key: Option<String> = None;
    for (line_index, raw_line) in fs::read_to_string(path)?.lines().enumerate() {
        let line_number = line_index + 1;
        if raw_line.is_empty() {
            continue;
        }
        if raw_line.starts_with(char::is_whitespace)
            && let Some(current_key) = current_key.as_ref()
        {
            let prior = result.get(current_key).cloned().unwrap_or_default();
            result.insert(current_key.clone(), format!("{prior}\n{}", raw_line.trim()));
            continue;
        }
        let Some((key, value)) = raw_line.split_once(':') else {
            return Err(ReceiveError::new(format!(
                "invalid bag-info line {line_number}: {}",
                python_repr(raw_line)
            )));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(ReceiveError::new(format!(
                "invalid empty bag-info label at line {line_number}"
            )));
        }
        result.insert(key.to_string(), value.trim().to_string());
        current_key = Some(key.to_string());
    }
    Ok(result)
}

pub fn read_manifest_sha256(path: &Path) -> ReceiveResult<BTreeMap<String, String>> {
    read_checksum_manifest(path, true)
}

pub fn read_package_index(path: &Path) -> ReceiveResult<Value> {
    let payload: Value = serde_json::from_str(&fs::read_to_string(path)?)?;
    if payload["profile"].as_str() != Some(PACKAGE_PROFILE_VERSION) {
        return Err(ReceiveError::new(format!(
            "Package-Profile-Version mismatch: expected {PACKAGE_PROFILE_VERSION}, actual {}",
            python_repr_value(&payload["profile"])
        )));
    }
    if payload["profile_hash"].as_str() != Some(PACKAGE_PROFILE_HASH) {
        return Err(ReceiveError::new(format!(
            "Package-Profile-Hash mismatch: expected {PACKAGE_PROFILE_HASH}, actual {}",
            python_repr_value(&payload["profile_hash"])
        )));
    }
    let Some(packages) = payload["packages"].as_array() else {
        return Err(ReceiveError::new(
            "package-index.json packages must be a list",
        ));
    };
    for package in packages {
        let Some(package) = package.as_object() else {
            return Err(ReceiveError::new(
                "package-index.json package entries must be objects",
            ));
        };
        for key in [
            "logical_member_path",
            "stored_member_path",
            "sha256",
            "members",
        ] {
            if !package.contains_key(key) {
                return Err(ReceiveError::new(format!(
                    "package-index.json package missing {key}"
                )));
            }
        }
        let Some(logical_member_path) = package["logical_member_path"].as_str() else {
            return Err(ReceiveError::new(
                "package-index.json package logical_member_path must be a string",
            ));
        };
        let Some(stored_member_path) = package["stored_member_path"].as_str() else {
            return Err(ReceiveError::new(
                "package-index.json package stored_member_path must be a string",
            ));
        };
        canonicalize_manifest_path(logical_member_path)?;
        canonicalize_manifest_path(stored_member_path)?;
        if !package["sha256"].as_str().is_some_and(is_sha256_hex) {
            return Err(ReceiveError::new(
                "package-index.json package has invalid sha256",
            ));
        }
        if !package["members"].is_array() {
            return Err(ReceiveError::new(
                "package-index.json package members must be a list",
            ));
        }
    }
    Ok(payload)
}

pub fn manifest_mismatch(
    actual: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
) -> ReceiveResult<Value> {
    let mut actual_canonical = BTreeMap::new();
    for (path, digest) in actual {
        actual_canonical.insert(
            canonicalize_manifest_path(path)?,
            digest.to_ascii_lowercase(),
        );
    }
    let mut expected_canonical = BTreeMap::new();
    for (path, digest) in expected {
        expected_canonical.insert(
            canonicalize_manifest_path(path)?,
            digest.to_ascii_lowercase(),
        );
    }
    let missing: Vec<String> = expected_canonical
        .keys()
        .filter(|path| !actual_canonical.contains_key(*path))
        .cloned()
        .collect();
    let extra: Vec<String> = actual_canonical
        .keys()
        .filter(|path| !expected_canonical.contains_key(*path))
        .cloned()
        .collect();
    let mut mismatched = Vec::new();
    for (path, actual_digest) in &actual_canonical {
        if let Some(expected_digest) = expected_canonical.get(path)
            && actual_digest != expected_digest
        {
            mismatched.push(ManifestDigestMismatch {
                path: path.clone(),
                expected: expected_digest.clone(),
                actual: actual_digest.clone(),
            });
        }
    }
    if missing.is_empty()
        && extra.is_empty()
        && mismatched.is_empty()
        && !expected_canonical.is_empty()
    {
        return Ok(json!({}));
    }
    if expected_canonical.is_empty() {
        return Ok(json!({
            "reason": "manifest-has-no-sha256",
            "missing": [],
            "extra": extra,
            "mismatched": [],
        }));
    }
    Ok(json!({
        "missing": missing,
        "extra": extra,
        "mismatched": mismatched,
    }))
}

pub fn validate_bag(bag_root: &Path) -> BagValidationResult {
    let data_root = bag_root.join(DATA_DIR_NAME);
    let mut errors = Vec::new();
    let mut metadata = BTreeMap::new();
    let mut manifest = BTreeMap::new();
    let mut actual_records = Vec::new();

    if !data_root.is_dir() {
        errors.push(format!("complete: missing {DATA_DIR_NAME}/ directory"));
    } else {
        match hash_payload_tree_with_policy(&data_root, true) {
            Ok(records) => actual_records = records,
            Err(error) => errors.push(format!("complete: cannot hash {DATA_DIR_NAME}/: {error}")),
        }
    }

    match read_manifest_sha256(&bag_root.join("manifest-sha256.txt")) {
        Ok(read_manifest) => manifest = read_manifest,
        Err(error) => errors.push(format!(
            "complete: cannot read manifest-sha256.txt: {error}"
        )),
    }
    match read_bag_info(&bag_root.join("bag-info.txt")) {
        Ok(read_metadata) => metadata = read_metadata,
        Err(error) => errors.push(format!("cannot read bag-info.txt: {error}")),
    }
    let package_index_path = bag_root.join("package-index.json");
    if package_index_path.exists()
        && let Err(error) = read_package_index(&package_index_path)
    {
        errors.push(format!("cannot read package-index.json: {error}"));
    }

    let actual: BTreeMap<String, String> = actual_records
        .iter()
        .map(|record| (record.relpath.clone(), record.sha256_hex.clone()))
        .collect();
    let mismatch = manifest_mismatch(&actual, &manifest).unwrap_or_else(|error| {
        errors.push(error.to_string());
        json!({"missing": [], "extra": [], "mismatched": []})
    });
    let missing = json_string_array(&mismatch["missing"]);
    let extra = json_string_array(&mismatch["extra"]);
    let mismatched = json_manifest_mismatches(&mismatch["mismatched"]);

    if !metadata.is_empty() {
        let expected_oxum = format!(
            "{}.{}",
            actual_records
                .iter()
                .map(|record| record.size_bytes)
                .sum::<u64>(),
            actual_records.len()
        );
        if metadata.get("Payload-Oxum") != Some(&expected_oxum) {
            errors.push(format!(
                "Payload-Oxum mismatch: expected {expected_oxum}, actual {}",
                python_repr(
                    metadata
                        .get("Payload-Oxum")
                        .map(String::as_str)
                        .unwrap_or("")
                )
            ));
        }
        if let Some(error) = receive_package_error(&metadata) {
            errors.push(error);
        }
        if metadata.get("Canonicalization-Version").map(String::as_str)
            != Some(CANONICALIZATION_VERSION)
        {
            errors.push(format!(
                "Canonicalization-Version mismatch: expected {CANONICALIZATION_VERSION}, actual {}",
                python_repr(
                    metadata
                        .get("Canonicalization-Version")
                        .map(String::as_str)
                        .unwrap_or("")
                )
            ));
        }
        let package_profile = metadata.get("Package-Profile-Version").map(String::as_str);
        if package_index_path.exists() {
            if package_profile != Some(PACKAGE_PROFILE_VERSION) {
                errors.push(format!(
                    "Package-Profile-Version mismatch: expected {PACKAGE_PROFILE_VERSION}, actual {}",
                    python_repr(package_profile.unwrap_or(""))
                ));
            }
        } else if !matches!(package_profile, None | Some(PACKAGE_PROFILE_VERSION)) {
            errors.push(format!(
                "Package-Profile-Version mismatch: expected {PACKAGE_PROFILE_VERSION}, actual {}",
                python_repr(package_profile.unwrap_or(""))
            ));
        }
        let package_hash = metadata.get("Package-Profile-Hash").map(String::as_str);
        if package_index_path.exists() {
            if package_hash != Some(PACKAGE_PROFILE_HASH) {
                errors.push(format!(
                    "Package-Profile-Hash mismatch: expected {PACKAGE_PROFILE_HASH}, actual {}",
                    python_repr(package_hash.unwrap_or(""))
                ));
            }
        } else if !matches!(package_hash, None | Some(PACKAGE_PROFILE_HASH)) {
            errors.push(format!(
                "Package-Profile-Hash mismatch: expected {PACKAGE_PROFILE_HASH}, actual {}",
                python_repr(package_hash.unwrap_or(""))
            ));
        }
    }

    let required_tags = if package_index_path.exists() {
        vec!["package-index.json".to_string()]
    } else {
        Vec::new()
    };
    let tag_mismatched = verify_tagmanifest(bag_root, &required_tags);

    BagValidationResult {
        bag_root: bag_root.to_path_buf(),
        data_root,
        metadata,
        manifest,
        actual,
        actual_records,
        missing,
        extra,
        mismatched,
        tag_mismatched,
        errors,
    }
}

pub fn slug_operator(operator: &str) -> String {
    let ascii: String = operator.nfkd().filter(|ch| ch.is_ascii()).collect();
    let mut slug = String::new();
    let mut last_was_dash = false;
    for byte in ascii.bytes() {
        if byte.is_ascii_alphanumeric() {
            slug.push((byte as char).to_ascii_lowercase());
            last_was_dash = false;
        } else if !slug.is_empty() && !last_was_dash {
            slug.push('-');
            last_was_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "operator".to_string()
    } else {
        slug
    }
}

pub fn safe_payload_path(payload_root: &Path, relpath: &str) -> ReceiveResult<PathBuf> {
    let parts: Vec<&str> = relpath.split('/').collect();
    if relpath.starts_with('/')
        || parts
            .iter()
            .any(|part| part.is_empty() || matches!(*part, "." | ".."))
    {
        return Err(ReceiveError::new(format!(
            "unsafe payload relpath: {relpath:?}"
        )));
    }
    let root = resolve_root(payload_root)?;
    let mut current = root.clone();
    for part in &parts[..parts.len() - 1] {
        current.push(part);
        if let Ok(metadata) = fs::symlink_metadata(&current) {
            if metadata.file_type().is_symlink() {
                return Err(ReceiveError::new(format!(
                    "payload directory is a symlink: {}",
                    current.display()
                )));
            }
            if !metadata.is_dir() {
                return Err(ReceiveError::new(format!(
                    "payload path component is not a directory: {}",
                    current.display()
                )));
            }
        }
    }
    let mut final_path = root;
    for part in parts {
        final_path.push(part);
    }
    if let Ok(metadata) = fs::symlink_metadata(&final_path)
        && metadata.file_type().is_symlink()
    {
        return Err(ReceiveError::new(format!(
            "payload destination is a symlink: {}",
            final_path.display()
        )));
    }
    Ok(final_path)
}

fn canonicalize_manifest_components<'a, I>(components: I) -> ReceiveResult<String>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut output = Vec::new();
    for component in components {
        if component.is_empty() || component == "." {
            continue;
        }
        if component == ".." {
            return Err(ReceiveError::new("relative paths must not contain '..'"));
        }
        let raw = unescape_member_name(component).map_err(|_| {
            ReceiveError::new(format!(
                "invalid escaped manifest member name {}",
                python_repr(component)
            ))
        })?;
        output.push(canonical_component_from_bytes(&raw));
    }
    join_canonical_parts(output)
}

fn scan_source(source_root: &Path) -> ReceiveResult<(Vec<SourceEntry>, Vec<RejectedEntry>)> {
    let source_name = source_root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    if is_package_boundary(&source_name) {
        return Ok((
            vec![SourceEntry {
                source_path: source_root.to_path_buf(),
                relpath: format!("{source_name}.tar"),
                entry_type: SourceEntryType::Package,
                logical_relpath: Some(source_name),
            }],
            Vec::new(),
        ));
    }
    let mut entries = Vec::new();
    let mut rejected = Vec::new();
    scan_source_dir(source_root, source_root, &mut entries, &mut rejected)?;
    entries.sort_by(|left, right| left.relpath.cmp(&right.relpath));
    rejected.sort_by(|left, right| left.relpath.cmp(&right.relpath));
    Ok((entries, rejected))
}

fn scan_source_dir(
    source_root: &Path,
    current_root: &Path,
    entries: &mut Vec<SourceEntry>,
    rejected: &mut Vec<RejectedEntry>,
) -> ReceiveResult<()> {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(current_root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relpath = canonicalize_filesystem_path(&path, source_root)?;
        if metadata.file_type().is_symlink() {
            let reason = symlink_rejection_reason(&path).to_string();
            rejected.push(RejectedEntry {
                relpath,
                source_path: path,
                reason,
            });
        } else if metadata.is_dir() {
            if is_package_boundary(&relpath) {
                entries.push(SourceEntry {
                    source_path: path,
                    relpath: format!("{relpath}.tar"),
                    entry_type: SourceEntryType::Package,
                    logical_relpath: Some(relpath),
                });
            } else {
                dirs.push((relpath, path));
            }
        } else if metadata.is_file() {
            files.push((relpath, path));
        } else {
            rejected.push(RejectedEntry {
                relpath,
                source_path: path,
                reason: special_file_reason(&metadata).to_string(),
            });
        }
    }
    dirs.sort_by(|left, right| left.0.cmp(&right.0));
    for (_relpath, path) in dirs {
        scan_source_dir(source_root, &path, entries, rejected)?;
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    for (relpath, path) in files {
        entries.push(SourceEntry {
            source_path: path,
            relpath,
            entry_type: SourceEntryType::File,
            logical_relpath: None,
        });
    }
    Ok(())
}

fn payload_unit_from_entry(entry: &SourceEntry) -> ReceiveResult<PayloadUnit> {
    match entry.entry_type {
        SourceEntryType::File => {
            let metadata = fs::metadata(&entry.source_path)?;
            Ok(PayloadUnit {
                source_path: entry.source_path.clone(),
                relpath: entry.relpath.clone(),
                entry_type: "file".to_string(),
                logical_relpath: None,
                hint_size: metadata.len(),
                plan_size: metadata.len(),
                mtime_ns: metadata_mtime_ns(&metadata)?,
            })
        }
        SourceEntryType::Package => {
            let logical_relpath = entry
                .logical_relpath
                .clone()
                .ok_or_else(|| ReceiveError::new("package entry missing logical relpath"))?;
            let (plan_size, mtime_ns) = package_plan_metadata(&entry.source_path)?;
            Ok(PayloadUnit {
                source_path: entry.source_path.clone(),
                relpath: entry.relpath.clone(),
                entry_type: "package".to_string(),
                logical_relpath: Some(logical_relpath),
                hint_size: 0,
                plan_size,
                mtime_ns,
            })
        }
    }
}

fn package_plan_metadata(package_root: &Path) -> ReceiveResult<(u64, u64)> {
    let metadata = fs::symlink_metadata(package_root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ReceiveError::new(format!(
            "package boundary is not a directory: {}",
            package_root.display()
        )));
    }
    let mut total_size = 0;
    let mut max_mtime = metadata_mtime_ns(&metadata)?;
    collect_package_plan_metadata(package_root, &mut total_size, &mut max_mtime)?;
    Ok((total_size, max_mtime))
}

fn collect_package_plan_metadata(
    current_root: &Path,
    total_size: &mut u64,
    max_mtime: &mut u64,
) -> ReceiveResult<()> {
    for entry in fs::read_dir(current_root)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        *max_mtime = (*max_mtime).max(metadata_mtime_ns(&metadata)?);
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_package_plan_metadata(&path, total_size, max_mtime)?;
        } else if metadata.is_file() {
            *total_size += metadata.len();
        } else {
            let relpath = path.to_string_lossy();
            return Err(ReceiveError::new(format!(
                "package contains unsupported {}: {relpath}",
                special_file_reason(&metadata)
            )));
        }
    }
    Ok(())
}

fn check_collisions(entries: &[SourceEntry]) -> ReceiveResult<()> {
    let mut seen = BTreeMap::new();
    for entry in entries {
        let key = entry.relpath.to_lowercase();
        if let Some(prior) = seen.insert(key, entry)
            && prior.source_path != entry.source_path
        {
            return Err(ReceiveError::new(format!(
                "canonical receive path collision: {} and {} -> {:?}",
                prior.source_path.display(),
                entry.source_path.display(),
                entry.relpath
            )));
        }
    }
    Ok(())
}

fn copy_or_package_entry(entry: &SourceEntry, data_root: &Path) -> ReceiveResult<FileReceipt> {
    match entry.entry_type {
        SourceEntryType::File => copy_file_entry(entry, data_root),
        SourceEntryType::Package => package_entry(entry, data_root),
    }
}

fn copy_file_entry(entry: &SourceEntry, data_root: &Path) -> ReceiveResult<FileReceipt> {
    let destination = safe_payload_path(data_root, &entry.relpath)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    reject_existing_unsupported_destination(&destination)?;
    let (digest, snapshot) = copy_file_with_digest(&entry.source_path, &destination)?;
    Ok(FileReceipt {
        relpath: entry.relpath.clone(),
        destination_path: destination,
        sha256_hex: digest,
        size_bytes: snapshot.size,
        logical_relpath: None,
        stored_relpath: None,
        package_members: Vec::new(),
    })
}

fn package_entry(entry: &SourceEntry, data_root: &Path) -> ReceiveResult<FileReceipt> {
    let logical_relpath = entry
        .logical_relpath
        .as_ref()
        .ok_or_else(|| ReceiveError::new("package entry missing logical relpath"))?;
    let destination = safe_payload_path(data_root, &entry.relpath)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    reject_existing_unsupported_destination(&destination)?;
    let temp_path = temp_path_for(&destination)?;
    let package = match build_package_tar(&entry.source_path, &temp_path, logical_relpath) {
        Ok(package) => package,
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
    };
    replace_file(&temp_path, &destination)?;
    Ok(FileReceipt {
        relpath: entry.relpath.clone(),
        destination_path: destination,
        sha256_hex: package.digest,
        size_bytes: package.size_bytes,
        logical_relpath: Some(logical_relpath.clone()),
        stored_relpath: Some(entry.relpath.clone()),
        package_members: package.members,
    })
}

fn reject_existing_unsupported_destination(destination: &Path) -> ReceiveResult<()> {
    let metadata = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        return Err(ReceiveError::new(format!(
            "payload destination is a symlink: {}",
            destination.display()
        )));
    }
    if !metadata.is_file() {
        return Err(ReceiveError::new(format!(
            "payload destination is unsupported {}: {}",
            special_file_reason(&metadata),
            destination.display()
        )));
    }
    Ok(())
}

fn copy_file_with_digest(
    source: &Path,
    destination: &Path,
) -> ReceiveResult<(String, SourceStatSnapshot)> {
    let before = source_stat_snapshot(source)?;
    let mut raw_in = File::open(source)?;
    let temp_path = temp_path_for(destination)?;
    let mut raw_out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    let result = (|| {
        loop {
            let read = raw_in.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
            raw_out.write_all(&buffer[..read])?;
        }
        raw_out.flush()?;
        raw_out.sync_all()?;
        drop(raw_out);
        let after = source_stat_snapshot(source)?;
        raise_if_source_mutated(source, &before, &after)?;
        replace_file(&temp_path, destination)?;
        if let Some(parent) = destination.parent() {
            fsync_dir_best_effort(parent);
        }
        Ok((hex_lower(&digest.finalize()), after))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn source_stat_snapshot(path: &Path) -> ReceiveResult<SourceStatSnapshot> {
    let metadata = fs::metadata(path)?;
    Ok(SourceStatSnapshot {
        size: metadata.len(),
        modified: metadata.modified()?,
        inode: metadata_inode(&metadata),
        device: metadata_device(&metadata),
    })
}

fn raise_if_source_mutated(
    path: &Path,
    before: &SourceStatSnapshot,
    after: &SourceStatSnapshot,
) -> ReceiveResult<()> {
    if before == after {
        return Ok(());
    }
    Err(ReceiveError::new(format!(
        "source changed during receive: {}",
        path.display()
    )))
}

#[cfg(unix)]
fn metadata_inode(metadata: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;

    Some(metadata.ino())
}

#[cfg(not(unix))]
fn metadata_inode(_metadata: &fs::Metadata) -> Option<u64> {
    None
}

#[cfg(unix)]
fn metadata_device(metadata: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;

    Some(metadata.dev())
}

#[cfg(not(unix))]
fn metadata_device(_metadata: &fs::Metadata) -> Option<u64> {
    None
}

fn verify_destination_files(receipts: &[FileReceipt]) -> ReceiveResult<()> {
    let mut mismatches = Vec::new();
    for receipt in receipts {
        let actual = sha256_file(&receipt.destination_path)?;
        if actual != receipt.sha256_hex {
            mismatches.push(format!(
                "{} expected {} actual {}",
                receipt.relpath, receipt.sha256_hex, actual
            ));
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(ReceiveError::new(format!(
            "destination verification failed: {mismatches:?}"
        )))
    }
}

fn package_index_payload(receipts: &[FileReceipt]) -> Option<Value> {
    let mut packages = Vec::new();
    for receipt in receipts {
        if receipt.package_members.is_empty() {
            continue;
        }
        packages.push(json!({
            "logical_member_path": receipt.logical_relpath,
            "members": receipt.package_members,
            "profile": PACKAGE_PROFILE_VERSION,
            "sha256": receipt.sha256_hex,
            "size_bytes": receipt.size_bytes,
            "stored_member_path": receipt.stored_relpath,
        }));
    }
    if packages.is_empty() {
        return None;
    }
    Some(json!({
        "package_globs": PACKAGE_GLOBS,
        "packages": packages,
        "profile": PACKAGE_PROFILE_VERSION,
        "profile_hash": PACKAGE_PROFILE_HASH,
    }))
}

fn bag_info_metadata(
    options: &ReceiveOptions,
    file_count: usize,
    total_bytes: u64,
    skipped_count: usize,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("Bagging-Date".to_string(), options.bagging_date.clone()),
        (
            "Payload-Oxum".to_string(),
            format!("{total_bytes}.{file_count}"),
        ),
        (
            "Bag-Software-Agent".to_string(),
            format!("{RECEIVE_PACKAGE_NAME}/{RECEIVE_VERSION}"),
        ),
        ("Receive-Package".to_string(), RECEIVE_PACKAGE.to_string()),
        ("Intake-Id".to_string(), options.intake_id.clone()),
        ("Operator".to_string(), options.operator.clone()),
        ("Source-Kind".to_string(), options.source_kind.clone()),
        (
            "Source-Ref".to_string(),
            options.source_ref.clone().unwrap_or_default(),
        ),
        ("Artifactclass".to_string(), options.artifactclass.clone()),
        (
            "Label".to_string(),
            options.label.clone().unwrap_or_default(),
        ),
        (
            "Canonicalization-Version".to_string(),
            CANONICALIZATION_VERSION.to_string(),
        ),
        (
            "Package-Profile-Version".to_string(),
            PACKAGE_PROFILE_VERSION.to_string(),
        ),
        (
            "Package-Profile-Hash".to_string(),
            PACKAGE_PROFILE_HASH.to_string(),
        ),
        ("Skipped-Count".to_string(), skipped_count.to_string()),
    ])
}

fn write_json_file(path: &Path, value: &Value) -> ReceiveResult<()> {
    let mut observer = |_temp_path: &Path, _final_path: &Path| Ok(());
    atomic_write_text(
        path,
        &format!("{}\n", serde_json::to_string_pretty(value)?),
        &mut observer,
    )
}

fn atomic_write_text<F>(path: &Path, text: &str, observer: &mut F) -> ReceiveResult<()>
where
    F: FnMut(&Path, &Path) -> ReceiveResult<()>,
{
    atomic_write_bytes(path, text.as_bytes(), observer)
}

fn atomic_write_bytes<F>(path: &Path, data: &[u8], observer: &mut F) -> ReceiveResult<()>
where
    F: FnMut(&Path, &Path) -> ReceiveResult<()>,
{
    let parent = path
        .parent()
        .ok_or_else(|| ReceiveError::new(format!("path has no parent: {}", path.display())))?;
    fs::create_dir_all(parent)?;
    fsync_dir_best_effort(parent);
    let temp_path = temp_path_for(path)?;
    let result = (|| {
        let mut handle = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        handle.write_all(data)?;
        handle.flush()?;
        handle.sync_all()?;
        drop(handle);
        observer(&temp_path, path)?;
        replace_file(&temp_path, path)?;
        fsync_dir_best_effort(parent);
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn temp_path_for(path: &Path) -> ReceiveResult<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| ReceiveError::new(format!("path has no file name: {}", path.display())))?
        .to_string_lossy();
    Ok(path.with_file_name(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    )))
}

fn replace_file(temp_path: &Path, final_path: &Path) -> ReceiveResult<()> {
    #[cfg(windows)]
    if final_path.exists() {
        fs::remove_file(final_path)?;
    }
    fs::rename(temp_path, final_path)?;
    Ok(())
}

#[cfg(unix)]
fn fsync_dir_best_effort(path: &Path) {
    if let Ok(handle) = File::open(path) {
        let _ = handle.sync_all();
    }
}

#[cfg(not(unix))]
fn fsync_dir_best_effort(_path: &Path) {}

fn is_package_boundary(relpath: &str) -> bool {
    let whole = relpath.to_lowercase();
    let name = whole.rsplit('/').next().unwrap_or(&whole);
    PACKAGE_GLOBS.iter().any(|pattern| {
        let suffix = pattern.strip_prefix('*').unwrap_or(pattern).to_lowercase();
        name.ends_with(&suffix) || whole.ends_with(&suffix)
    })
}

fn validate_source_root(source: &Path) -> ReceiveResult<()> {
    let metadata = fs::metadata(source)?;
    if !metadata.is_dir() {
        return Err(ReceiveError::new(format!(
            "receive source must be a directory: {}",
            source.display()
        )));
    }
    if is_inside_existing_payload(source) {
        return Err(ReceiveError::new(format!(
            "receive source is inside an existing intake payload: {}",
            source.display()
        )));
    }
    Ok(())
}

fn validate_landing_relationship(source: &Path, landing: &Path) -> ReceiveResult<()> {
    if landing.starts_with(source) {
        return Err(ReceiveError::new(format!(
            "landing root {} must not be inside source {}",
            landing.display(),
            source.display()
        )));
    }
    Ok(())
}

fn is_inside_existing_payload(path: &Path) -> bool {
    path.ancestors().any(|candidate| {
        let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        if !matches!(name, DATA_DIR_NAME | "payload") {
            return false;
        }
        candidate.parent().is_some_and(|parent| {
            parent.join("intake.json").exists() || parent.join(".receiving.json").exists()
        })
    })
}

fn required_json_string(value: &Value, key: &str) -> ReceiveResult<String> {
    optional_json_string(value, key)
        .ok_or_else(|| ReceiveError::new(format!(".receiving.json missing {key}")))
}

fn optional_json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .filter(|item| !item.is_empty())
}

fn prune_stale_payload_files(
    payload_root: &Path,
    desired_relpaths: &std::collections::BTreeSet<String>,
) -> ReceiveResult<()> {
    if !payload_root.exists() {
        return Ok(());
    }
    let mut paths = collect_payload_paths(payload_root)?;
    paths.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for path in &paths {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.is_dir() {
            continue;
        }
        let relpath = canonicalize_filesystem_path(path, payload_root)?;
        if desired_relpaths.contains(&relpath) {
            continue;
        }
        fs::remove_file(path)?;
    }
    for path in paths {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() {
            continue;
        }
        let _ = fs::remove_dir(&path);
    }
    fsync_dir_best_effort(payload_root);
    Ok(())
}

fn collect_payload_paths(root: &Path) -> ReceiveResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if !root.exists() {
        return Ok(paths);
    }
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        paths.push(path.clone());
        if fs::symlink_metadata(&path)?.is_dir() {
            paths.extend(collect_payload_paths(&path)?);
        }
    }
    Ok(paths)
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> ReceiveResult<u64> {
    let duration = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    Ok(duration.as_nanos().try_into().unwrap_or(u64::MAX))
}

fn symlink_rejection_reason(path: &Path) -> &'static str {
    if fs::metadata(path)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
    {
        "symlink-directory"
    } else {
        "symlink"
    }
}

fn special_file_reason(metadata: &fs::Metadata) -> &'static str {
    special_file_reason_for_type(metadata.file_type())
}

#[cfg(unix)]
fn special_file_reason_for_type(file_type: fs::FileType) -> &'static str {
    use std::os::unix::fs::FileTypeExt;

    if file_type.is_fifo() {
        "fifo"
    } else if file_type.is_char_device() {
        "character-device"
    } else if file_type.is_block_device() {
        "block-device"
    } else if file_type.is_socket() {
        "socket"
    } else {
        "non-regular"
    }
}

#[cfg(not(unix))]
fn special_file_reason_for_type(_file_type: fs::FileType) -> &'static str {
    "non-regular"
}

fn collect_payload_hashes(
    payload_root: &Path,
    current_root: &Path,
    reject_native_packages: bool,
    receipts: &mut Vec<HashReceipt>,
) -> ReceiveResult<()> {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(current_root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relpath = canonicalize_filesystem_path(&path, payload_root)?;
        if metadata.file_type().is_symlink() {
            return Err(ReceiveError::new(format!(
                "payload contains unsupported symlink: {relpath}"
            )));
        }
        if metadata.is_dir() {
            if reject_native_packages && is_package_boundary(&relpath) {
                return Err(ReceiveError::new(format!(
                    "payload contains un-normalized package directory {relpath:?}; re-run sutra receive so it is stored as a {PACKAGE_PROFILE_VERSION} tar"
                )));
            }
            dirs.push((relpath, path));
        } else if metadata.is_file() {
            files.push((relpath, path, metadata.len()));
        } else {
            return Err(ReceiveError::new(format!(
                "payload contains unsupported special-file: {relpath}"
            )));
        }
    }
    dirs.sort_by(|left, right| left.0.cmp(&right.0));
    for (_relpath, path) in dirs {
        collect_payload_hashes(payload_root, &path, reject_native_packages, receipts)?;
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    for (relpath, path, size_bytes) in files {
        receipts.push(HashReceipt {
            relpath,
            sha256_hex: sha256_file(&path)?,
            size_bytes,
        });
    }
    Ok(())
}

fn read_checksum_manifest(
    path: &Path,
    payload_manifest: bool,
) -> ReceiveResult<BTreeMap<String, String>> {
    let mut records = BTreeMap::new();
    for (line_index, raw_line) in fs::read_to_string(path)?.lines().enumerate() {
        let line_number = line_index + 1;
        if raw_line.is_empty() {
            continue;
        }
        if raw_line.len() < 66 {
            return Err(ReceiveError::new(format!(
                "invalid checksum line {line_number} in {}",
                path.display()
            )));
        }
        let bytes = raw_line.as_bytes();
        let digest_bytes = &bytes[..64];
        if !digest_bytes.iter().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ReceiveError::new(format!(
                "invalid sha256 at line {line_number} in {}",
                path.display()
            )));
        }
        let digest: String = digest_bytes
            .iter()
            .map(|byte| (*byte as char).to_ascii_lowercase())
            .collect();
        let mut index = 64;
        if !matches!(bytes.get(index), Some(b' ' | b'\t')) {
            return Err(ReceiveError::new(format!(
                "invalid checksum separator at line {line_number} in {}",
                path.display()
            )));
        }
        while matches!(bytes.get(index), Some(b' ' | b'\t')) {
            index += 1;
        }
        let encoded_path = &raw_line[index..];
        if encoded_path.is_empty() {
            return Err(ReceiveError::new(format!(
                "missing checksum path at line {line_number} in {}",
                path.display()
            )));
        }
        let decoded_path = decode_bagit_path(encoded_path)?;
        let relpath = if payload_manifest {
            canonicalize_payload_manifest_path(&decoded_path)?
        } else {
            normalize_posix_path(&decoded_path)
        };
        records.insert(relpath, digest);
    }
    Ok(records)
}

fn canonicalize_payload_manifest_path(decoded_path: &str) -> ReceiveResult<String> {
    let mut value = decoded_path;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    if value.starts_with('/') {
        return Err(ReceiveError::new(format!(
            "payload manifest path must be relative: {}",
            python_repr(decoded_path)
        )));
    }
    let Some(_rest) = value.strip_prefix("data/") else {
        return Err(ReceiveError::new(format!(
            "payload manifest path must start with data/: {}",
            python_repr(decoded_path)
        )));
    };
    canonicalize_manifest_path(value)
}

fn verify_tagmanifest(root: &Path, required_tag_files: &[String]) -> Vec<TagManifestMismatch> {
    let path = root.join("tagmanifest-sha256.txt");
    let expected = match read_checksum_manifest(&path, false) {
        Ok(expected) => expected,
        Err(error) => {
            return vec![TagManifestMismatch {
                path: "tagmanifest-sha256.txt".to_string(),
                expected: Some("readable".to_string()),
                actual: Some(error.to_string()),
            }];
        }
    };
    let mut mismatched = Vec::new();
    for relpath in ["bagit.txt", "bag-info.txt", "manifest-sha256.txt"]
        .into_iter()
        .map(ToString::to_string)
        .chain(required_tag_files.iter().cloned())
    {
        if !expected.contains_key(&relpath) {
            mismatched.push(TagManifestMismatch {
                path: relpath,
                expected: Some("listed".to_string()),
                actual: None,
            });
        }
    }
    for (relpath, expected_digest) in expected {
        let target = root.join(&relpath);
        if !is_safe_bag_relative_path(&relpath) {
            mismatched.push(TagManifestMismatch {
                path: relpath,
                expected: Some(expected_digest),
                actual: Some("unsafe path".to_string()),
            });
            continue;
        }
        if !target.is_file() {
            mismatched.push(TagManifestMismatch {
                path: relpath,
                expected: Some(expected_digest),
                actual: None,
            });
            continue;
        }
        match sha256_file(&target) {
            Ok(actual) if actual == expected_digest => {}
            Ok(actual) => mismatched.push(TagManifestMismatch {
                path: relpath,
                expected: Some(expected_digest),
                actual: Some(actual),
            }),
            Err(error) => mismatched.push(TagManifestMismatch {
                path: relpath,
                expected: Some(expected_digest),
                actual: Some(error.to_string()),
            }),
        }
    }
    mismatched
}

fn decode_bagit_path(path: &str) -> ReceiveResult<String> {
    let mut output = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        if current != '%' {
            output.push(current);
            index += 1;
            continue;
        }
        if index + 2 >= chars.len() {
            return Err(ReceiveError::new(format!(
                "invalid BagIt percent escape in path: {}",
                python_repr(path)
            )));
        }
        let token = format!("{}{}", chars[index + 1], chars[index + 2]);
        match token.to_ascii_uppercase().as_str() {
            "25" => output.push('%'),
            "0D" => output.push('\r'),
            "0A" => output.push('\n'),
            _ => {
                return Err(ReceiveError::new(format!(
                    "unsupported BagIt percent escape %{token} in path: {}",
                    python_repr(path)
                )));
            }
        }
        index += 3;
    }
    Ok(output)
}

fn normalize_posix_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let normalized = path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/");
    if absolute && normalized.is_empty() {
        "/".to_string()
    } else if absolute {
        format!("/{normalized}")
    } else if normalized.is_empty() {
        ".".to_string()
    } else {
        normalized
    }
}

fn is_safe_bag_relative_path(relpath: &str) -> bool {
    !relpath.is_empty()
        && !relpath.starts_with('/')
        && relpath
            .split('/')
            .all(|part| !part.is_empty() && !matches!(part, "." | ".."))
}

fn json_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn json_manifest_mismatches(value: &Value) -> Vec<ManifestDigestMismatch> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    Some(ManifestDigestMismatch {
                        path: item["path"].as_str()?.to_string(),
                        expected: item["expected"].as_str()?.to_string(),
                        actual: item["actual"].as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn receive_package_error(metadata: &BTreeMap<String, String>) -> Option<String> {
    let actual = metadata.get("Receive-Package").map(String::as_str);
    if actual == Some(RECEIVE_PACKAGE) {
        return None;
    }
    Some(format!(
        "Receive-Package mismatch: expected {RECEIVE_PACKAGE}, actual {}",
        actual
            .map(python_repr)
            .unwrap_or_else(|| "None".to_string())
    ))
}

fn package_members(
    package_root: &Path,
    logical_relpath: &str,
) -> ReceiveResult<Vec<PackageMember>> {
    let mut members = vec![PackageMember {
        source_path: package_root.to_path_buf(),
        member_name: logical_relpath.to_string(),
        member_type: PackageMemberType::Directory,
        mode: PACKAGE_DIR_MODE,
        size: 0,
        linkname: None,
    }];
    collect_package_members(package_root, package_root, logical_relpath, &mut members)?;
    members.sort_by(|left, right| left.member_name.cmp(&right.member_name));
    Ok(members)
}

fn collect_package_members(
    package_root: &Path,
    current_root: &Path,
    logical_relpath: &str,
    members: &mut Vec<PackageMember>,
) -> ReceiveResult<()> {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in fs::read_dir(current_root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relpath = canonicalize_filesystem_path(&path, package_root)?;
        if metadata.file_type().is_symlink() {
            let member = package_member_from_path(package_root, &path, logical_relpath, &metadata)?;
            members.push(member);
        } else if metadata.is_dir() {
            dirs.push((relpath, path, metadata));
        } else if metadata.is_file() {
            files.push((relpath, path, metadata));
        } else {
            return Err(ReceiveError::new(format!(
                "package contains unsupported special-file: {relpath}"
            )));
        }
    }

    dirs.sort_by(|left, right| left.0.cmp(&right.0));
    for (_relpath, path, metadata) in dirs {
        let member = package_member_from_path(package_root, &path, logical_relpath, &metadata)?;
        members.push(member);
        collect_package_members(package_root, &path, logical_relpath, members)?;
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    for (_relpath, path, metadata) in files {
        let member = package_member_from_path(package_root, &path, logical_relpath, &metadata)?;
        members.push(member);
    }
    Ok(())
}

fn package_member_from_path(
    package_root: &Path,
    path: &Path,
    logical_relpath: &str,
    metadata: &fs::Metadata,
) -> ReceiveResult<PackageMember> {
    let relpath = canonicalize_filesystem_path(path, package_root)?;
    let member_name = format!("{logical_relpath}/{relpath}");
    if metadata.file_type().is_symlink() {
        return Ok(PackageMember {
            source_path: path.to_path_buf(),
            member_name,
            member_type: PackageMemberType::Symlink,
            mode: PACKAGE_SYMLINK_MODE,
            size: 0,
            linkname: Some(link_text(path)?),
        });
    }
    if metadata.is_dir() {
        return Ok(PackageMember {
            source_path: path.to_path_buf(),
            member_name,
            member_type: PackageMemberType::Directory,
            mode: PACKAGE_DIR_MODE,
            size: 0,
            linkname: None,
        });
    }
    if metadata.is_file() {
        return Ok(PackageMember {
            source_path: path.to_path_buf(),
            member_name,
            member_type: PackageMemberType::File,
            mode: PACKAGE_FILE_MODE,
            size: metadata.len(),
            linkname: None,
        });
    }
    Err(ReceiveError::new(format!(
        "package contains unsupported special-file: {member_name}"
    )))
}

fn tar_header(
    member_name: &str,
    mode: u32,
    size: u64,
    typeflag: u8,
    linkname: &str,
    directory_name: bool,
) -> ReceiveResult<[u8; TAR_BLOCK_BYTES as usize]> {
    let mut header = [0_u8; TAR_BLOCK_BYTES as usize];
    let header_name = if directory_name {
        format!("{member_name}/")
    } else {
        member_name.to_string()
    };
    write_tar_field(
        &mut header[0..100],
        header_name.as_bytes(),
        "tar member name",
    )?;
    write_tar_octal(&mut header[100..108], mode as u64, 7);
    write_tar_octal(&mut header[108..116], 0, 7);
    write_tar_octal(&mut header[116..124], 0, 7);
    write_tar_octal(&mut header[124..136], size, 11);
    write_tar_octal(&mut header[136..148], PACKAGE_MTIME, 11);
    for byte in &mut header[148..156] {
        *byte = b' ';
    }
    header[156] = typeflag;
    write_tar_field(&mut header[157..257], linkname.as_bytes(), "tar link name")?;
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
    let checksum_text = format!("{checksum:06o}");
    header[148..154].copy_from_slice(checksum_text.as_bytes());
    header[154] = 0;
    header[155] = b' ';
    Ok(header)
}

fn write_tar_field(field: &mut [u8], value: &[u8], label: &str) -> ReceiveResult<()> {
    if value.len() > field.len() {
        return Err(ReceiveError::new(format!(
            "{label} exceeds {} bytes",
            field.len()
        )));
    }
    field[..value.len()].copy_from_slice(value);
    Ok(())
}

fn write_tar_octal(field: &mut [u8], value: u64, digits: usize) {
    let text = format!("{value:0digits$o}");
    field[..digits].copy_from_slice(text.as_bytes());
    field[digits] = 0;
}

fn copy_file_to_tar(source: &Path, writer: &mut TarHashingWriter<File>) -> ReceiveResult<String> {
    let before = source_stat_snapshot(source)?;
    let mut digest = Sha256::new();
    let mut handle = File::open(source)?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = handle.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        writer.write_all(&buffer[..read])?;
    }
    let after = source_stat_snapshot(source)?;
    raise_if_source_mutated(source, &before, &after)?;
    Ok(hex_lower(&digest.finalize()))
}

fn pad_tar_data(writer: &mut TarHashingWriter<File>, size: u64) -> ReceiveResult<()> {
    let remainder = size % TAR_BLOCK_BYTES;
    if remainder == 0 {
        return Ok(());
    }
    let padding = TAR_BLOCK_BYTES - remainder;
    writer.write_all(&vec![0_u8; padding as usize])?;
    Ok(())
}

struct TarHashingWriter<W: Write> {
    inner: W,
    digest: Sha256,
    position: u64,
}

impl<W: Write> TarHashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            position: 0,
        }
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn hexdigest(self) -> String {
        hex_lower(&self.digest.finalize())
    }
}

impl<W: Write> Write for TarHashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.digest.update(&buffer[..written]);
        self.position += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn join_canonical_parts(parts: Vec<String>) -> ReceiveResult<String> {
    if parts.is_empty() {
        return Err(ReceiveError::new("relative path is empty"));
    }
    Ok(parts.join("/"))
}

fn canonical_component_from_bytes(raw: &[u8]) -> String {
    match std::str::from_utf8(raw) {
        Ok(text) => {
            let normalized: String = text.nfc().collect();
            escape_member_name(normalized.as_bytes())
        }
        Err(_) => escape_member_name(raw),
    }
}

fn must_escape_byte(byte: u8) -> bool {
    byte < 0x20 || byte == 0x7f
}

fn push_hex_escape(output: &mut String, byte: u8) {
    output.push_str("\\x");
    output.push(hex_digit(byte >> 4));
    output.push(hex_digit(byte & 0x0f));
}

fn utf8_sequence_len(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 0,
    }
}

fn lower_hex_value(value: char) -> Option<u8> {
    match value {
        '0'..='9' => Some(value as u8 - b'0'),
        'a'..='f' => Some(value as u8 - b'a' + 10),
        _ => None,
    }
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex digit nibble must be <= 15"),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    output
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn encode_bagit_path(path: &str) -> String {
    let mut output = String::new();
    for char in path.chars() {
        match char {
            '%' => output.push_str("%25"),
            '\r' => output.push_str("%0D"),
            '\n' => output.push_str("%0A"),
            _ => output.push(char),
        }
    }
    output
}

fn bag_info_value(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn resolve_root(path: &Path) -> ReceiveResult<PathBuf> {
    match fs::canonicalize(path) {
        Ok(resolved) => Ok(resolved),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if path.is_absolute() {
                Ok(path.to_path_buf())
            } else {
                Ok(std::env::current_dir()?.join(path))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn link_text(path: &Path) -> ReceiveResult<String> {
    Ok(fs::read_link(path)?.to_string_lossy().into_owned())
}

fn python_repr(value: &str) -> String {
    let mut output = String::from("'");
    for char in value.chars() {
        match char {
            '\\' => output.push_str(r"\\"),
            '\'' => output.push_str(r"\'"),
            '\n' => output.push_str(r"\n"),
            '\r' => output.push_str(r"\r"),
            '\t' => output.push_str(r"\t"),
            '\u{00}'..='\u{1f}' | '\u{7f}' => {
                let byte = char as u8;
                output.push_str("\\x");
                output.push(hex_digit(byte >> 4));
                output.push(hex_digit(byte & 0x0f));
            }
            _ => output.push(char),
        }
    }
    output.push('\'');
    output
}

fn python_repr_value(value: &Value) -> String {
    if value.is_null() {
        "None".to_string()
    } else if let Some(text) = value.as_str() {
        python_repr(text)
    } else {
        value.to_string()
    }
}

fn join_lines(lines: Vec<String>) -> String {
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

#[cfg(unix)]
fn os_str_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(not(unix))]
fn os_str_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    value.to_string_lossy().as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_operator_matches_python_shape() {
        assert_eq!(slug_operator("Op Name"), "op-name");
        assert_eq!(slug_operator("Śwami / Camera 1"), "owner-camera-1");
        assert_eq!(slug_operator("..."), "operator");
    }

    #[test]
    fn safe_payload_rejects_absolute_and_traversal_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        std::fs::create_dir(&root).unwrap();
        assert!(safe_payload_path(&root, "../escape.mov").is_err());
        assert!(safe_payload_path(&root, "/absolute.mov").is_err());
        assert_eq!(
            safe_payload_path(&root, "folder/clip.mov").unwrap(),
            root.canonicalize().unwrap().join("folder").join("clip.mov")
        );
    }

    #[cfg(unix)]
    #[test]
    fn safe_payload_rejects_symlink_components() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let target = temp.path().join("target");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&target).unwrap();

        symlink(&target, root.join("linked-dir")).unwrap();
        assert!(
            safe_payload_path(&root, "linked-dir/clip.mov")
                .unwrap_err()
                .to_string()
                .contains("payload directory is a symlink")
        );

        symlink(target.join("clip.mov"), root.join("linked-file.mov")).unwrap();
        assert!(
            safe_payload_path(&root, "linked-file.mov")
                .unwrap_err()
                .to_string()
                .contains("payload destination is a symlink")
        );
    }

    #[test]
    fn copy_file_with_digest_reports_source_size() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.mov");
        let destination = temp.path().join("data").join("clip.mov");
        std::fs::create_dir(destination.parent().unwrap()).unwrap();
        std::fs::write(&source, b"hello").unwrap();

        let (digest, snapshot) = copy_file_with_digest(&source, &destination).unwrap();

        assert_eq!(
            digest,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(snapshot.size, 5);
        assert_eq!(std::fs::read(destination).unwrap(), b"hello");
    }

    #[test]
    fn source_stat_guard_rejects_changed_size() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.mov");
        std::fs::write(&source, b"hello").unwrap();
        let before = source_stat_snapshot(&source).unwrap();
        raise_if_source_mutated(&source, &before, &before).unwrap();

        std::fs::write(&source, b"hello world").unwrap();
        let after = source_stat_snapshot(&source).unwrap();
        assert!(
            raise_if_source_mutated(&source, &before, &after)
                .unwrap_err()
                .to_string()
                .contains("source changed during receive")
        );
    }
}
