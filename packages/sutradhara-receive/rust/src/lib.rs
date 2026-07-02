//! Rust receive-core primitives for the Sutradhara receive contract.
//!
//! This M2 slice intentionally covers the stratum-1 behavior pinned by the
//! Python-derived conformance fixtures: reversible member-name escaping,
//! receive-path canonicalization, BagIt tag text builders, SHA-256 hashing, and
//! path-safe operator slugs. Later migration stages build the write side,
//! command-line binary, and PyO3 wheel on top of these primitives.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
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

pub type ReceiveResult<T> = Result<T, ReceiveError>;

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
}
