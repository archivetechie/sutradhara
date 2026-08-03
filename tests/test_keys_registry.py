"""Tests for Sutradhara's X-Wing recipient-key epoch registry."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest
from click.testing import CliRunner

from sutradhara.cli.main import cli
from sutradhara.keys import (
    KEY_DOMAIN_ARCHIVE,
    KEY_DOMAIN_BACKUP,
    KEY_DOMAIN_HDCACHE,
    KEY_DOMAIN_RECOVERY,
    KeyRegistry,
    assert_key_epoch_domain,
    key_domain,
    mint_recovery_keypair,
)
from sutradhara.keys.remanence import RecipientPublicIdentity
from tests.key_helpers import (
    TEST_RECIPIENT_CODEC,
    DeterministicRecipientKeyCodec,
    registry_with_recovery,
)

_TEST_SEED = bytes.fromhex(
    "73797374656d2d6861726e6573733a737574726164686172612d6b65792d7365"
    "616d3a616d6265722d616561642d6465763a7631"
)


@pytest.fixture(autouse=True)
def _hermetic_recipient_codec(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep registry unit tests independent of an installed Remanence binary."""

    monkeypatch.setattr(
        "sutradhara.keys.registry.RemRecipientKeyCodec",
        lambda: TEST_RECIPIENT_CODEC,
    )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _expected_epoch_id(domain: str, generation: int = 0) -> str:
    prefix = _TEST_SEED + b":" + domain.encode() + b":" + str(generation).encode()
    return f"{domain}-{hashlib.sha256(prefix + b':epoch-id').digest()[:16].hex()}"


def test_key_registry_create_materialize_and_retire_preserves_keypair(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    epoch = registry.create_epoch()

    assert epoch.key_id == _expected_epoch_id(KEY_DOMAIN_ARCHIVE)
    assert epoch.active is True
    assert _mode(registry.registry_dir) == 0o700
    private_path = registry.registry_dir / f"{epoch.key_id}.private"
    public_path = registry.registry_dir / f"{epoch.key_id}.public"
    state_path = registry.registry_dir / f"{epoch.key_id}.json"
    assert len(private_path.read_bytes()) == 32
    assert public_path.read_bytes().startswith(b"REMR\0")
    assert _mode(private_path) == 0o600
    assert _mode(public_path) == 0o644
    assert _mode(state_path) == 0o600
    assert json.loads(state_path.read_text())["deterministic_test"] is True

    with registry.materialized_private_key(epoch.key_id) as key_path:
        materialized = key_path
        payload = materialized.read_bytes()
        assert payload.startswith(b"REMP" + bytes.fromhex(epoch.key_id.rsplit("-", 1)[1]))
        assert b"archive" in payload
        assert _mode(materialized) == 0o600
    assert not materialized.exists()

    retired = registry.retire_epoch(epoch.key_id)
    assert retired["private_key_preserved"] is True
    assert retired["public_key_preserved"] is True
    assert private_path.is_file()
    assert public_path.is_file()
    assert registry.get_epoch(epoch.key_id).active is False
    with registry.materialized_private_key(epoch.key_id) as retired_key:
        assert retired_key.read_bytes().startswith(b"REMP")


def test_key_registry_create_epoch_is_idempotent_and_random_by_default(tmp_path: Path) -> None:
    first_registry = KeyRegistry(tmp_path / "keys-one")
    first = first_registry.create_epoch()
    assert first_registry.create_epoch() == first

    second = KeyRegistry(tmp_path / "keys-two").create_epoch()
    assert first.key_id.startswith("archive-")
    assert second.key_id.startswith("archive-")
    assert second.key_id != first.key_id


def test_materialized_private_key_is_zeroized_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    epoch = registry.create_epoch()
    original_unlink = Path.unlink
    erased: list[bytes] = []

    def recording_unlink(path: Path, missing_ok: bool = False) -> None:
        if path.name.startswith("rem-private-") and path.exists():
            erased.append(path.read_bytes())
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", recording_unlink)
    with registry.materialized_private_key(epoch.key_id) as key_path:
        materialized_size = key_path.stat().st_size
        assert key_path.read_bytes() != b"\0" * materialized_size

    assert len(erased) == 2  # integrity derivation temp + yielded private temp
    assert all(payload == b"\0" * len(payload) for payload in erased)
    assert len(erased[-1]) == materialized_size


def test_key_registry_domains_are_explicit_namespaces(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)

    archive = registry.create_epoch(KEY_DOMAIN_ARCHIVE)
    hdcache = registry.create_epoch(KEY_DOMAIN_HDCACHE)
    backup = registry.create_epoch(KEY_DOMAIN_BACKUP)

    assert key_domain(archive.key_id) == KEY_DOMAIN_ARCHIVE
    assert key_domain(hdcache.key_id) == KEY_DOMAIN_HDCACHE
    assert key_domain(backup.key_id) == KEY_DOMAIN_BACKUP
    assert len({archive.key_id, hdcache.key_id, backup.key_id}) == 3
    with pytest.raises(ValueError, match="prefix"):
        key_domain("1" * 32)


def test_recovery_is_offline_minted_and_imported_public_only(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    public_path = tmp_path / "escrow" / "recovery.remr"
    private_path = tmp_path / "escrow" / "recovery.remp"
    public_path.parent.mkdir()

    minted = mint_recovery_keypair(
        public_key_path=public_path,
        private_key_path=private_path,
    )
    imported = registry.import_public_epoch(public_path)

    assert imported.key_id == minted.key_id
    assert key_domain(imported.key_id) == KEY_DOMAIN_RECOVERY
    assert private_path.is_file()
    assert _mode(private_path) == 0o600
    assert registry.public_key_path(imported.key_id).is_file()
    assert not (registry.registry_dir / f"{imported.key_id}.private").exists()
    with (
        pytest.raises(KeyError, match="private key unavailable"),
        registry.materialized_private_key(imported.key_id),
    ):
        pass
    with pytest.raises(ValueError, match="minted offline"):
        registry.create_epoch(KEY_DOMAIN_RECOVERY)


def test_recovery_import_validates_and_stores_one_public_snapshot(tmp_path: Path) -> None:
    escrow = tmp_path / "escrow"
    escrow.mkdir()
    public_path = escrow / "recovery.remr"
    private_path = escrow / "recovery.remp"
    mint_recovery_keypair(
        public_key_path=public_path,
        private_key_path=private_path,
    )
    original = public_path.read_bytes()
    inspected_payloads: list[bytes] = []

    class SourceSwappingCodec(DeterministicRecipientKeyCodec):
        def inspect_public(self, snapshot_path: Path) -> RecipientPublicIdentity:
            assert snapshot_path != public_path
            public_path.write_bytes(b"RAOR legacy replacement")
            inspected_payloads.append(snapshot_path.read_bytes())
            return super().inspect_public(snapshot_path)

    registry = KeyRegistry(
        tmp_path / "keys",
        deterministic_test=True,
        recipient_codec=SourceSwappingCodec(),
    )
    imported = registry.import_public_epoch(public_path)

    assert inspected_payloads
    assert set(inspected_payloads) == {original}
    assert registry.public_key_path(imported.key_id).read_bytes() == original
    assert public_path.read_bytes() == b"RAOR legacy replacement"


def test_recovery_import_rejects_legacy_public_key_without_codec_call(tmp_path: Path) -> None:
    public_path = tmp_path / "legacy.raor"
    public_path.write_bytes(b"RAOR" + b"\0" * 58)
    registry = KeyRegistry(
        tmp_path / "keys",
        deterministic_test=True,
        recipient_codec=TEST_RECIPIENT_CODEC,
    )

    with pytest.raises(ValueError, match="recipient public-key file"):
        registry.import_public_epoch(public_path)


def test_recovery_reimport_refuses_corrupt_existing_state(tmp_path: Path) -> None:
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    public_path = registry.public_key_path(recovery.key_id)
    state_path = registry.registry_dir / f"{recovery.key_id}.json"
    state = json.loads(state_path.read_text())
    state["domain"] = KEY_DOMAIN_ARCHIVE
    state_path.write_text(json.dumps(state))

    with pytest.raises(RuntimeError, match="state identity mismatch"):
        registry.import_public_epoch(public_path)


def test_recovery_import_rotates_active_public_epoch_without_private_material(
    tmp_path: Path,
) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    escrow = tmp_path / "escrow"
    escrow.mkdir()
    imported = []
    for index in (1, 2):
        public_path = escrow / f"recovery-{index}.remr"
        private_path = escrow / f"recovery-{index}.remp"
        mint_recovery_keypair(
            public_key_path=public_path,
            private_key_path=private_path,
        )
        imported.append(registry.import_public_epoch(public_path))

    first, second = imported
    assert registry.get_epoch(first.key_id).active is False
    assert registry.active_epoch(KEY_DOMAIN_RECOVERY) == second
    assert registry.public_key_path(first.key_id).is_file()
    assert not list(registry.registry_dir.glob("recovery-*.private"))


def test_registry_scan_refuses_invalid_inactive_state_markers(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    archive = registry.create_epoch()
    registry.retire_epoch(archive.key_id)
    state_path = registry.registry_dir / f"{archive.key_id}.json"
    state = json.loads(state_path.read_text())
    state["active"] = 0
    state_path.write_text(json.dumps(state))

    with pytest.raises(RuntimeError, match="active marker is invalid"):
        registry.create_epoch(KEY_DOMAIN_BACKUP)


def test_registry_resolves_seal_recipients_and_domain_private_key(tmp_path: Path) -> None:
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    archive = registry.create_epoch(KEY_DOMAIN_ARCHIVE)
    hdcache = registry.create_epoch(KEY_DOMAIN_HDCACHE)

    assert registry.recipients_for_seal(archive.key_id, domain=KEY_DOMAIN_ARCHIVE) == (
        archive,
        recovery,
    )
    assert (
        registry.select_private_epoch(
            [archive.key_id, recovery.key_id],
            domain=KEY_DOMAIN_ARCHIVE,
        )
        == archive
    )
    with pytest.raises(KeyError, match="no hdcache recipient"):
        registry.select_private_epoch(
            [archive.key_id, recovery.key_id],
            domain=KEY_DOMAIN_HDCACHE,
        )
    with pytest.raises(ValueError, match="requires archive"):
        registry.recipients_for_seal(hdcache.key_id, domain=KEY_DOMAIN_ARCHIVE)


def test_deterministic_mode_has_path_and_state_interlocks(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside /var/lib"):
        KeyRegistry("/var/lib/test-sutradhara-keys", deterministic_test=True)
    with pytest.raises(ValueError, match="outside /var/lib"):
        KeyRegistry(deterministic_test=True)
    with pytest.raises(ValueError, match="custom recipient codec"):
        KeyRegistry("/var/lib/test-sutradhara-keys", recipient_codec=TEST_RECIPIENT_CODEC)

    path = tmp_path / "keys"
    test_registry = KeyRegistry(path, deterministic_test=True)
    epoch = test_registry.create_epoch()
    with pytest.raises(RuntimeError, match="deterministic test epoch"):
        KeyRegistry(path).get_epoch(epoch.key_id)


def test_private_epoch_selection_fails_closed_on_corrupt_matching_state(tmp_path: Path) -> None:
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    archive = registry.create_epoch()
    state_path = registry.registry_dir / f"{archive.key_id}.json"
    state = json.loads(state_path.read_text())
    state["key_kind"] = "public-only"
    state_path.write_text(json.dumps(state))

    with pytest.raises(RuntimeError, match="not a keypair"):
        registry.select_private_epoch(
            [archive.key_id, recovery.key_id],
            domain=KEY_DOMAIN_ARCHIVE,
        )


def test_private_epoch_selection_fails_closed_on_mismatched_keypair(tmp_path: Path) -> None:
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    archive = registry.create_epoch()
    private_path = registry.registry_dir / f"{archive.key_id}.private"
    private_path.write_bytes(b"x" * 32)

    with pytest.raises(RuntimeError, match="keypair material mismatch"):
        registry.select_private_epoch(
            [archive.key_id, recovery.key_id],
            domain=KEY_DOMAIN_ARCHIVE,
        )
    with (
        pytest.raises(RuntimeError, match="keypair material mismatch"),
        registry.materialized_private_key(archive.key_id),
    ):
        pass


def test_admin_recovery_mint_and_public_import_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_dir = tmp_path / "registry"
    public_path = tmp_path / "escrow" / "recovery public.remr"
    private_path = tmp_path / "escrow" / "recovery private.remp"
    public_path.parent.mkdir()
    monkeypatch.setenv("SUTRADHARA_KEY_REGISTRY_DIR", str(registry_dir))
    runner = CliRunner()

    minted = runner.invoke(
        cli,
        [
            "admin",
            "keys",
            "mint-recovery",
            "--public-key",
            str(public_path),
            "--private-key",
            str(private_path),
        ],
    )
    assert minted.exit_code == 0, minted.output
    assert "sutra admin keys import-public" in minted.output
    assert public_path.is_file()
    assert private_path.is_file()
    assert _mode(private_path) == 0o600

    imported = runner.invoke(
        cli,
        ["admin", "keys", "import-public", "--public-key", str(public_path)],
    )
    assert imported.exit_code == 0, imported.output
    [state_path] = registry_dir.glob("recovery-*.json")
    epoch_id = state_path.stem
    assert (registry_dir / f"{epoch_id}.public").is_file()
    assert not (registry_dir / f"{epoch_id}.private").exists()


def test_recovery_mint_removes_partial_private_output_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public_path = tmp_path / "recovery.remr"
    private_path = tmp_path / "recovery.remp"

    def fail_fdopen(fd: int, mode: str) -> object:
        del mode
        os.close(fd)
        raise OSError("injected write failure")

    monkeypatch.setattr("sutradhara.keys.registry.os.fdopen", fail_fdopen)
    with pytest.raises(OSError, match="injected write failure"):
        mint_recovery_keypair(
            public_key_path=public_path,
            private_key_path=private_path,
        )

    assert not private_path.exists()
    assert not public_path.exists()


def test_recovery_mint_refuses_private_output_under_registry_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_dir = tmp_path / "registry"
    registry_dir.mkdir()
    monkeypatch.setenv("SUTRADHARA_KEY_REGISTRY_DIR", str(registry_dir))

    with pytest.raises(ValueError, match="outside registry_dir"):
        mint_recovery_keypair(
            public_key_path=tmp_path / "recovery.remr",
            private_key_path=registry_dir / "escrow.remp",
        )


def test_key_domain_assertion_rejects_cross_domain_use(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys", deterministic_test=True)
    archive = registry.create_epoch()
    hdcache = registry.create_epoch(domain=KEY_DOMAIN_HDCACHE)

    assert_key_epoch_domain(hdcache, KEY_DOMAIN_HDCACHE, context="hdcache fill")
    assert_key_epoch_domain(archive, KEY_DOMAIN_ARCHIVE, context="pool sealing")
    with pytest.raises(ValueError, match="requires archive key epochs"):
        assert_key_epoch_domain(hdcache, KEY_DOMAIN_ARCHIVE, context="pool sealing")
    with pytest.raises(ValueError, match="requires hdcache key epochs"):
        assert_key_epoch_domain(archive, KEY_DOMAIN_HDCACHE, context="hdcache fill")
