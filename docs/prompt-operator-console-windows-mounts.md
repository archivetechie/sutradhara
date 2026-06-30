# Codex prompt — operator console: **Windows drive enumeration (helper)**

> Status: **pending**. Gap-fill for `prompt-operator-console-sutra-agent.md` (implemented):
> the helper's mount watcher has a macOS (`_darwin_mounts`) and a Linux
> (`_posix_mounts`, `/proc/mounts`) path but **no Windows path** — on Windows
> `current_mounts()` falls through to `_posix_mounts()`, finds no `/proc/mounts`, and
> advertises **zero cards**, so the browser console shows nothing for a Windows
> workstation. The design (`docs/design-operator-console-relay.md`) claims Mac **and**
> Windows; this closes the Windows half. **Scope: `packages/sutra-agent/src/sutra_agent/
> mounts.py` + tests only.** Per `AGENTS.md`: `uv run pytest -q` green + commit.

## The contract (do not change — already in `mounts.py`)
`_windows_mounts()` must return `list[MountInfo]`; `card_from_mount` already turns each
into a `MountedCard`:
- `MountInfo(mount_path, label, source?, volume_uuid?, removable, size_bytes?)`.
- `card_from_mount`: `card_id = f"volume:{volume_uuid or _stable_volume_id(info)}"`,
  `kind = card if removable else drive`, `status = available if mount_path.exists()`.
So Windows work is purely **producing correct `MountInfo`s**; the opaque `card_id`, the
`card`/`drive` kind, and the snapshot machinery are unchanged.

## Work

1. **`current_mounts()` dispatch** — add the Windows branch:
   ```python
   system = platform.system()
   if system == "Darwin": return _darwin_mounts()
   if system == "Windows": return _windows_mounts()
   return _posix_mounts()
   ```

2. **`_windows_mounts() -> list[MountInfo]`** via stdlib **`ctypes` + `kernel32`** — **no
   new dependency** (do NOT add `pywin32`; keep the edge package light):
   - `GetLogicalDrives()` → the bitmask of present drive letters (`A:\`…`Z:\`).
   - For each present letter, `GetDriveTypeW(r"D:\")` → classify:
     - `DRIVE_REMOVABLE (2)` → include, `removable=True` → **card**.
     - `DRIVE_FIXED (3)` → include **unless it is the system volume** (the drive holding
       `%SystemRoot%` / `os.environ["SystemDrive"]`), `removable=False` → **drive** (so
       Thunderbolt/USB SSDs, which present as fixed on Windows, are pickable).
     - `DRIVE_REMOTE (4)`, `DRIVE_CDROM (5)`, `DRIVE_NO_ROOT_DIR (1)`, `DRIVE_UNKNOWN (0)`
       → **exclude**.
   - `GetVolumeInformationW(r"D:\", …)` → the **volume label** and the **volume serial
     number** (32-bit). Use the serial (hex, e.g. `"1A2B-3C4D"`) as `volume_uuid` so
     `card_id` is **stable per volume** across re-inserts (the Windows analogue of the
     Linux blkid UUID / Mac `VolumeUUID`). If the call fails (e.g. a card-reader with no
     media), skip that drive.
   - `mount_path = Path("D:\\")`; `label = volume_label or "D:"`; `source = "D:"`;
     `size_bytes` left `None` (`card_from_mount` fills it via `shutil.disk_usage`).
   - **Over-inclusion is acceptable for v1** (an operator picks the right drive from the
     list); a future hardening could filter true-external via `STORAGE_BUS_TYPE`
     (`DeviceIoControl`) — note it, don't build it.

3. **Make the Win32 calls mockable** — factor them behind tiny helpers
   (`_win_logical_drive_letters() -> list[str]`, `_win_drive_type(letter) -> int`,
   `_win_volume_info(letter) -> tuple[label, serial] | None`) so tests monkeypatch them on
   any OS (the real `ctypes` path runs only on Windows).

## Tests (`tests/test_mounts_windows.py` — run on the Linux CI by mocking the seams)
- `current_mounts()` with `platform.system` monkeypatched to `"Windows"` calls
  `_windows_mounts()`.
- A `DRIVE_REMOVABLE` letter → `MountInfo(removable=True)` → `card_from_mount` → `kind=card`,
  `card_id = "volume:<serial>"` (stable across two calls with the same serial).
- A `DRIVE_FIXED` non-system letter → `kind=drive`; **the system drive
  (`SystemDrive`) is excluded**; `DRIVE_REMOTE`/`DRIVE_CDROM`/`DRIVE_NO_ROOT_DIR` excluded.
- A drive whose `_win_volume_info` returns `None` (no media) is skipped.
- The **mount path (`D:\`) never appears in `card_from_mount`'s outbound `MountedCard`
  beyond the opaque `card_id`/label** (the path stays local) — reuse the existing
  no-path-on-the-wire assertion style.

## Definition of done
`uv run pytest -q` green (the mocked Windows tests run on Linux); `current_mounts()`
enumerates removable + non-system fixed drives on Windows with stable serial-based
`card_id`s; no new dependency; `mounts.py` is the only non-test file changed. **Real
Win32 behavior is verified by the operator on the Windows laptop** (note this in the
commit) — the CI tests mock the kernel32 seams. INDEX updated.
