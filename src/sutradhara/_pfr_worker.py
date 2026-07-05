"""Managed subprocess entry point for pfr_core scrape isolation.

The pfr-index job runs this module through Sutradhara's resource-control
wrapper. Keeping the entry point importable, instead of using ``python -c``,
lets pfr_core's multiprocessing ``spawn`` isolation bootstrap reliably.
"""

from __future__ import annotations

from sutradhara.pfr import _scrape_path_isolated_worker


def main() -> int:
    """Run the managed PFR scrape worker."""

    return _scrape_path_isolated_worker()


if __name__ == "__main__":
    raise SystemExit(main())
