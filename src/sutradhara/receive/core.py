"""Compatibility module alias for `sutradhara_receive.core`.

The receive core is dependency-light code shared by the edge receive CLI and
server-side intake validation. Keeping this module as an alias preserves legacy
imports and test monkeypatching while avoiding a second implementation.
"""

from __future__ import annotations

import sys

from sutradhara_receive import core as _core

sys.modules[__name__] = _core
