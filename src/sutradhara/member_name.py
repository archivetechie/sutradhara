"""Compatibility module alias for `sutradhara_receive.member_name`.

Member-name escaping is part of the receive/archive path contract. The
dependency-light implementation lives with the extracted receive package; this
module keeps the historical `sutradhara.member_name` import path working.
"""

from __future__ import annotations

import sys

from sutradhara_receive import member_name as _member_name

sys.modules[__name__] = _member_name
