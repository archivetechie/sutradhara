"""Built-in job handlers.

Importing this package registers every built-in handler (via the
`register_handler` decorator at module-import time).
"""

from __future__ import annotations

# Side-effect imports: each module's @register_handler decorator runs.
from sutradhara.jobs.handlers import copy as _copy  # noqa: F401
from sutradhara.jobs.handlers import restore as _restore  # noqa: F401
from sutradhara.jobs.handlers import verify as _verify  # noqa: F401
