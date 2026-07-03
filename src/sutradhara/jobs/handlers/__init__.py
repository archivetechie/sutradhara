"""Built-in job handlers.

Importing this package registers every built-in handler (via the
`register_handler` decorator at module-import time).
"""

from __future__ import annotations

# Side-effect imports: each module's @register_handler decorator runs.
from sutradhara.jobs.handlers import cloud_blob as _cloud_blob  # noqa: F401
from sutradhara.jobs.handlers import copy as _copy  # noqa: F401
from sutradhara.jobs.handlers import hdcache_fill as _hdcache_fill  # noqa: F401
from sutradhara.jobs.handlers import pfr_index as _pfr_index  # noqa: F401
from sutradhara.jobs.handlers import restore as _restore  # noqa: F401
from sutradhara.jobs.handlers import transcode as _transcode  # noqa: F401
from sutradhara.jobs.handlers import validate as _validate  # noqa: F401
from sutradhara.jobs.handlers import verify as _verify  # noqa: F401
