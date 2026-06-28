"""Compatibility import path for the extracted Sutradhara receive core.

The canonical dependency-light implementation now lives in
`sutradhara_receive`. This module remains so older server and test imports from
`sutradhara.receive` continue to resolve while callers move to the standalone
receive package.
"""

from sutradhara_receive import *  # noqa: F403
from sutradhara_receive import __all__ as __all__
