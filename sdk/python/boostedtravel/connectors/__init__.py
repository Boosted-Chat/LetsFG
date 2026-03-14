"""
Airline connectors package — 58 production-grade airline integrations.

Path shim: connector source files use bare imports like ``from connectors.X``
and ``from models.X`` (shared with the standalone top-level directory).
We add the ``boostedtravel/`` package root to sys.path so those imports
resolve to the copies shipped inside this package.
"""

import os as _os
import sys as _sys

_pkg_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _pkg_root not in _sys.path:
    _sys.path.insert(0, _pkg_root)
