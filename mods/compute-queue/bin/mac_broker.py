"""Controller-owned local macOS job cohort: windows_broker.LocalPool over mac_pool (flock heavy lock, process-tree
containment). The admission, cohort, drain and completion-proof logic is windows_broker's."""
from __future__ import annotations

import mac_pool
import windows_broker

BusyError = windows_broker.BusyError


class LocalPool(windows_broker.LocalPool):
    _pool = mac_pool
    _exclusive_lock = mac_pool.ExclusiveLock
