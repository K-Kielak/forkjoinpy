"""
Used primarily for debugging/logging purposes
"""
from threading import Lock


class _GlobalCounter:
    def __init__(self):
        self._counter = 0
        self._lock = Lock()

    def get_new(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter


counter = _GlobalCounter()
