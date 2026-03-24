"""Thread/task-safe atomic counters for test stats."""

from collections import Counter
from threading import Lock


class Stats:
    def __init__(self):
        self._lock = Lock()
        self._counts = Counter()

    def incr(self, key, n=1):
        with self._lock:
            self._counts[key] += n

    def get(self, key):
        with self._lock:
            return self._counts[key]

    def __str__(self):
        with self._lock:
            return ", ".join(f"{k}: {v}" for k, v in sorted(self._counts.items()))
