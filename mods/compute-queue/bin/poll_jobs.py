"""Bounded, metadata-only cache for a controller's settled job files.

Active jobs and outstanding notices are always read fresh. Every filename is
still visited so shard/duplicate validation and changes by older clients work.
This cache is not the job store and never changes a job or notification mark.
"""
from __future__ import annotations

import contextlib
import os
import stat
import threading
import time


class SettledJobs:
    def __init__(self, read_json, terminal, limit=8192, refresh_after=300, clock=time.monotonic):
        self.read_json = read_json
        self.terminal = terminal
        self.limit = limit
        self.refresh_after = refresh_after
        self.clock = clock
        self.cache = {}
        self.lock = threading.Lock()
        self.seen = None

    @contextlib.contextmanager
    def generation(self):
        with self.lock:
            self.seen = set()
            try:
                yield
            finally:
                # Removed files and hosts cannot accumulate tombstones forever.
                for path in self.cache.keys() - self.seen:
                    del self.cache[path]
                self.seen = None

    @staticmethod
    def signature(info):
        return info.st_mtime_ns, info.st_ctime_ns, info.st_size

    def settled(self, job):
        return (job.get('status') in self.terminal and
                (job.get('parent') or job.get('notified') or not job.get('notify', True)))

    def scan(self, ctx):
        """Yield (job id, actionable job or None), including IDs of settled files."""
        if self.seen is None:
            raise RuntimeError('poll scan requires a generation')
        try:
            entries = os.scandir(ctx.p('jobs'))
        except FileNotFoundError:
            return
        with entries:
            for entry in entries:
                if not entry.name.endswith('.json'):
                    continue
                path = entry.path
                try:
                    info = entry.stat(follow_symlinks=False)
                    stamp = self.signature(info) if stat.S_ISREG(info.st_mode) else None
                except OSError:
                    stamp = None  # Never hide work because a metadata query failed.
                previous = self.cache.get(path)
                now = self.clock()
                if (previous is not None and stamp is not None and previous[0] == stamp
                        and 0 <= now - previous[2] < self.refresh_after):
                    self.seen.add(path)
                    yield previous[1], None
                    continue
                self.cache.pop(path, None)
                job = self.read_json(path)
                if not isinstance(job, dict) or 'id' not in job:
                    continue
                if not self.settled(job):
                    yield job['id'], job
                    continue
                # Check the path afresh: DirEntry.stat caches its first result,
                # and a writer may have replaced the file during read_json.
                try:
                    stable = stamp is not None and stamp == self.signature(os.stat(path, follow_symlinks=False))
                except OSError:
                    stable = False
                if stable and len(self.cache) < self.limit:
                    # Keep no job body, log, command or artifact list here.
                    self.cache[path] = (stamp, job['id'], now)
                    self.seen.add(path)
                yield job['id'], None
