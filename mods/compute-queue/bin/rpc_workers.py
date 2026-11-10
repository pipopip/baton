"""Bounded RPC admission; worker threads leave as soon as their batch drains."""
from collections import deque
import sys
import threading


class Workers:
    def __init__(self, workers=4, outstanding=16, waits=2):
        self.limit, self.capacity, self.wait_limit = workers, outstanding, waits
        self.condition = threading.Condition()
        self.queue = deque()
        self.threads = set()
        self.pending = self.waiting = 0
        self.closed = False

    def submit(self, function, *args, long_wait=False):
        """False means the call was never queued or executed; callers report busy."""
        with self.condition:
            if self.closed or self.pending >= self.capacity or (long_wait and self.waiting >= self.wait_limit):
                return False
            entry = (function, args, long_wait)
            self.queue.append(entry)
            self.pending += 1
            self.waiting += int(long_wait)
            if len(self.threads) < self.limit:
                worker = threading.Thread(target=self._work, name='compute-queue-rpc', daemon=False)
                self.threads.add(worker)
                try:
                    worker.start()
                except RuntimeError:
                    self.threads.remove(worker)
                    self.queue.pop()  # Existing workers cannot dequeue while this lock is held.
                    self.pending -= 1
                    self.waiting -= int(long_wait)
                    return False
            return True

    def _work(self):
        try:
            while True:
                with self.condition:
                    if not self.queue:
                        # Remove atomically with the empty check so a new submit
                        # cannot mistake this departing thread for a live worker.
                        self.threads.discard(threading.current_thread())
                        return
                    function, args, long_wait = self.queue.popleft()
                try:
                    function(*args)
                except BaseException:
                    # The protocol handles tool errors itself. A failed response
                    # writer must not strand other admitted calls or replay this one.
                    print('compute-queue: RPC handler failed; call not replayed', file=sys.stderr)
                finally:
                    del function, args
                    with self.condition:
                        self.pending -= 1
                        self.waiting -= int(long_wait)
        finally:
            with self.condition:
                self.threads.discard(threading.current_thread())
                self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            while self.threads:
                self.condition.wait()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
