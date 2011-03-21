
import threading
from _coroutine import Future

class Lock(object):

    def __init__(self):

        self._lock = threading.Lock()

        self._locked = False
        self._pending = []

    def aquire(self):
        with self._lock:
            future = Future()

            if not self._locked:
                self._locked = True
                future.on_result(True)
            else:
                self._pending.append(future)

            return future

    def release(self):
        with self._lock:

            if self._pending:
                self._pending[0].on_result(True)
                del self._pending[0]
            else:
                self._locked = False

