
from _coroutine import Future

#TODO(johng): Need to protect w/ a threading.Lock

class Event(object):

    def __init__(self):

        self._exception = None
        self._result = None

        self._pending = []

    def wait(self):

        future = Future()

        if self._result:
            future.on_result(self._result)
        elif self._exception:
            future.on_error(
                type(self._exception), self._exception)
        else:
            self._pending.append(future)

        return future

    def on_result(self, result):
        assert not (self._result or self._exception), "Already called"

        self._result = result

        pending = self._pending
        self._pending = None

        for future in pending:
            future.on_result(self._result)

    def on_error(self, exception):
        assert not (self._result or self._exception), "Already called"

        self._exception = exception

        pending = self._pending
        self._pending = None

        for future in pending:
            future.on_error(
                type(self._exception), self._exception)

