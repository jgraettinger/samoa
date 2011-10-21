
class StateException(RuntimeError):

    def __init__(self, code, message):
        RuntimeError.__init__(self, message)
        self.code = code

    def __repr__(self):
        return "StateException(%d, %r)" % (self.code, self.message)

import _request
_request._set_state_exception_python_class(StateException)

