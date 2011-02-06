
import samoa.command
from samoa.core import protobuf

class Error(samoa.command.Command):

    def __init__(self, err_type = '', message = None, closing = True):
        samoa.command.Command.__init__(self)
        self.err_type = err_type
        self.message = message
        self.closing = closing

    def request(self, server):

        req_proxy = yield server.schedule_request()

        req = req_proxy.get_request()
        req.type = protobuf.CommandType.ERROR
        err = req.mutable_error()

        err.type = self.err_type
        err.closing = self.closing
        if self.message:
            err.message = self.message

        resp_proxy = yield req_proxy.finish_request()
        self.check_for_error(resp_proxy)

        raise RuntimeError("Shouldn't be here")

    @classmethod
    def _handle(cls, client):

        req_err = client.get_request().error
        resp_err = client.get_response().mutable_error()

        resp_err.type = req_err.type
        resp_err.closing = req_err.closing
        if req_err.message:
            resp_err.message = req_err.message
        yield

