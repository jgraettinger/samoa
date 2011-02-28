
import samoa.command
from samoa.core import protobuf

class Error(samoa.command.Command):

    def __init__(self, err_type = '', message = None):
        samoa.command.Command.__init__(self)
        self.err_type = err_type
        self.message = message

    def _write_request(self, request, server):

        request.type = protobuf.CommandType.ERROR
        err = request.mutable_error()

        err.type = self.err_type
        if self.message:
            err.message = self.message
        yield

    @classmethod
    def _handle(cls, client):

        request = client.get_request()
        response = client.get_response()

        req_err = client.get_request().error
        resp_err = client.get_response().mutable_error()

        resp_err.type = req_err.type
        if req_err.message:
            resp_err.message = req_err.message
        yield

