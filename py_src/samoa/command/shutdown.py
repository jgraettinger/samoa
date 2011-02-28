
import samoa.command
from samoa.core import protobuf

class Shutdown(samoa.command.Command):

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.SHUTDOWN
        yield

    def _read_response(self, response, server):
        yield

    @classmethod
    def _handle(cls, client):
        client.get_context().get_proactor().shutdown()
        client.get_response().closing = True
        yield
