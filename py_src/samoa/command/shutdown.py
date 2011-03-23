
import samoa.command
from samoa.core import protobuf

class Shutdown(samoa.command.Command):

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.SHUTDOWN
        yield

    def _read_response(self, response, server):
        yield

class ShutdownHandler(samoa.command.CommandHandler):
    def _handle(self, client):
        client.get_context().get_proactor().shutdown()
        client.get_response().closing = True
        yield
