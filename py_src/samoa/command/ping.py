
import samoa.command
from samoa.core import protobuf

class Ping(samoa.command.Command):

    def __init__(self, value = ''):
        samoa.command.Command.__init__(self)
        self.value = value

    def _write_request(self, request, server):

        request.type = protobuf.CommandType.PING
        request.mutable_ping().value_len = len(self.value)

        server.start_request()

        if self.value:
            server.write_interface().queue_write(self.value)
        yield

    def _read_response(self, response, server):

        value = yield server.read_interface().read_data(
            response.ping.value_len)
        yield value

class PingHandler(samoa.command.CommandHandler):
    def _handle(self, client):

        req = client.get_request()

        value = yield client.read_interface().read_data(req.ping.value_len)

        resp = client.get_response()
        resp.type = req.type
        resp.mutable_ping().value_len = req.ping.value_len

        client.start_response()
        client.write_interface().queue_write(value)
        yield

