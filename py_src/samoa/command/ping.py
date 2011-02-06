
import samoa.command
from samoa.core import protobuf

class Ping(samoa.command.Command):

    def __init__(self, value = ''):
        samoa.command.Command.__init__(self)
        self.value = value

    def request(self, server):

        req_proxy = yield server.schedule_request()

        req = req_proxy.get_request()
        req.type = protobuf.CommandType.PING
        req.mutable_ping().value_len = len(self.value)

        req_proxy.start_request()

        if self.value:
            req_proxy.write_interface().queue_write(self.value)

        resp_proxy = yield req_proxy.finish_request()

        self.check_for_error(resp_proxy)
        ping = resp_proxy.get_response().ping

        value = yield resp_proxy.read_interface().read_data(ping.value_len)
        yield value

    @classmethod
    def _handle(cls, client):

        req = client.get_request()

        value = yield client.read_interface().read_data(req.ping.value_len)

        resp = client.get_response()
        resp.type = req.type
        resp.mutable_ping().value_len = req.ping.value_len

        client.start_response()
        client.write_interface().queue_write(value)
        yield

