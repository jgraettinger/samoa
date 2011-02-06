
import samoa.command
from samoa.core import protobuf

class Shutdown(samoa.command.Command):

    def request(self, server):

        req_proxy = yield server.schedule_request()

        req = req_proxy.get_request()
        req.type = protobuf.CommandType.SHUTDOWN

        resp_proxy = yield req_proxy.finish_request()
        self.check_for_error(resp_proxy)
        yield

    @classmethod
    def _handle(cls, client):
        client.get_context().get_proactor().shutdown()
        yield

