
import samoa.server.command_handler
from samoa.core import protobuf

import traceback

class Command(object):

    def __init__(self):
        self.closing = False

    def request_of(self, server_pool, server_uuid):

        req_proxy = yield server_pool.schedule_request(server_uuid)

        if self.closing:
            req_proxy.get_request().closing = True

        yield self._write_request(req_proxy.get_request(), req_proxy)

        resp_proxy = yield req_proxy.finish_request()

        resp = resp_proxy.get_response()

        if resp.type == protobuf.CommandType.ERROR:
            err = resp.error
            resp_proxy.finish_response()
            raise RuntimeError("%s" % err)

        result = yield self._read_response(resp, resp_proxy)
        resp_proxy.finish_response()
        yield result

    def request(self, server):

        req_proxy = yield server.schedule_request()

        if self.closing:
            req_proxy.get_request().closing = True

        yield self._write_request(req_proxy.get_request(), req_proxy)

        resp_proxy = yield req_proxy.finish_request()

        resp = resp_proxy.get_response()

        if resp.type == protobuf.CommandType.ERROR:
            err = resp.error
            resp_proxy.finish_response()
            raise RuntimeError("%s" % err)

        result = yield self._read_response(resp, resp_proxy)
        resp_proxy.finish_response()
        yield result

    def check_for_error(self, server_response_proxy):

        resp = server_response_proxy.get_response()
        if resp.type == protobuf.CommandType.ERROR:
            err = resp.error
            server_response_proxy.finish_response()
            raise RuntimeError("%s" % err)

class CommandHandler(samoa.server.command_handler.CommandHandler):

    def handle(self, client):
        try:
            yield self._handle(client)
        except Exception, err:
            traceback.print_exc()
            client.get_context().log.exception('<caught by Command.handle()>')
            client.set_error(str(type(err)), repr(err), True)

        client.finish_response()
        yield

