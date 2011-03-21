
import samoa.server.command_handler
from samoa.core import protobuf

import traceback

import logging
log = logging.getLogger('command')

class Command(samoa.server.command_handler.CommandHandler):

    def __init__(self):
        samoa.server.command_handler.CommandHandler.__init__(self)
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

    @classmethod
    def check_for_error(cls, server_response_proxy):

        resp = server_response_proxy.get_response()
        if resp.type == protobuf.CommandType.ERROR:
            err = resp.error
            server_response_proxy.finish_response()
            raise RuntimeError("%s" % err)

    @classmethod
    def handle(cls, client):
        try:
            yield cls._handle(client)
        except Exception, err:
            traceback.print_exc()
            log.exception('<caught by Command.handle()>')
            client.set_error(str(type(err)), repr(err), True)

        client.finish_response()
        yield

