
import samoa.server
from samoa.core import protobuf

import traceback

import logging
log = logging.getLogger('command')

class Command(samoa.server.CommandHandler):

    @classmethod
    def check_for_error(cls, server_response_proxy):

        resp = server_response_proxy.get_response()
        if resp.type == protobuf.CommandType.ERROR:
            raise RuntimeError("%s" % resp.error)

    @classmethod
    def handle(cls, client):
        try:
            yield cls._handle(client)
        except Exception, err:
            traceback.print_exc()
            log.exception('<caught by Command.handle()>')
            client.set_error(str(type(err)), repr(err), True)

        client.finish_response()
