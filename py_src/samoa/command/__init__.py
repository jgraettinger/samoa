
import samoa.server.command_handler
from samoa.core import protobuf

import traceback

class Command(object):

    def request_of(self, server_pool, server_uuid):
        pass

    def request(self, server):
        pass

    def check_for_error(self, server_response_proxy):
        pass

class CommandHandler(samoa.server.command_handler.CommandHandler):
    pass

"""
    def handle(self, client):
        try:
            yield self._handle(client)
        except Exception, err:
            traceback.print_exc()
            client.get_context().log.exception('<caught by Command.handle()>')
            client.set_error(str(type(err)), repr(err), True)

        client.finish_response()
        yield
"""
