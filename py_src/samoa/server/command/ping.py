
from samoa.server.command_handler import CommandHandler

class PingHandler(CommandHandler):

    def handle(self, request_state):
        request_state.flush_client_response()
        yield

