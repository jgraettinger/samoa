
from samoa.server.command_handler import CommandHandler

class PingHandler(CommandHandler):

    def handle(self, client):
        client.finish_response()
        yield

