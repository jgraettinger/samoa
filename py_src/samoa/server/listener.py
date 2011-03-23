
import _server

class Listener(_server.Listener):

    def __init__(self, server_model, context, protocol, listen_backlog = 5):

        _server.Listener.__init__(self, server_model.hostname,
            str(server_model.port), listen_backlog, context, protocol)

