
import getty
import samoa.core
import samoa.coroutine
import samoa.client

class ConnectionManager(object):

    @getty.requires(proactor = samoa.core.Proactor)
    def __init__(self, proactor):

        self._proactor = proactor

        # Map of active connections
        # {remote-addr: samoa.client.Server}
        self._connections = {}

        # Coroutine futures waiting on the connection
        # {remote-addr: samoa.coroutine.Event}
        self._pending = {}

        return

    def get_connection(self, remote_host, remote_port):

        # Case 1: we have a connection
        key = (remote_host, remote_port)
        if key in self._connections:
            return samoa.coroutine.Future(self._connections[key])

        # Case 2: we're already connecting
        if key in self._pending:
            return self._pending.wait()

        # Case 3: spawn a coroutine to connect to the server
        self._proactor.spawn(self._connect_coro,
            (remote_host, remote_port))

        event = samoa.coroutine.Event()
        self._pending[key] = event 
        return event.wait()

    def notify_of_remote_close(self, server):

        key = (server.remote_host, server.remote_port)
        if key in self._connections and self._connections[key] is server:
            del self._connections[key]

        return

    def _connect_coro(self, remote_host, remote_port):

        key = (remote_host, remote_port)

        server, error = None, None
        try:
            server = yield samoa.client.Server.connect_to(
                self._proactor, remote_host, remote_port)

        except Exception, e:
            error = e

        event = self._pending[key]

        if error:
            event.on_error(error)
            return

        self._connections[key] = server
        event.on_result(server)
        return
