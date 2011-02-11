
import getty
import samoa.core
import samoa.coroutine
import samoa.client

class ServerPool(object):

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

    def request(self, remote_host, remote_port, request):

        # Case 1: we have a connection
        key = (remote_host, str(remote_port))
        if key in self._connections:
            
            try:
                

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

        key = (remote_host, str(remote_port))
        event = self._pending[key]

        try:
            self._connections[key] = yield samoa.client.Server.connect_to(
                self._proactor, remote_host, str(remote_port))
            event.on_result(self._connections[key])

        except Exception, e:
            event.on_error(error)

