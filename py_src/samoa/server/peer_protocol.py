
import getty
import samoa.core
import samoa.client
import samoa.command.table_state

class PeerProtocol(object):

    # Default period of 60 seconds
    PERIOD_MS = 60 * 1000

    @getty.requires(proactor = samoa.core.Proactor,
        conn_mgr = samoa.client.ConnectionManager)
    def __init__(self, proactor, conn_mgr):
        self.proactor = proactor
        self.conn_mgr = conn_mgr

    def run(self, peer_host, peer_port):
        self.proactor.spawn(self._protocol_iteration,
            (peer_host, peer_port))

    def _protocol_iteration(self, peer_host, peer_port): 

        # schedule the next iteration
        self.proactor.run_later(self._protocol_iteration,
            (peer_host, peer_port))

        srv = yield self.conn_mgr.get_connection(
            peer_host, peer_port)

        
