
import getty
#from peer import Peer

class PeerPool(object):

    @getty.requires()
    def __init__(self):

        self._conns = {}
        return

    def get_remote_peer(self, host, port):
        key = (host, port)

        if key not in self._conns:
            self._conns[key] = RemoteServer(host, port)

        return self._conns[key]

