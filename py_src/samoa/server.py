"""
import getty
import gevent.socket as socket

import model
import runtime

class Server(object):

    @getty.requires(
        port = getty.Config('port'),
        tables = runtime.TableSet,
        peers = runtime.PeerPool,
        meta_db = model.Meta)
    def __init__(self, port, tables, peers, meta_db):

        self.tables = tables
        self.peers = peers

        self.srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv_sock.bind(('', port))
        self.srv_sock.listen(1)
        return

    def close(self):

        if self.srv_sock:
            self.srv_sock, t = None, self.srv_sock
            t.close()
        return

    def service_loop(self):

        while self.srv_sock:

            client_sock, _ = self.srv_sock.accept()
            runtime.RemoteClient(self, client_sock)
"""
