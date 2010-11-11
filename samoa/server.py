
import getty
import gevent.socket as socket
import sqlalchemy
import samoa

import meta_db
import config

class Server(object):

    @getty.requires(
        port = config.Config('port'),
        meta_db = meta_db.MetaDB)
    def __init__(self, port, meta_db):

        self.peers = {}
        self.tables = {}

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
            samoa.remote_client.RemoteClient(self, client_sock)

