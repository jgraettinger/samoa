
import threading
import getty

from sqlalchemy.sql import expression as sql_exp

import samoa.core
import samoa.client.server_pool
import samoa.model.meta
import samoa.model.server
import samoa.model.partition

from peer import Peer

class PeerSet(samoa.client.server_pool.ServerPool):

    def __init__(self, context, session, prev_set):
        samoa.client.server_pool.ServerPool.__init__(
            self, context.get_proactor())

        self._peers = {}
        # Pull peer state from database
        for model in session.query(samoa.model.Server):
            if model.uuid == context.get_server_uuid():
                continue

            # Declare peer address to underlying ServerPool
            self.set_server_address(model.uuid, model.hostname, model.port)

            # Reuse an existing connected server instance, where possible
            prev_server = prev_set and prev_set.get_server(model.uuid)
            if prev_server:
                self.set_connected_server(model.uuid, prev_server)

            self._peers[model.uuid] = Peer(model, context,
                prev_set and prev_set.get_peer(model.uuid))

    def get_peer(self, peer_uuid):
        return self._peers.get(peer_uuid)

    def get_peers(self):
        return self._peers.itervalues()

    def retire(self):
        for peer in self._peers.values():
            peer.retire()

