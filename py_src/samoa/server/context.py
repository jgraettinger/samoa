
import getty

import samoa.core
import samoa.model

import table_set
#import peer_protocol
import _server

class Context(_server.Context):

    @getty.requires(
        proactor = samoa.core.Proactor,
        table_set = table_set.TableSet,
        meta = samoa.model.Meta)
    def __init__(self, proactor, table_set, meta):
        super(Context, self).__init__(proactor)

        self.table_set = table_set
        self.meta = meta

        # collect set of remote peers
        self._peers = {}
        for table in table_set.tables:
            for remote in table.remote_partitions:
                self._peers[(remote.host, remote.port)] = None

        # initialize peer protocol for each remote peer
        for peer_addr in self._peers.keys():

            proto = peer_protocol.PeerProtocol(self, *peer_addr)
            self._peers[peer_addr] = proto

            # schedule the protocol's main coroutine
            proactor.spawn(proto.run)

        return

