
import getty
import logging

import samoa.core
import samoa.coroutine.lock
import samoa.model.meta
import samoa.model.table
import samoa.model.partition

import cluster_state
import listener
import protocol

import _server

class Context(_server.Context):

    @getty.requires(
        proactor = samoa.core.Proactor,
        meta = samoa.model.meta.Meta,
        protocol = protocol.Protocol,
        server_uuid = getty.Config('server_uuid'))
    def __init__(self, proactor, meta, protocol, server_uuid):
        super(Context, self).__init__(proactor)

        self._meta = meta
        session = self._meta.new_session()

        model = session.query(samoa.model.server.Server).filter_by(
            uuid = server_uuid).one()

        self._uuid = model.uuid
        self._hostname = model.hostname
        self._port = model.port

        self._cluster_state_lock = samoa.coroutine.lock.Lock()
        self._cluster_state = cluster_state.ClusterState(
            self, session, None)

        self._listener = listener.Listener(model, self, protocol)

        self.log = logging.getLogger('samoa')

    def get_server_uuid(self):
        return self._uuid

    def get_server_hostname(self):
        return self._hostname

    def get_server_port(self):
        return self._port

    def get_listener(self):
        return self._listener

    def get_cluster_state(self):
        return self._cluster_state

    def get_table(self, table_uuid):
        return self._cluster_state.get_table(table_uuid)

    def get_table_by_name(self, table_name):
        return self._cluster_state.get_table_by_name(table_name)

    def cluster_state_transaction(self, callback, *args, **kwargs):

        yield self._cluster_state_lock.aquire()
        session = self._meta.new_session()

        try:
            is_dirty, notify_peers = yield callback(self,
                session, *args, **kwargs)
            session.flush()
        except:
            self.log.exception("<in cluster_state_transaction>:")
            is_dirty = False

        if not is_dirty:
            session.rollback()
            self._cluster_state_lock.release()
            yield

        # build next cluster-state
        state = cluster_state.ClusterState(
            self, session, self._cluster_state)

        # Commit transaction, updating exposed ClusterState instance
        session.commit()
        self._cluster_state = state
        self._cluster_state_lock.release()

        # Optionally notify peers of a model change
        if notify_peers:
            for peer in state.get_peer_set().get_peers():
                peer.poll_cluster_state(self)
        yield

