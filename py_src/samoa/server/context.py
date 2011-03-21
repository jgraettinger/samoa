
import getty

import samoa.core
import samoa.model.meta
import samoa.model.table
import samoa.model.partition

import cluster_state
import _server

class Context(_server.Context):

    @getty.requires(
        proactor = samoa.core.Proactor,
        meta = samoa.model.meta.Meta,
        server_uuid = getty.Config('server_uuid'))
    def __init__(self, proactor, meta, server_uuid):
        super(Context, self).__init__(proactor)

        self._meta = meta
        session = self.new_session()

        model = session.query(samoa.model.server.Server).filter_by(
            uuid = server_uuid).one()

        self._uuid = model.uuid
        self._hostname = model.hostname
        self._port = model.port

        self._cluster_state = cluster_state.ClusterState(
            self, session, None)

    def new_session(self):
        return self._meta.new_session()

    def get_server_uuid(self):
        return self._uuid

    def get_server_hostname(self):
        return self._hostname

    def get_server_port(self):
        return self._port

    def get_cluster_state(self):
        return self._cluster_state

    def set_cluster_state(self, cluster_state):
        self._cluster_state = cluster_state
