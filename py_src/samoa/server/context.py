
import getty
import samoa.core.protobuf

import _server

class Context(_server.Context):

    @getty.requires(
        cluster_state = samoa.core.protobuf.ClusterState)
    def __init__(self, cluster_state):
        _server.Context.__init__(self, cluster_state)
        self.spawn_tasklets()

