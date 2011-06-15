
import getty
import logging

import samoa.core.protobuf
import samoa.server.context
import samoa.server.listener

class Module(object):

    def __init__(self, cluster_state):
        self.cluster_state = cluster_state

    def configure(self, binder):

        logging.basicConfig(level = logging.INFO)

        binder.bind_instance(samoa.core.protobuf.ClusterState,
            self.cluster_state)

        binder.bind(samoa.server.context.Context, scope = getty.Singleton)
        binder.bind(samoa.server.listener.Listener, scope = getty.Singleton)

        return binder

