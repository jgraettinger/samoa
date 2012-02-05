
import getty
import uuid
import logging

from samoa.module import Module
from samoa.server.listener import Listener
from samoa.core.proactor import Proactor
from samoa.core.protobuf import ClusterState

class RuntimeModule(Module):
    def __init__(self):
        cluster_state = ClusterState()
        cluster_state.set_local_uuid(str(uuid.uuid4()))
        cluster_state.set_local_hostname('localhost')
        cluster_state.set_local_port(4572)

        Module.__init__(self, cluster_state)

    def configure(self, binder):
        binder = Module.configure(self, binder)

        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger('samoa')
        binder.bind_instance(logging.Logger, logger)
        return binder


module = RuntimeModule()
injector = module.configure(getty.Injector())

listener = injector.get_instance(Listener)

proactor = Proactor.get_proactor()
proactor.run()

