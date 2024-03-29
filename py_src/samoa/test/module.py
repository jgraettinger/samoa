
import logging

from samoa.module import Module
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestModule(Module):

    def __init__(self):
        self.fixture = ClusterStateFixture()
        Module.__init__(self, None)

    def configure(self, binder):
        self.cluster_state = self.fixture.state

        binder = Module.configure(self, binder)

        binder.bind_instance(ClusterStateFixture, self.fixture)

        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger('samoa')
        binder.bind_instance(logging.Logger, logger)

        return binder

