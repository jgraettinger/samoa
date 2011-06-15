
from samoa.module import Module
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestModule(Module):

    def __init__(self):
        self.fixture = ClusterStateFixture()
        Module.__init__(self, self.fixture.state)

    def configure(self, binder):
        binder = Module.configure(self, binder)

        binder.bind_instance(ClusterStateFixture, self.fixture)

        return binder

