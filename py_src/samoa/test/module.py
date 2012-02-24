import os
import logging

from samoa.module import Module
from samoa.test.cluster_state_fixture import ClusterStateFixture

from samoa.server.digest import Digest

class TestModule(Module):

    def __init__(self):
        self.fixture = ClusterStateFixture()
        Module.__init__(self, None)

    def configure(self, binder):
        self.cluster_state = self.fixture.state

        digest_directory = "/tmp/samoa_test_%s" % \
            self.fixture.server_uuid.to_hex()
        os.makedirs(digest_directory)

        Digest.set_default_byte_length(1024)
        Digest.set_directory(digest_directory)

        binder = Module.configure(self, binder)

        binder.bind_instance(ClusterStateFixture, self.fixture)

        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger('samoa')
        binder.bind_instance(logging.Logger, logger)

        return binder

