
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class PeeredCluster(object):

    def __init__(self, common_fixture, server_names, random_seed = None):

        self.injectors = {}
        self.fixtures = {}

        # select the first server as the server other servers peer against
        root_fixture = None

        for name in server_names:

            module = TestModule()
            module.fixture = common_fixture.clone_peer()

            if root_fixture is None:
                root_fixture = module.fixture
            else:
                module.fixture.add_peer(
                    uuid = root_fixture.server_uuid,
                    port = root_fixture.server_port)

            self.injectors[name] = module.configure(getty.Injector())
            self.fixtures[name] = module.fixture

        # not accessible until start_server_contexts is called
        self.listeners = None
        self.contexts = None

    def start_server_contexts(self):

        self.listeners = {}
        self.contexts = {}

        for name, injector in self.injectors.iteritems():
            self.listeners[name] = injector.get_instance(Listener)
            self.contexts[name] = self.listeners[name].get_context()

    def stop_server_contexts(self):

        for context in self.contexts.values():
            context.get_tasklet_group().cancel_group()

    def get_connection(self, name):

        connection = yield Server.connect_to(
            self.listeners[name].get_address(), self.listeners[name].get_port())

        yield connection

    def schedule_request(self, name):

        connection = yield self.get_connection(name)
        yield (yield connection.schedule_request())

