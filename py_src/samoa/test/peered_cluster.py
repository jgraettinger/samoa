
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
        self.partition_uuids = {}

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
        self.rolling_hashes = None

    def add_partition(self, table_uuid, server_name):

        part_uuid = self.fixtures[name].add_local_partition(table_uuid).uuid

        self.partition_uuids.setdefault(name, []).append(
            (table_uuid, part_uuid))
        return part_uuid

    def start_server_contexts(self):

        self.listeners = {}
        self.contexts = {}
        self.rolling_hashes = {}

        for name, injector in self.injectors.iteritems():
            self.listeners[name] = injector.get_instance(Listener)

            context = self.listeners[name].get_context()
            self.contexts[name] = context

            if name not in self.partition_uuids:
                continue

            for tbl_uuid, part_uuid in self.partition_uuids[name]:

                self.rolling_hashes[name] = self.context.get_cluster_state(
                    ).get_table_set(
                    ).get_table(tbl_uuid
                    ).get_partition(part_uuid
                    ).get_persister(
                    ).get_layer(0)

    def stop_server_contexts(self):

        for context in self.contexts.values():
            context.get_tasklet_group().cancel_group()

    def get_connection(self, name):

        connection = yield Server.connect_to(
            self.listeners[name].get_address(), self.listeners[name].get_port())
        yield connection

    def schedule_request(self, name):

        connection = yield self.get_connection(name)
        request = yield connection.schedule_request()
        yield request

