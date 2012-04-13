
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class PeeredCluster(object):

    def __init__(self, common_fixture, server_names, set_peers = True):

        self.injectors = {}
        self.fixtures = {}
        self.partitions = []

        last_fixture = None

        for name in server_names:

            module = TestModule()
            module.fixture = common_fixture.clone_peer()

            self.injectors[name] = module.configure(getty.Injector())
            self.fixtures[name] = module.fixture

            if last_fixture and set_peers:
                self.fixtures[name].add_peer(
                    uuid = last_fixture.server_uuid,
                    port = last_fixture.server_port)

            last_fixture = self.fixtures[name]

        # not accessible until start_server_contexts is called
        self.listeners = None
        self.contexts = None
        self.persisters = None

    def set_known_peer(self, server_name, peer_name, seed = False):

        peer_uuid = self.fixtures[peer_name].server_uuid
        peer_port = self.fixtures[peer_name].server_port

        current = self.fixtures[server_name].get_peer(peer_uuid)

        if current:
            current.set_seed(seed)
        else:
            self.fixtures[server_name].add_peer(
                uuid = peer_uuid, port = peer_port, seed = seed)

    def add_partition(self, table_uuid, server_name):

        part_uuid = self.fixtures[server_name
            ].add_local_partition(table_uuid).uuid
        part_uuid = UUID(part_uuid)

        self.partitions.append((table_uuid, server_name, part_uuid))
        return part_uuid

    def start_server_contexts(self):

        self.listeners = {}
        self.contexts = {}

        for name, injector in self.injectors.iteritems():
            self.listeners[name] = injector.get_instance(Listener)

            context = self.listeners[name].get_context()
            self.contexts[name] = context

        self.persisters = {}
        for table_uuid, name, part_uuid in self.partitions:
            self.persisters[part_uuid] = self.contexts[name].get_cluster_state(
                ).get_table_set(
                ).get_table(table_uuid
                ).get_partition(part_uuid
                ).get_persister()

    def stop_server_contexts(self):

        for context in self.contexts.values():
            context.shutdown()

    def get_connection(self, name):

        connection = yield Server.connect_to(
            self.listeners[name].get_address(),
            self.listeners[name].get_port())
        yield connection

    def schedule_request(self, name):

        connection = yield self.get_connection(name)
        request = yield connection.schedule_request()
        yield request

