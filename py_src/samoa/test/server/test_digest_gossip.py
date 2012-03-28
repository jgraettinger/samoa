
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.server.listener import Listener
from samoa.server.peer_discovery import PeerDiscovery
from samoa.server.peer_set import PeerSet
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestDigestGossip(object):

    def setUp(self):

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['main', 'peer'])

        self.cluster.set_known_peer('peer', 'main')

        self.partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')

        # add other partitions
        for i in xrange(3):
            self.cluster.add_partition(self.table_uuid, 'peer')

        #self.cluster.start_server_contexts()
        #self.main_persister = self.cluster.persisters[self.partition_uuid]

    def test_digest_gossip(self):
        pass
        #def test():
        #    self.main_persister.put(

