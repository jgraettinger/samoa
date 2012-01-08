
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.server.peer_discovery import PeerDiscovery
from samoa.server.peer_set import PeerSet
from samoa.client.server import Server

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestPeerDiscovery(unittest.TestCase):

    def setUp(self):
        PeerSet.set_discovery_enabled(False)

    def tearDown(self):
        PeerSet.set_discovery_enabled(True)

    def test_basic(self):

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table().uuid)

        cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B'])

        cluster.set_known_peer('peer_A', 'peer_B')

        # add divergent partitions
        part_A = cluster.add_partition(table_uuid, 'peer_A')
        part_B = cluster.add_partition(table_uuid, 'peer_B')

        cluster.start_server_contexts()

        def test():

            table = lambda name: cluster.contexts[name
                ].get_cluster_state(
                ).get_table_set(
                ).get_table(table_uuid)

            self.assertFalse(table('peer_A').get_partition(part_B))
            self.assertFalse(table('peer_B').get_partition(part_A))

            yield PeerDiscovery(cluster.contexts['peer_A'],
                cluster.contexts['peer_B'].get_server_uuid())()

            self.assertTrue(table('peer_A').get_partition(part_B))
            self.assertTrue(table('peer_B').get_partition(part_A))

            cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_extended(self):

        cluster = PeeredCluster(ClusterStateFixture(),
            server_names = ['peer_A', 'peer_B', 'peer_C', 'peer_D'])

        cluster.set_known_peer('peer_D', 'peer_C')
        cluster.set_known_peer('peer_C', 'peer_B')
        cluster.set_known_peer('peer_B', 'peer_A')

        fixture_A = cluster.fixtures['peer_A']
        fixture_B = cluster.fixtures['peer_B']
        fixture_C = cluster.fixtures['peer_C']
        fixture_D = cluster.fixtures['peer_D']

        tbl0 = UUID.from_random()
        tbl1 = UUID.from_random()

        # peers A & C know of tbl0 only
        fixture_A.add_table(tbl0)
        fixture_C.add_table(tbl0)

        # peer B knows of tbl0 & tbl1
        fixture_B.add_table(tbl0)
        fixture_B.add_table(tbl1)

		# peer D knows of tbl1 only
        fixture_D.add_table(tbl1)

        # note tbl0 & tbl1 share UUID across servers, but differ
        #  in name, lamport-timestamp, and replication factor

        # peer A has three partitions in tbl0
        pA_tbl0_p0 = UUID(fixture_A.add_local_partition(tbl0).uuid)
        pA_tbl0_p1 = UUID(fixture_A.add_local_partition(tbl0).uuid)
        pA_tbl0_p2 = UUID(fixture_A.add_local_partition(tbl0).uuid)

        # peer B has one partition each in tbl0 & tbl1
        pB_tbl0_p0 = UUID(fixture_B.add_local_partition(tbl0).uuid)
        pB_tbl1_p0 = UUID(fixture_B.add_local_partition(tbl1).uuid)

        # peer C has one partition on tbl0
        pC_tbl0_p0 = UUID(fixture_C.add_local_partition(tbl0).uuid)

        # peer D has one partition on tbl1
        pD_tbl1_p0 = UUID(fixture_D.add_local_partition(tbl1).uuid)

        cluster.start_server_contexts()

        def test():

            table = lambda name, tbl_uuid: cluster.contexts[name
                ].get_cluster_state(
                ).get_table_set(
                ).get_table(tbl_uuid)

            # peer B discovers from A
            yield PeerDiscovery(cluster.contexts['peer_B'],
                cluster.contexts['peer_A'].get_server_uuid())()

            # peer_B knows of peer_A's partitions
            self.assertTrue(table('peer_B', tbl0).get_partition(pA_tbl0_p0))
            self.assertTrue(table('peer_B', tbl0).get_partition(pA_tbl0_p1))
            self.assertTrue(table('peer_B', tbl0).get_partition(pA_tbl0_p2))

            # peer_A knows of tbl1
            self.assertTrue(table('peer_A', tbl1))

            # peer_A knows of peer_B's partitions
            self.assertTrue(table('peer_A', tbl0).get_partition(pB_tbl0_p0))
            self.assertTrue(table('peer_A', tbl1).get_partition(pB_tbl1_p0))

            # peer C discovers from B
            yield PeerDiscovery(cluster.contexts['peer_C'],
                cluster.contexts['peer_B'].get_server_uuid())()

            # peer_B knows of peer_C's partition
            self.assertTrue(table('peer_C', tbl0).get_partition(pC_tbl0_p0))

            # peer_C knows of tbl1
            self.assertTrue(table('peer_C', tbl1))

            # peer_C knows of peer_A's partitions
            self.assertTrue(table('peer_C', tbl0).get_partition(pA_tbl0_p0))
            self.assertTrue(table('peer_C', tbl0).get_partition(pA_tbl0_p1))
            self.assertTrue(table('peer_C', tbl0).get_partition(pA_tbl0_p2))

            # peer_C knows of peer_B's partitions
            self.assertTrue(table('peer_C', tbl0).get_partition(pB_tbl0_p0))
            self.assertTrue(table('peer_C', tbl1).get_partition(pB_tbl1_p0))

            # peer D discovers from C
            yield PeerDiscovery(cluster.contexts['peer_D'],
                cluster.contexts['peer_C'].get_server_uuid())()

            # peer_C knows of peer_D's partition 
            self.assertTrue(table('peer_C', tbl1).get_partition(pD_tbl1_p0))

            # peer_D knows of tbl0
            self.assertTrue(table('peer_D', tbl0))

            # peer_D knows of peer_A's partitions
            self.assertTrue(table('peer_D', tbl0).get_partition(pA_tbl0_p0))
            self.assertTrue(table('peer_D', tbl0).get_partition(pA_tbl0_p1))
            self.assertTrue(table('peer_D', tbl0).get_partition(pA_tbl0_p2))

            # peer_D knows of peer_B's partitions
            self.assertTrue(table('peer_D', tbl0).get_partition(pB_tbl0_p0))
            self.assertTrue(table('peer_D', tbl1).get_partition(pB_tbl1_p0))

            # peer_D knows of peer_C's partition
            self.assertTrue(table('peer_D', tbl0).get_partition(pC_tbl0_p0))

            # peer_C knows of peer_D's partition
            self.assertTrue(table('peer_C', tbl1).get_partition(pD_tbl1_p0))

            # At this point, A doesn't know of C or D, and B doesn't know of D
            self.assertFalse(table('peer_A', tbl0).get_partition(pC_tbl0_p0))
            self.assertFalse(table('peer_A', tbl1).get_partition(pD_tbl1_p0))

            self.assertFalse(table('peer_B', tbl1).get_partition(pD_tbl1_p0))

            # Additional discovery propagates C to A, and D to B
            yield PeerDiscovery(cluster.contexts['peer_B'],
                cluster.contexts['peer_A'].get_server_uuid())()
            yield PeerDiscovery(cluster.contexts['peer_C'],
                cluster.contexts['peer_B'].get_server_uuid())()

            # Now A knows of C but not D; B knows of D
            self.assertTrue(table('peer_A', tbl0).get_partition(pC_tbl0_p0))
            self.assertFalse(table('peer_A', tbl1).get_partition(pD_tbl1_p0))

            self.assertTrue(table('peer_B', tbl1).get_partition(pD_tbl1_p0))

            # Additional discovery propagates D to A
            yield PeerDiscovery(cluster.contexts['peer_B'],
                cluster.contexts['peer_A'].get_server_uuid())()

            self.assertTrue(table('peer_A', tbl1).get_partition(pD_tbl1_p0))

            cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

