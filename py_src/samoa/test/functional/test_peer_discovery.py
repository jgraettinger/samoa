
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestPeerDiscovery(unittest.TestCase):

    def test_peer_discovery(self):

        proactor = Proactor.get_proactor()

        injectors, fixtures = [], []

        # create injectors & inject fixtures for each of four servers
        for i in xrange(4):
            injectors.append(TestModule().configure(getty.Injector()))
            fixtures.append(injectors[-1].get_instance(ClusterStateFixture))

        tbl0 = UUID.from_name('tbl0')
        tbl1 = UUID.from_name('tbl1')

        # servers 0 & 2 know of tbl0 only
        fixtures[0].add_table(tbl0)
        fixtures[2].add_table(tbl0)

        # server 3 knows of tbl1 only
        fixtures[3].add_table(tbl1)

        # server 1 knows of both tbl0 & tbl1
        fixtures[1].add_table(tbl0)
        fixtures[1].add_table(tbl1)

        # note tbl0 & tbl1 share UUID & data-type across servers,
        #  but differ in name, lamport-timestamp, and replication factor

        # server 0 has three partitions in tbl0
        fixtures[0].add_local_partition(tbl0, UUID.from_name('s0_tbl0_p0'))
        fixtures[0].add_local_partition(tbl0, UUID.from_name('s0_tbl0_p1'))
        fixtures[0].add_local_partition(tbl0, UUID.from_name('s0_tbl0_p2'))

        # server 1 has one partition each in tbl0 & tbl1
        fixtures[1].add_local_partition(tbl0, UUID.from_name('s1_tbl0_p0'))
        fixtures[1].add_local_partition(tbl1, UUID.from_name('s1_tbl1_p1'))

        # server 2 has one partition on tbl0
        fixtures[2].add_local_partition(tbl0, UUID.from_name('s2_tbl0_p0'))

        # server 3 has one partition on tbl1
        fixtures[3].add_local_partition(tbl1, UUID.from_name('s3_tbl1_p0'))

        # server 0 has no known peers

        # server 1 knows of server 0
        fixtures[1].add_peer(uuid = fixtures[0].state.local_uuid,
            port = fixtures[0].state.local_port)

        # server 2 knows of server 1
        fixtures[2].add_peer(uuid = fixtures[1].state.local_uuid,
            port = fixtures[1].state.local_port)

        # server 3 knows of server 2
        fixtures[3].add_peer(uuid = fixtures[2].state.local_uuid,
            port = fixtures[2].state.local_port)

        # bootstrap server contexts
        contexts = [i.get_instance(Listener).get_context() for i in injectors]

        # schedule servers to stop 150ms from now
        for ctxt in contexts:
            proactor.run_later(ctxt.get_tasklet_group().cancel_group, 150)

        proactor.run()

        # verify all servers know of all peers, tables, & partitions
        for ctxt in contexts:
            table_set = ctxt.get_cluster_state().get_table_set()

            table = table_set.get_table(tbl0)
            self.assertTrue(table.get_partition(UUID.from_name('s0_tbl0_p0')))
            self.assertTrue(table.get_partition(UUID.from_name('s0_tbl0_p1')))
            self.assertTrue(table.get_partition(UUID.from_name('s0_tbl0_p2')))
            self.assertTrue(table.get_partition(UUID.from_name('s1_tbl0_p0')))
            self.assertTrue(table.get_partition(UUID.from_name('s2_tbl0_p0')))

            table = table_set.get_table(tbl1)
            self.assertTrue(table.get_partition(UUID.from_name('s1_tbl1_p1')))
            self.assertTrue(table.get_partition(UUID.from_name('s3_tbl1_p0')))

            self.assertEquals(3,
                len(ctxt.get_cluster_state().get_protobuf_description().peer))

        return

