
import unittest

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.table import Table

from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestTable(unittest.TestCase):

    def setUp(self):

        self.gen = ClusterStateFixture()
        self.gen.add_table(name = 'table', uuid = UUID.from_name('table'))
        self.state = self.gen.state.table[0]

    def test_ctor_edge_cases(self):

        self.gen.add_local_partition(self.state.uuid)
        self.gen.add_remote_partition(self.state.uuid)
        self.gen.add_remote_partition(self.state.uuid
            ).set_dropped(True)

        # null hypothesis - should build
        Table(self.state, self.gen.server_uuid, None)

        # invalid partition order
        tst_state = pb.ClusterState_Table(self.state)
        tst_state.partition.SwapElements(1, 2)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            Table(tst_state, self.gen.server_uuid, None)

        # duplicate partition UUID
        tst_state = pb.ClusterState_Table(self.state)
        tst_state.partition[2].set_uuid(tst_state.partition[0].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            Table(tst_state, self.gen.server_uuid, None)

    def test_merge_edge_cases(self):

        pgen = self.gen.clone_peer()
        pstate = pgen.state.table[0]

        pgen.add_local_partition(pstate.uuid)
        pgen.add_local_partition(pstate.uuid)

        table = Table(self.state, self.gen.server_uuid, None)

        # null hypothesis - doesn't throw 
        table.merge_table(pstate, pb.ClusterState_Table(self.state))

        # invalid partition order
        tst_state = pb.ClusterState_Table(pstate)
        tst_state.partition.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            table.merge_table(tst_state, pb.ClusterState_Table(self.state))

        # remote partition w/ local server uuid
        tst_state = pb.ClusterState_Table(pstate)
        tst_state.partition[0].set_server_uuid(self.gen.server_uuid.to_hex())

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            table.merge_table(tst_state, pb.ClusterState_Table(self.state))

    def test_merge_lamport(self):

        self.state.set_name('test')
        self.state.set_replication_factor(2)
        self.state.set_consistency_horizon(300)
        self.state.set_lamport_ts(10)

        table = Table(self.state, self.gen.server_uuid, None)

        peer_state = pb.ClusterState_Table(self.state)

        # peer has smaller timestamp
        peer_state.set_name('new_name')
        peer_state.set_replication_factor(1)
        peer_state.set_consistency_horizon(200)
        peer_state.set_lamport_ts(1)

        # merge keeps local state
        out = pb.ClusterState_Table(self.state)
        table.merge_table(peer_state, out)

        self.assertEquals(out.name, 'test')
        self.assertEquals(out.replication_factor, 2)
        self.assertEquals(out.consistency_horizon, 300)
        self.assertEquals(out.lamport_ts, 10)

        # peer has larger timestamp
        peer_state.set_lamport_ts(12)
        
        # merge replaces local state
        out = pb.ClusterState_Table(self.state)
        table.merge_table(peer_state, out)

        self.assertEquals(out.name, 'new_name')
        self.assertEquals(out.replication_factor, 1)
        self.assertEquals(out.consistency_horizon, 200)
        self.assertEquals(out.lamport_ts, 12)

    def test_merge_extended(self):

        tbl_uuid = UUID.from_name('table')

        # common set of known partitions
        self.gen.add_local_partition(tbl_uuid,  uuid = UUID.from_name('p1'))
        self.gen.add_remote_partition(tbl_uuid, uuid = UUID.from_name('p2'))
        self.gen.add_remote_partition(tbl_uuid, uuid = UUID.from_name('p3'))
        self.gen.add_remote_partition(tbl_uuid, uuid = UUID.from_name('p4'))

        pgen = self.gen.clone_peer()
        pstate = pgen.state.table[0]

        # locally drop p1
        part = self.gen.get_partition(tbl_uuid, UUID.from_name('p1'))
        part.set_dropped(True)

        # locally update consistent range of p2
        part = self.gen.get_partition(tbl_uuid, UUID.from_name('p2'))
        part.set_lamport_ts(part.lamport_ts + 1)
        part.set_consistent_range_end(part.consistent_range_end + 100)
        p2_cre = part.consistent_range_end

        # peer drops p3
        part = pgen.get_partition(tbl_uuid, UUID.from_name('p3'))
        part.set_dropped(True)

        # peer updates consistent range of p4
        part = pgen.get_partition(tbl_uuid, UUID.from_name('p4'))
        part.set_lamport_ts(part.lamport_ts + 2)
        part.set_consistent_range_end(part.consistent_range_end + 200)
        p4_cre = part.consistent_range_end

        # additional partitions known only locally
        self.gen.add_local_partition(tbl_uuid)
        self.gen.add_remote_partition(tbl_uuid)
        self.gen.add_remote_partition(tbl_uuid)

        # additional partitions known only by peer
        pgen.add_remote_partition(tbl_uuid)
        pgen.add_remote_partition(tbl_uuid).set_dropped(True)
        pgen.add_remote_partition(tbl_uuid)


        table = Table(self.state, self.gen.server_uuid, None)

        # p1 is locally dropped
        self.assertNotIn(UUID.from_name('p1'),
            [p.get_uuid() for p in table.get_ring()])

        # p2 is locally updated
        self.assertEquals(table.get_partition(UUID.from_name('p2')
            ).get_consistent_range_end(), p2_cre)

        # p3 isn't yet dropped
        self.assertTrue(table.get_partition(UUID.from_name('p3')))

        # p4 isn't yet updated
        self.assertEquals(table.get_partition(UUID.from_name('p4')
            ).get_consistent_range_end(), p4_cre - 200)

        # MERGE from peer, & rebuild table
        out = pb.ClusterState_Table(self.state)
        self.assertTrue(table.merge_table(pstate, out))

        table = Table(out, self.gen.server_uuid, table)

        # ten partitions in total...
        self.assertEquals(len(out.partition), 10)
        #  ... but only seven are live
        self.assertEquals(len(table.get_ring()), 7)

        # p1 is still dropped
        self.assertNotIn(UUID.from_name('p1'),
            [p.get_uuid() for p in table.get_ring()])

        # p2 is still updated
        self.assertEquals(table.get_partition(UUID.from_name('p2')
            ).get_consistent_range_end(), p2_cre)

        # p3 is now dropped
        self.assertFalse(table.get_partition(UUID.from_name('p3')))

        # p4 is now updated
        self.assertEquals(table.get_partition(UUID.from_name('p4')
            ).get_consistent_range_end(), p4_cre)

        # run again, & no futher changes detected from peer
        out2 = pb.ClusterState_Table(out)
        self.assertFalse(table.merge_table(pstate, out2))

        self.assertEquals(out.SerializeToText(),
            out2.SerializeToText())

    def test_ring_order(self):

        tbl_uuid = UUID.from_name('table')
        self.gen.add_local_partition(tbl_uuid)
        self.gen.add_remote_partition(tbl_uuid)
        self.gen.add_local_partition(tbl_uuid)
        self.gen.add_remote_partition(tbl_uuid)
        self.gen.add_remote_partition(tbl_uuid)

        table = Table(self.state, self.gen.server_uuid, None)

        last_key = None

        # both protobuf partition descriptions & table.get_ring() are in
        #  'ring' order of (ring_position, uuid) ascending
        for pb_part, part in zip(self.state.partition, table.get_ring()):
            self.assertEquals(pb_part.ring_position, part.get_ring_position())
            self.assertEquals(pb_part.uuid, part.get_uuid().to_hex())

            key = (pb_part.ring_position, pb_part.uuid)
            self.assertTrue(last_key is None or last_key < key)
            last_key = key

    def test_runtime_partition_ranges(self):

        self.state.set_replication_factor(3)

        tbl_uuid = UUID.from_name('table')
        self.gen.add_remote_partition(tbl_uuid, ring_position = 0)
        self.gen.add_remote_partition(tbl_uuid, ring_position = 1000)
        self.gen.add_remote_partition(tbl_uuid, ring_position = 2000)
        self.gen.add_local_partition( tbl_uuid, ring_position = 3000)
        self.gen.add_remote_partition(tbl_uuid, ring_position = 4000)

        table = Table(self.state, self.gen.server_uuid, None)
        ring = table.get_ring()

        # validate ranges of each runtime partition
        self.assertEquals(ring[0].get_range_begin(), 4000)
        self.assertEquals(ring[0].get_range_end(), 1999)
        self.assertEquals(ring[1].get_range_begin(), 0)
        self.assertEquals(ring[1].get_range_end(), 2999)
        self.assertEquals(ring[2].get_range_begin(), 1000)
        self.assertEquals(ring[2].get_range_end(), 3999)
        self.assertEquals(ring[3].get_range_begin(), 2000)
        self.assertEquals(ring[3].get_range_end(), (1<<64) - 1)
        self.assertEquals(ring[4].get_range_begin(), 3000)
        self.assertEquals(ring[4].get_range_end(), 999)

    def test_is_neighbor(self):

        # arguments here are:
        #  local_partition_indicies, ring_size, ring_index, replication_factor

        # best way to follow along with these, is to print out the list of
        #  ordered partition indicies, and underline the local ones

        # 0 replicates forward to 1
        self.assertTrue(Table.is_neighbor([1, 4], 9, 0, 3))
        # 2 replicates back to 1, forward to 4
        self.assertTrue(Table.is_neighbor([1, 4], 9, 2, 3))
        # 3 replicates back to 1, forward to 4
        self.assertTrue(Table.is_neighbor([1, 4], 9, 3, 3))
        # 5 replicates back to 4
        self.assertTrue(Table.is_neighbor([1, 4], 9, 5, 3))
        # 6 replicates back to 4
        self.assertTrue(Table.is_neighbor([1, 4], 9, 6, 3))
        # 7 is too far from 4
        self.assertFalse(Table.is_neighbor([1, 4], 9, 7, 3))
        # 8 replicates forward to 1
        self.assertTrue(Table.is_neighbor([1, 4], 9, 8, 3))

        # factor 3 => 1 doesn't replicate back to 7
        self.assertFalse(Table.is_neighbor([5, 7], 9, 1, 3))
        # factor 4, it does
        self.assertTrue(Table.is_neighbor([5, 7], 9, 1, 4))

