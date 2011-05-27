
import unittest
import random
import bisect

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.persistence.data_type import DataType

from samoa.server.table import Table

class TestTable(unittest.TestCase):

    def setUp(self):

        # /psuedo/ random
        self.rnd = random.Random(42)

        self.server_uuid = UUID.from_name('server')

        self.desc = pb.ClusterState_Table()
        self.desc.set_uuid(UUID.from_name('table').to_hex())
        self.desc.set_data_type(DataType.BLOB_TYPE.name)
        self.desc.set_name('test')
        self.desc.set_replication_factor(2)
        self.desc.set_lamport_ts(10)

    def test_ctor_edge_cases(self):

        self._add_partition(self.desc, True, True)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, True, False)

        # null hypothesis - should build
        Table(self.desc, self.server_uuid, None)

        # invalid partition order
        tst_desc = pb.ClusterState_Table(self.desc)
        tst_desc.partition.SwapElements(1, 2)

        with self.assertRaises(RuntimeError):
            Table(tst_desc, self.server_uuid, None)

        # duplicate partition UUID
        tst_desc = pb.ClusterState_Table(self.desc)
        tst_desc.partition[2].set_uuid(tst_desc.partition[0].uuid)

        with self.assertRaises(RuntimeError):
            Table(tst_desc, self.server_uuid, None)

    def test_merge_edge_cases(self):

        pdesc = pb.ClusterState_Table(self.desc)

        self._add_partition(pdesc, False, False)
        self._add_partition(pdesc, False, False)

        table = Table(self.desc, self.server_uuid, None)

        # null hypothesis - doesn't throw 
        table.merge_table(pdesc, pb.ClusterState_Table(self.desc))

        # invalid partition order
        pdesc_bad = pb.ClusterState_Table(pdesc)
        pdesc_bad.partition.SwapElements(0, 1)

        with self.assertRaises(RuntimeError):
            table.merge_table(pdesc_bad, pb.ClusterState_Table(self.desc))

        # remote partition w/ local server uuid
        pdesc_bad = pb.ClusterState_Table(pdesc)
        pdesc_bad.partition[0].set_server_uuid(self.server_uuid.to_hex())

        with self.assertRaises(RuntimeError):
            table.merge_table(pdesc_bad, pb.ClusterState_Table(self.desc))

    def test_merge_lamport(self):

        table = Table(self.desc, self.server_uuid, None)

        # peer has smaller timestamp
        peer_desc = pb.ClusterState_Table(self.desc)

        peer_desc.set_name('new_name')
        peer_desc.set_replication_factor(1)
        peer_desc.set_lamport_ts(2)

        # merge keeps local state
        out = pb.ClusterState_Table(self.desc)
        table.merge_table(peer_desc, out)

        self.assertEquals(out.name, 'test')
        self.assertEquals(out.replication_factor, 2)
        self.assertEquals(out.lamport_ts, 10)

        # peer has larger timestamp
        peer_desc.set_lamport_ts(12)
        
        # merge replaces local state
        out = pb.ClusterState_Table(self.desc)
        table.merge_table(peer_desc, out)

        self.assertEquals(out.name, 'new_name')
        self.assertEquals(out.replication_factor, 1)
        self.assertEquals(out.lamport_ts, 12)

    def test_merge_extended(self):

        # common set of known partitions
        self._add_partition(self.desc, True,  False)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, False, False)

        peer_desc = pb.ClusterState_Table(self.desc)

        # locally update consistent range of a partition
        self.desc.partition[0].set_lamport_ts(
            self.desc.partition[0].lamport_ts + 1)
        self.desc.partition[0].set_consistent_range_end(
            self.desc.partition[0].consistent_range_end + 100)

        local_updated = self.desc.partition[0].uuid
        local_cr_end  = self.desc.partition[0].consistent_range_end

        # locally drop a partition
        local_dropped = self.desc.partition[1].uuid
        local_dropped_rpos = self.desc.partition[1].ring_position

        self.desc.partition[1].Clear()
        self.desc.partition[1].set_uuid(local_dropped)
        self.desc.partition[1].set_ring_position(local_dropped_rpos)
        self.desc.partition[1].set_dropped(True)

        # peer updates consistent range of remote partition
        peer_desc.partition[2].set_lamport_ts(
            peer_desc.partition[2].lamport_ts + 1)
        peer_desc.partition[2].set_consistent_range_end(
            peer_desc.partition[2].consistent_range_end + 200)

        peer_updated = peer_desc.partition[2].uuid
        peer_cr_end  = peer_desc.partition[2].consistent_range_end

        # peer remotely drops a partition
        peer_dropped = peer_desc.partition[3].uuid
        peer_dropped_rpos = peer_desc.partition[3].ring_position

        peer_desc.partition[3].Clear()
        peer_desc.partition[3].set_uuid(peer_dropped)
        peer_desc.partition[3].set_ring_position(peer_dropped_rpos)
        peer_desc.partition[3].set_dropped(True)

        # additional partitions known only locally
        self._add_partition(self.desc, True, False)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, False, False)

        # additional partitions known only by peer
        self._add_partition(peer_desc, False, False)
        self._add_partition(peer_desc, False, True)
        self._add_partition(peer_desc, False, False)

        table = Table(self.desc, self.server_uuid, None)

        # local updates are visible
        with self.assertRaises(RuntimeError):
            table.get_partition(UUID.from_hex(local_dropped))

        self.assertEquals(table.get_partition(UUID.from_hex(
            local_updated)).get_consistent_range_end(), local_cr_end)

        # peer updates are not yet visible
        table.get_partition(UUID.from_hex(peer_dropped))

        self.assertEquals(table.get_partition(UUID.from_hex(
            peer_updated)).get_consistent_range_end(), peer_cr_end - 200)

        # local table detected changes from peer
        out = pb.ClusterState_Table(self.desc)
        self.assertTrue(table.merge_table(peer_desc, out))

        # rebuild table instance
        old_desc, self.desc = self.desc, out
        table = Table(self.desc, self.server_uuid, table)

        # ten partitions in total, now
        self.assertEquals(len(self.desc.partition), 10)
        # but only seven are live
        self.assertEquals(len(table.get_ring()), 7)

        # local changes are still present
        with self.assertRaises(RuntimeError):
            table.get_partition(UUID.from_hex(local_dropped))

        self.assertEquals(table.get_partition(UUID.from_hex(
            local_updated)).get_consistent_range_end(), local_cr_end)

        # but peer changes are now visible
        with self.assertRaises(RuntimeError):
            table.get_partition(UUID.from_hex(peer_dropped))

        self.assertEquals(table.get_partition(UUID.from_hex(
            peer_updated)).get_consistent_range_end(), peer_cr_end)

        # run again, we detect no changes from peer
        out = pb.ClusterState_Table(self.desc)
        self.assertFalse(table.merge_table(peer_desc, out))

        self.assertEquals(self.desc.SerializeToText(),
            out.SerializeToText())

    def test_ring_order(self):

        self._add_partition(self.desc, True,  False)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, False, False)
        self._add_partition(self.desc, True, False)
        self._add_partition(self.desc, False, False)

        table = Table(self.desc, self.server_uuid, None)

        last_key = None

        # both protobuf partition descriptions & table.get_ring() are in
        #  'ring' order of (ring_position, uuid) ascending
        for pb_part, part in zip(self.desc.partition, table.get_ring()):
            self.assertEquals(pb_part.ring_position, part.get_ring_position())
            self.assertEquals(pb_part.uuid, part.get_uuid().to_hex())

            key = (pb_part.ring_position, pb_part.uuid)
            self.assertTrue(last_key is None or last_key < key)
            last_key = key

    def test_routing(self):

        1 / 0

    def _add_partition(self, desc, is_local, is_dropped):

        uuid = UUID.from_name(str(self.rnd.randint(0, 1<<32)))
        server_uuid = UUID.from_name(str(self.rnd.randint(0, 1<<32)))

        if is_local:
            server_uuid = self.server_uuid

        # add & fill in basic fields of the partition
        part = desc.add_partition()
        part.set_uuid(uuid.to_hex())
        part.set_ring_position(self.rnd.randint(0, 1<<32))

        if is_dropped:
            # no other fields are set on a dropped partition
            part.set_dropped(True)

        else:
            part.set_server_uuid(server_uuid.to_hex())
            part.set_consistent_range_begin(part.ring_position)
            part.set_consistent_range_end(part.ring_position)
            part.set_lamport_ts(self.rnd.randint(1, 256))

        # re-establish sorted invariant by bubbling into place
        keys = [(p.ring_position, p.uuid) for p in desc.partition]
        ind = bisect.bisect_left(keys[:-1], keys[-1])

        for ind in xrange(len(desc.partition) - 1, ind, -1):
            desc.partition.SwapElements(ind, ind - 1)

        return part

