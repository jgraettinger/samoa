
import unittest
import random
import bisect

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.persistence.data_type import DataType

from samoa.server.table_set import TableSet


class TestTableSet(unittest.TestCase):

    def setUp(self):

        # /psuedo/ random
        self.rnd = random.Random(42)

        self.desc = pb.ClusterState()
        self.desc.set_local_uuid(UUID.from_name('server').to_hex())
        self.desc.set_local_hostname('localhost')
        self.desc.set_local_port(1234)
        
    def test_ctor_edge_cases(self):

        self._add_table(self.desc, 'tbl1', False)
        self._add_table(self.desc, 'tbl2', False)

        # null hypothesis - should build normally
        table_set = TableSet(self.desc, None)

        self.assertEquals(table_set.get_table_by_name(
            'tbl1').get_name(), 'tbl1')
        self.assertEquals(table_set.get_table_by_name(
            'tbl2').get_name(), 'tbl2')

        # invalid table order
        tst_desc = pb.ClusterState(self.desc)
        tst_desc.table.SwapElements(0, 1)

        with self.assertRaises(RuntimeError):
            TableSet(tst_desc, None)

        # duplicate table UUID
        tst_desc = pb.ClusterState(self.desc)
        tst_desc.table[1].set_uuid(tst_desc.table[0].uuid)

        with self.assertRaises(RuntimeError):
            TableSet(tst_desc, None)

        # duplicate table name
        tst_desc = pb.ClusterState(self.desc)
        tst_desc.table[0].set_name('tbl')
        tst_desc.table[1].set_name('tbl')

        # doesn't throw, and we can query by uuid...
        table_set = TableSet(tst_desc, None)
        table_set.get_table_by_uuid(UUID.from_hex(tst_desc.table[0].uuid))
        table_set.get_table_by_uuid(UUID.from_hex(tst_desc.table[1].uuid))

        # ... but we can't query tables by name
        with self.assertRaises(RuntimeError):
            table_set.get_table_by_name('tbl')

    def test_merge_edge_cases(self):

        peer_desc = pb.ClusterState()

        self._add_table(peer_desc, 'tbl1', False)
        self._add_table(peer_desc, 'tbl2', False)

        table_set = TableSet(self.desc, None)

        # null hypothesis - doesn't throw
        table_set.merge_table_set(peer_desc, pb.ClusterState(self.desc))

        # invalid table order
        tst_desc = pb.ClusterState(peer_desc)
        tst_desc.table.SwapElements(0, 1)

        with self.assertRaises(RuntimeError):
            table_set.merge_table_set(tst_desc, pb.ClusterState(self.desc))

    def test_merge_extended(self):

        # common set of known tables
        self._add_table(self.desc, 'tbl1', False)
        self._add_table(self.desc, 'tbl2', False)
        self._add_table(self.desc, 'tbl3', False)

        peer_desc = pb.ClusterState(self.desc)
        peer_desc.set_local_uuid(UUID.from_name('peer').to_hex())

        # mark tbl1 as locally-dropped
        local_dropped = self.desc.table[0].uuid
        self.desc.table[0].Clear()
        self.desc.table[0].set_uuid(local_dropped)
        self.desc.table[0].set_dropped(True)

        # mark tbl2 as peer-dropped
        peer_dropped = peer_desc.table[1].uuid
        peer_desc.table[1].Clear()
        peer_desc.table[1].set_uuid(peer_dropped)
        peer_desc.table[1].set_dropped(True)

        # peer updates tbl3's name
        peer_desc.table[2].set_lamport_ts(peer_desc.table[2].lamport_ts + 1)
        peer_desc.table[2].set_name('tbl3_new')

        # additional tables known only locally
        self._add_table(self.desc, 'tbl4', False)
        self._add_table(self.desc, 'tbl5', False)

        # additional tables known only by peer
        self._add_table(peer_desc, 'tbl6', False)
        self._add_table(peer_desc, 'tbl7', True)

        table_set = TableSet(self.desc, None)

        # tables are locally available
        self.assertEquals(table_set.get_table_by_name(
            'tbl2').get_name(), 'tbl2')
        self.assertEquals(table_set.get_table_by_name(
            'tbl3').get_name(), 'tbl3')

        # local table_set detects changes from peer
        out = pb.ClusterState(self.desc)
        self.assertTrue(table_set.merge_table_set(peer_desc, out))
        self.assertEquals(len(out.table), 7)

        # rebuild table_set
        old_desc, self.desc = self.desc, out
        table_set = TableSet(self.desc, table_set)

        # tbl2 is now dropped
        with self.assertRaises(RuntimeError):
            table_set.get_table_by_name('tbl2')

        # tbl6 is now known
        self.assertEquals(table_set.get_table_by_name(
            'tbl6').get_name(), 'tbl6')

        # tbl3 is now known as tbl3_new
        with self.assertRaises(RuntimeError):
            table_set.get_table_by_name('tbl3')

        self.assertEquals(table_set.get_table_by_name(
            'tbl3_new').get_name(), 'tbl3_new')

        # no further changes detected from peer
        out = pb.ClusterState(self.desc)
        self.assertFalse(table_set.merge_table_set(peer_desc, out))

        self.assertEquals(self.desc.SerializeToText(),
            out.SerializeToText())

    def _add_table(self, desc, tbl_name, is_dropped):

        uuid = UUID.from_name(str(self.rnd.randint(0, 1<<32)))

        tbl = desc.add_table()
        tbl.set_uuid(uuid.to_hex())

        if is_dropped:
            tbl.set_dropped(True)
        else:
            tbl.set_data_type(DataType.BLOB_TYPE.name)
            tbl.set_name(tbl_name)
            tbl.set_replication_factor(self.rnd.randint(1, 10))
            tbl.set_lamport_ts(self.rnd.randint(1, 256))

        # re-establish sorted invariant by bubbling into place
        keys = [t.uuid for t in desc.table]
        ind = bisect.bisect_left(keys[:-1], keys[-1])

        for ind in xrange(len(desc.table) - 1, ind, -1):
            desc.table.SwapElements(ind, ind - 1)

        return tbl

