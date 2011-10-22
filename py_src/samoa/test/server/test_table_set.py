
import unittest

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.table_set import TableSet
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestTableSet(unittest.TestCase):

    def setUp(self):

        self.gen = ClusterStateFixture()
        
    def test_ctor_edge_cases(self):

        self.gen.add_table(name = 'tbl1')
        self.gen.add_table(name = 'tbl2')

        # null hypothesis - should build normally
        table_set = TableSet(self.gen.state, None)

        self.assertEquals(table_set.get_table_by_name(
            'tbl1').get_name(), 'tbl1')
        self.assertEquals(table_set.get_table_by_name(
            'tbl2').get_name(), 'tbl2')

        # invalid table order
        tst_state = pb.ClusterState(self.gen.state)
        tst_state.table.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            TableSet(tst_state, None)

        # duplicate table UUID
        tst_state = pb.ClusterState(self.gen.state)
        tst_state.table[1].set_uuid(tst_state.table[0].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            TableSet(tst_state, None)

        # duplicate table name
        tst_state = pb.ClusterState(self.gen.state)
        tst_state.table[0].set_name('tbl')
        tst_state.table[1].set_name('tbl')

        # doesn't throw, and we can query by uuid...
        table_set = TableSet(tst_state, None)

        self.assertEquals(table_set.get_table(
            UUID(tst_state.table[0].uuid)).get_name(), 'tbl')
        self.assertEquals(table_set.get_table(
            UUID(tst_state.table[1].uuid)).get_name(), 'tbl')

        # ... but we can't query tables by name
        self.assertFalse(table_set.get_table_by_name('tbl'))

    def test_merge_edge_cases(self):

        self.gen.add_table()

        pgen = self.gen.clone_peer()

        pgen.add_table()
        pgen.add_table()

        table_set = TableSet(self.gen.state, None)

        # null hypothesis - doesn't throw
        table_set.merge_table_set(pgen.state,
            pb.ClusterState(self.gen.state))

        # invalid table order
        pgen.state.table.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            table_set.merge_table_set(pgen.state,
                pb.ClusterState(self.gen.state))

    def test_merge_extended(self):

        # common set of known tables
        self.gen.add_table(name = 'tbl1')
        self.gen.add_table(name = 'tbl2')
        self.gen.add_table(name = 'tbl3')
        self.gen.add_table(name = 'tbl4')

        pgen = self.gen.clone_peer()

        # locally drop tbl1
        tbl = self.gen.get_table_by_name('tbl1')
        local_dropped = tbl.uuid
        tbl.Clear()
        tbl.set_uuid(local_dropped)
        tbl.set_dropped(True)

        # locally update tbl2's name
        tbl = self.gen.get_table_by_name('tbl2')
        tbl.set_lamport_ts(tbl.lamport_ts + 1)
        tbl.set_name('tbl2_new')

        # peer drops tbl3
        tbl = pgen.get_table_by_name('tbl3')
        peer_dropped = tbl.uuid
        tbl.Clear()
        tbl.set_uuid(peer_dropped)
        tbl.set_dropped(True)

        # peer updates tbl4's name
        tbl = pgen.get_table_by_name('tbl4')
        tbl.set_lamport_ts(tbl.lamport_ts + 1)
        tbl.set_name('tbl4_new')

        # additional tables known only locally
        self.gen.add_table(name = 'tbl5')
        self.gen.add_table(name = 'name_conflict')

        # additional tables known only by peer
        pgen.add_table(name = 'tbl7', is_dropped = True)
        pgen.add_table(name = 'name_conflict')

        table_set = TableSet(self.gen.state, None)

        # tbl1 is locally dropped
        self.assertFalse(table_set.get_table_by_name('tbl1'))

        # tbl2 is locally renamed
        self.assertEquals(table_set.get_table_by_name(
            'tbl2_new').get_name(), 'tbl2_new')

        # tbl3 isn't yet dropped
        self.assertEquals(table_set.get_table_by_name(
            'tbl3').get_name(), 'tbl3')

        # tbl4 isn't yet renamed
        self.assertEquals(table_set.get_table_by_name(
            'tbl4').get_name(), 'tbl4')

        self.assertEquals(table_set.get_table_by_name(
            'tbl5').get_name(), 'tbl5')

        # there is no table name conflict yet
        self.assertEquals(table_set.get_table_by_name(
            'name_conflict').get_name(), 'name_conflict')

        # MERGE from peer, & rebuild table_set
        out = pb.ClusterState(self.gen.state)
        self.assertTrue(table_set.merge_table_set(pgen.state, out))

        table_set = TableSet(out, table_set)

        # eight total tables
        self.assertEquals(len(out.table), 8)

        # tbl1 is still locally dropped
        self.assertFalse(table_set.get_table_by_name('tbl1'))

        # tbl2 is still locally renamed
        self.assertEquals(table_set.get_table_by_name(
            'tbl2_new').get_name(), 'tbl2_new')

        # tbl3 is now dropped
        self.assertFalse(table_set.get_table_by_name('tbl3'))

        # tbl4 is now renamed
        self.assertEquals(table_set.get_table_by_name(
            'tbl4_new').get_name(), 'tbl4_new')

        # tbl5 is still here
        self.assertEquals(table_set.get_table_by_name(
            'tbl5').get_name(), 'tbl5')

        # tbl7 is dropped
        self.assertFalse(table_set.get_table_by_name('tbl7'))

        # both tables named 'name_conflict' can be queried by UUID
        #  but are not available by name
        uuid1 = UUID(self.gen.get_table_by_name('name_conflict').uuid)
        uuid2 = UUID(pgen.get_table_by_name('name_conflict').uuid)

        self.assertEquals(table_set.get_table(uuid1).get_name(),
            'name_conflict')
        self.assertEquals(table_set.get_table(uuid2).get_name(),
            'name_conflict')

        self.assertFalse(table_set.get_table_by_name('name_conflict'))

        # no further changes detected from peer
        out2 = pb.ClusterState(out)
        self.assertFalse(table_set.merge_table_set(pgen.state, out2))

        self.assertEquals(out.SerializeToText(),
            out2.SerializeToText())

