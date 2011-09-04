
import unittest

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.remote_partition import RemotePartition
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestRemotePartition(unittest.TestCase):

    def setUp(self):

        self.gen = ClusterStateFixture()
        table = self.gen.add_table()
        self.gen.add_remote_partition(table.uuid)

    def test_merge(self):

        # common partition state
        lstate = self.gen.state.table[0].partition[0]
        lstate.set_lamport_ts(1)
        lstate.set_consistent_range_begin(1)
        lstate.set_consistent_range_end(1)

        pgen = self.gen.clone_peer()

        pstate = pgen.state.table[0].partition[0]

        # local state is more recent
        lstate.set_lamport_ts(2)
        lstate.set_consistent_range_begin(2)
        lstate.set_consistent_range_end(2)

        part = RemotePartition(lstate, 0, 0, None)

        # MERGE peer state - no changes
        out = pb.ClusterState_Table_Partition(lstate)
        self.assertFalse(part.merge_partition(pstate, out))

        # peer state is more recent
        pstate.set_lamport_ts(3)
        pstate.set_consistent_range_begin(3)
        pstate.set_consistent_range_end(3)

        # MERGE peer state
        out = pb.ClusterState_Table_Partition(lstate)
        self.assertTrue(part.merge_partition(pstate, out))

        part = RemotePartition(out, 0, 0, part)

        # local state is now updated
        self.assertEquals(part.get_lamport_ts(), 3)
        self.assertEquals(part.get_consistent_range_begin(), 3)
        self.assertEquals(part.get_consistent_range_end(), 3)

        # no further changes detected from peer
        out2 = pb.ClusterState_Table_Partition(out)
        self.assertFalse(part.merge_partition(pstate, out2))

        self.assertEquals(out.SerializeToText(),
            out2.SerializeToText())

