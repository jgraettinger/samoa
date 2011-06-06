
import unittest

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.local_partition import LocalPartition
from samoa.test.cluster_state_generator import ClusterStateGenerator

class TestLocalPartition(unittest.TestCase):

    def setUp(self):

        self.gen = ClusterStateGenerator()
        table = self.gen.add_table()
        self.state = self.gen.add_local_partition(table)

    def test_merge(self):

        pgen = self.gen.clone_peer()
        pstate = pgen.state.table[0].partition[0]

        part = LocalPartition(self.state, None)

        # null hypothesis - doesn't throw
        part.merge_partition(pstate,
            pb.ClusterState_Table_Partition(self.state))

        tst_state = pb.ClusterState_Table_Partition(pstate)

        # peer has larger lamport_ts for local partition
        tst_state.set_lamport_ts(pstate.lamport_ts + 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            part.merge_partition(tst_state,
                pb.ClusterState_Table_Partition(self.state))

