
import unittest

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.peer_set import PeerSet
from samoa.server.table_set import TableSet
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestPeerSet(unittest.TestCase):

    def setUp(self):

        self.gen = ClusterStateFixture()
        self.table = self.gen.add_table()

    def test_ctor_edge_cases(self):

        self.gen.add_peer()
        self.gen.add_peer()

        # null hypothesis - should build normally
        peer_set = PeerSet(self.gen.state, None)

        # invalid peer order
        tst_state = pb.ClusterState(self.gen.state)
        tst_state.peer.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            PeerSet(tst_state, None)

        # duplicate peer UUID
        tst_state = pb.ClusterState(self.gen.state)
        tst_state.peer[0].set_uuid(tst_state.peer[1].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            PeerSet(tst_state, None)

    def test_merge_edge_cases(self):

        pgen = self.gen.clone_peer()

        p1 = pgen.add_peer().uuid
        p2 = pgen.add_peer().uuid

        # p1 is referenced, p2 is not
        pgen.add_remote_partition(self.table, server_uuid = p1)

        peer_set = PeerSet(self.gen.state, None)

        # null hypothesis - should merge normally
        peer_set.merge_peer_set(pgen.state,
            pb.ClusterState(self.gen.state))

        # invalid peer order
        tst_state = pb.ClusterState(pgen.state)
        tst_state.peer.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            peer_set.merge_peer_set(tst_state,
                pb.ClusterState(self.gen.state))

        # duplicate peer UUID
        tst_state = pb.ClusterState(pgen.state)
        tst_state.peer[1].set_uuid(tst_state.peer[0].uuid)

        with self.assertRaisesRegexp(RuntimeError, 'assertion_failure'):
            peer_set.merge_peer_set(tst_state,
                pb.ClusterState(self.gen.state))

    def test_merge_extended(self):

        # common set of known peers
        p1 = self.gen.add_peer().uuid
        p2 = self.gen.add_peer().uuid
        p3 = self.gen.add_peer().uuid

        # create remote partitions referencing all peers
        part1 = self.gen.add_remote_partition(self.table,
            server_uuid = p1).uuid
        part2 = self.gen.add_remote_partition(self.table,
            server_uuid = p2).uuid
        part3 = self.gen.add_remote_partition(self.table,
            server_uuid = p3).uuid

        pgen = self.gen.clone_peer()

        # locally drop part1
        part = self.gen.get_partition(self.table.uuid, part1)
        rpos = part.ring_position

        part.Clear()
        part.set_uuid(part1)
        part.set_ring_position(rpos)
        part.set_dropped(True)

        # peer drops part2
        part = pgen.get_partition(self.table.uuid, part2)
        rpos = part.ring_position

        part.Clear()
        part.set_uuid(part2)
        part.set_ring_position(rpos)
        part.set_dropped(True)

        # additional peers known locally
        p4 = self.gen.add_peer().uuid
        p5 = self.gen.add_peer().uuid
        self.gen.add_remote_partition(self.table, server_uuid = p4)

        # additional peers known remotely
        p6 = pgen.add_peer().uuid
        p7 = pgen.add_peer().uuid
        pgen.add_remote_partition(self.table, server_uuid = p7)

        peer_set = PeerSet(self.gen.state, None)
        table_set = TableSet(self.gen.state, None)

        # p1 is not referenced, but still known
        peer_set.get_server_hostname(UUID(p1))

        # p2 is locally referenced & known
        peer_set.get_server_hostname(UUID(p2))

        # p3 is known / referenced by both
        peer_set.get_server_hostname(UUID(p3))

        # p4 is locally referenced & known
        peer_set.get_server_hostname(UUID(p4))

        # p5 is locally not referenced, but still known
        peer_set.get_server_hostname(UUID(p5))

        # p6 isn't yet known
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p6))

        # p7 isn't known
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p7))

        # MERGE from peer, & rebuild peer_set / table_set
        out = pb.ClusterState(self.gen.state)
        self.assertTrue(table_set.merge_table_set(pgen.state, out))
        peer_set.merge_peer_set(pgen.state, out)

        peer_set = PeerSet(out, peer_set)
        table_set = TableSet(out, table_set)

        # p1 is not kept (partition locally dropped)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p1))

        # p2 is not kept (partition remotely dropped)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p2))

        # p3 is known / referenced by both
        peer_set.get_server_hostname(UUID(p3))

        # p4 is still known / referenced
        peer_set.get_server_hostname(UUID(p4))

        # p5 is not kept (not referenced)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p5))

        # p6 was never added (not referenced)
        with self.assertRaisesRegexp(RuntimeError, "<assertion_failure>"):
            peer_set.get_server_hostname(UUID(p6))

        # p6 is now known
        peer_set.get_server_hostname(UUID(p7))

        # no further changes detected from peer
        out2 = pb.ClusterState(out)
        self.assertFalse(table_set.merge_table_set(pgen.state, out2))
        peer_set.merge_peer_set(pgen.state, out2)

        self.assertEquals(out.SerializeToText(),
            out2.SerializeToText())

