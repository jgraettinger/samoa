
import unittest

from samoa.core.uuid import UUID
from samoa.server.table import Table
from samoa.request.replication_state import ReplicationState
from samoa.request.state_exception import StateException

class TestReplicationState(unittest.TestCase):

    def test_quorum_met_early_callback(self):

        replication = ReplicationState()

        replication.set_quorum_count(2)
        replication.load_replication_state(4)

        # peer success
        self.assertFalse(replication.peer_replication_success())
        self.assertFalse(replication.is_replication_finished())

        # peer success - quorum met, returns true
        self.assertTrue(replication.peer_replication_success())
        self.assertTrue(replication.is_replication_finished())

        self.assertEquals(replication.get_peer_success_count(), 2)
        self.assertEquals(replication.get_peer_failure_count(), 0)

        # peer failure - returns false, as quorum already met
        self.assertFalse(replication.peer_replication_failure())
        self.assertTrue(replication.is_replication_finished())

        # peer success - returns false, as quorum already met
        self.assertFalse(replication.peer_replication_success())
        self.assertTrue(replication.is_replication_finished())

        # counts are frozen at time of success
        self.assertEquals(replication.get_peer_success_count(), 2)
        self.assertEquals(replication.get_peer_failure_count(), 0)

    def test_quorum_unmet_on_success(self):

        replication = ReplicationState()

        replication.set_quorum_count(3)
        replication.load_replication_state(4)

        # peer success
        self.assertFalse(replication.peer_replication_success())
        self.assertFalse(replication.is_replication_finished())

        # peer failure
        self.assertFalse(replication.peer_replication_failure())
        self.assertFalse(replication.is_replication_finished())

        # peer failure
        self.assertFalse(replication.peer_replication_failure())
        self.assertFalse(replication.is_replication_finished())

        # peer success - quorum still unmet, but returns true
        self.assertTrue(replication.peer_replication_success())
        self.assertTrue(replication.is_replication_finished())

        self.assertEquals(replication.get_peer_success_count(), 2)
        self.assertEquals(replication.get_peer_failure_count(), 2)

    def test_quorum_unmet_on_failure(self):

        replication = ReplicationState()

        replication.set_quorum_count(2)
        replication.load_replication_state(4)

        # peer success
        self.assertFalse(replication.peer_replication_success())
        self.assertFalse(replication.is_replication_finished())

        # peer failure
        self.assertFalse(replication.peer_replication_failure())
        self.assertFalse(replication.is_replication_finished())

        # peer failure
        self.assertFalse(replication.peer_replication_failure())
        self.assertFalse(replication.is_replication_finished())

        # peer failure - quorum unmet, but returns true
        self.assertTrue(replication.peer_replication_failure())
        self.assertTrue(replication.is_replication_finished())

        self.assertEquals(replication.get_peer_success_count(), 1)
        self.assertEquals(replication.get_peer_failure_count(), 3)

    def test_quorum_edge_cases(self):

        replication = ReplicationState()

        # not set => implicitly equal to replication factor
        replication.load_replication_state(4)
        self.assertEquals(4, replication.get_quorum_count())

        replication.reset_replication_state()

        # explicitly set to 0 => implicitly equal to replication factor
        replication.set_quorum_count(0)
        replication.load_replication_state(4)
        self.assertEquals(4, replication.get_quorum_count())

        replication.reset_replication_state()

        # too large => raises StateException
        replication.set_quorum_count(5)
        with self.assertRaisesRegexp(StateException, "quorum too large"):
            replication.load_replication_state(4)

