
import unittest

from samoa.core.uuid import UUID
from samoa.datamodel.clock_util import ClockUtil
from samoa.request.request_state import RequestState
from samoa.request.state_exception import StateException


class TestRequestState(unittest.TestCase):

    def setUp(self):

        self.rstate = RequestState()

        # this is a bit ugly. we're relying on the fact that boost::python
        #  discards const protection, to populate our test fixture
        self.samoa_request = self.rstate.get_samoa_request()

        self.samoa_request.set_table_uuid(UUID.from_random().to_hex())
        self.samoa_request.set_partition_uuid(
            UUID.from_random().to_bytes())

        self.samoa_request.add_peer_partition_uuid(
            UUID.from_random().to_hex())
        self.samoa_request.add_peer_partition_uuid(
            UUID.from_random().to_bytes())

        clock = self.samoa_request.mutable_cluster_clock()
        ClockUtil.tick(clock, ClockUtil.generate_author_id(), None)
        ClockUtil.tick(clock, ClockUtil.generate_author_id(), None)

    def test_basic_fixture_parses(self):

        self.rstate.parse_samoa_request()

    def test_invalid_table_uuid(self):

        self.samoa_request.set_table_uuid('invalid')
        with self.assertRaisesRegexp(StateException, 'table-uuid'):
            self.rstate.parse_samoa_request()

    def test_invalid_partition_uuid(self):

        self.samoa_request.set_partition_uuid('invalid')
        with self.assertRaisesRegexp(StateException, ' partition-uuid'):
            self.rstate.parse_samoa_request()

    def test_invalid_peer_partition_uuid(self):

        self.samoa_request.add_peer_partition_uuid('invalid')
        with self.assertRaisesRegexp(StateException, 'peer-partition-uuid'):
            self.rstate.parse_samoa_request()

    def test_invalid_cluster_clock(self):

        self.samoa_request.mutable_cluster_clock(
            ).author_clock.SwapElements(0, 1)

        with self.assertRaisesRegexp(StateException, 'cluster-clock'):
            self.rstate.parse_samoa_request()

