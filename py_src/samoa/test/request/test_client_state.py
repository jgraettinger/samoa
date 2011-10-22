
import unittest

from samoa.core.uuid import UUID
from samoa.datamodel.clock_util import ClockUtil
from samoa.request.client_state import ClientState
from samoa.request.state_exception import StateException


class TestClientState(unittest.TestCase):

    def setUp(self):

        self.client = ClientState()

        # populate a fixture to excercise SamoaRequest validation
        samoa_request = self.client.mutable_samoa_request()

        samoa_request.set_table_uuid(UUID.from_random().to_hex())
        samoa_request.set_partition_uuid(
            UUID.from_random().to_bytes())

        samoa_request.add_peer_partition_uuid(
            UUID.from_random().to_hex())
        samoa_request.add_peer_partition_uuid(
            UUID.from_random().to_bytes())

        clock = samoa_request.mutable_cluster_clock()
        ClockUtil.tick(clock, UUID.from_random())
        ClockUtil.tick(clock, UUID.from_random())

    def test_fixture_syntax_passes(self):

        self.client.validate_samoa_request_syntax()

    def test_invalid_table_uuid(self):

        self.client.mutable_samoa_request().set_table_uuid('invalid')
        with self.assertRaisesRegexp(StateException, 'table-uuid'):
            self.client.validate_samoa_request_syntax()

    def test_invalid_partition_uuid(self):

        self.client.mutable_samoa_request().set_partition_uuid('invalid')
        with self.assertRaisesRegexp(StateException, ' partition-uuid'):
            self.client.validate_samoa_request_syntax()

    def test_invalid_peer_partition_uuid(self):

        self.client.mutable_samoa_request().add_peer_partition_uuid('invalid')
        with self.assertRaisesRegexp(StateException, 'peer-partition-uuid'):
            self.client.validate_samoa_request_syntax()

    def test_invalid_cluster_clock(self):

        self.client.mutable_samoa_request(
            ).mutable_cluster_clock().partition_clock.SwapElements(0, 1)
        with self.assertRaisesRegexp(StateException, 'cluster-clock'):
            self.client.validate_samoa_request_syntax()

