
import getty
import unittest

from samoa.core.uuid import UUID
from samoa.core.protobuf import CommandType, ClusterClock, SamoaRequest
from samoa.datamodel.clock_util import ClockUtil
from samoa.server.context import Context
from samoa.server.request_state import RequestState

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestRequestState(unittest.TestCase):

    def setUp(self):

        fixture = ClusterStateFixture()

        # build a basic test fixture (table, two local partitions, two peers)
        table = fixture.add_table(replication_factor = 4)
        local_part1 = fixture.add_local_partition(table.uuid)
        local_part2 = fixture.add_local_partition(table.uuid)
        peer_part1 = fixture.add_remote_partition(table.uuid)
        peer_part2 = fixture.add_remote_partition(table.uuid)

        self.table_name = table.name
        self.table_uuid = UUID(table.uuid)
        self.local_part1_uuid = UUID(local_part1.uuid)
        self.local_part2_uuid = UUID(local_part2.uuid)
        self.peer_part1_uuid = UUID(peer_part1.uuid)
        self.peer_part2_uuid = UUID(peer_part2.uuid)

        # build a SamoaRequest fully populated with valid fields
        #  extracted to runtime entitites by RequestState

        # tests will selectively modify / clear field state
        test_request = SamoaRequest()
        test_request.set_type(CommandType.TEST)

        # populate cluster_clock
        ClockUtil.tick(test_request.mutable_cluster_clock(),
            self.local_part1_uuid)
        ClockUtil.tick(test_request.mutable_cluster_clock(),
            self.peer_part1_uuid)

        # populate table_uuid & table_name
        test_request.set_table_uuid(self.table_uuid.to_bytes())
        test_request.set_table_name(self.table_name)

        # populate key
        test_request.set_key('test-key')

        # populate partition_uuid & peer_partition_uuid
        test_request.set_partition_uuid(
            self.local_part1_uuid.to_bytes())
        test_request.add_peer_partition_uuid(
            self.local_part2_uuid.to_bytes())
        test_request.add_peer_partition_uuid(
            self.peer_part1_uuid.to_bytes())
        test_request.add_peer_partition_uuid(
            self.peer_part2_uuid.to_bytes())

        # populate quorum
        test_request.set_requested_quorum(4)

        self.fixture = fixture
        self.test_request = test_request
        return

    def test_fixed_routes(self):

        context = Context(self.fixture.state)

        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.load_from_samoa_request(context)

        # validate populated runtime entities
        self.assertEquals(rstate.get_table().get_uuid(),
            self.table_uuid)

        # assert partitions are as dictated by test fixture
        self.assertEquals(rstate.get_primary_partition().get_uuid(),
            self.local_part1_uuid)

        self.assertEquals(
            set([self.local_part2_uuid, self.peer_part1_uuid,
                self.peer_part2_uuid]),
            set(p.partition.get_uuid() for p in rstate.get_partition_peers()))

    def test_table_routing(self):

        context = Context(self.fixture.state)

        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().clear_table_uuid()
        rstate.get_samoa_request().clear_partition_uuid()
        rstate.get_samoa_request().clear_peer_partition_uuid()

        rstate.load_from_samoa_request(context)

        # validate populated runtime entities
        self.assertEquals(rstate.get_table().get_uuid(),
            self.table_uuid)

        # one of the local partitions was returned
        self.assertTrue(rstate.get_primary_partition().get_uuid() in \
            (self.local_part1_uuid, self.local_part2_uuid))

        # three peer partitions are also returned
        self.assertEquals(len(rstate.get_partition_peers()), 3)

    def test_bad_table(self):

        context = Context(self.fixture.state)

        # table name doesn't exist
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().clear_table_uuid()
        rstate.get_samoa_request().set_table_name('invalid-table')

        with self.assertRaisesRegexp(RuntimeError, 'code 404'):
            rstate.load_from_samoa_request(context)

        # invalid table UUID
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().clear_table_name()
        rstate.get_samoa_request().set_table_uuid('invalid-uuid')

        with self.assertRaisesRegexp(RuntimeError, 'code 400'):
            rstate.load_from_samoa_request(context)

        # table UUID doesn't exist
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().clear_table_name()
        rstate.get_samoa_request().set_table_uuid(
            UUID.from_random().to_bytes())

        with self.assertRaisesRegexp(RuntimeError, 'code 404'):
            rstate.load_from_samoa_request(context)

    def test_bad_partition(self):

        context = Context(self.fixture.state)

        # invalid partition UUID
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().set_partition_uuid('invalid-uuid')

        with self.assertRaisesRegexp(RuntimeError, 'code 400'):
            rstate.load_from_samoa_request(context)

        # partition UUID doesn't exist
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().set_partition_uuid(
            UUID.from_random().to_bytes())

        with self.assertRaisesRegexp(RuntimeError, 'code 404'):
            rstate.load_from_samoa_request(context)

        # partition UUID is remote
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().set_partition_uuid(
            self.peer_part1_uuid.to_bytes())

        with self.assertRaisesRegexp(RuntimeError, 'code 404'):
            rstate.load_from_samoa_request(context)

    def test_bad_peer_partition(self):

        context = Context(self.fixture.state)

        # invalid peer_partition UUID
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().add_peer_partition_uuid('invalid-uuid')

        with self.assertRaisesRegexp(RuntimeError, 'code 400'):
            rstate.load_from_samoa_request(context)

        # peer partition UUID doesn't exist
        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().add_peer_partition_uuid(
            UUID.from_random().to_bytes())

        with self.assertRaisesRegexp(RuntimeError, 'code 404'):
            rstate.load_from_samoa_request(context)

    def test_no_table_partitions(self):

        table2 = self.fixture.add_table()
        context = Context(self.fixture.state)

        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().clear_requested_quorum()
        rstate.get_samoa_request().set_table_name(table2.name)
        rstate.get_samoa_request().clear_table_uuid()

        rstate.get_samoa_request().clear_partition_uuid()
        rstate.get_samoa_request().clear_peer_partition_uuid()

        with self.assertRaisesRegexp(RuntimeError, 'code 410'):
            rstate.load_from_samoa_request(context)


    def test_invalid_cluster_clock(self):

        context = Context(self.fixture.state)

        rstate = RequestState(None)
        rstate.get_samoa_request().CopyFrom(self.test_request)

        rstate.get_samoa_request().mutable_cluster_clock(
            ).partition_clock.SwapElements(0, 1)

        with self.assertRaisesRegexp(RuntimeError, 'code 400'):
            rstate.load_from_samoa_request(context)

