
import unittest

from samoa.core.uuid import UUID
from samoa.server.table import Table
from samoa.request.route_state import RouteState
from samoa.request.state_exception import StateException

from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestRouteState(unittest.TestCase):

    def setUp(self):

        tbl = UUID.from_random()

        fixture = ClusterStateFixture()
        fixture.add_table(name = 'table', uuid = tbl)
        fixture.state.table[0].set_replication_factor(3)

        p0 = fixture.add_remote_partition(tbl, ring_position = 0)
        p1 = fixture.add_remote_partition(tbl, ring_position = 10)
        p2 = fixture.add_remote_partition(tbl, ring_position = 20)
        p3 = fixture.add_local_partition( tbl, ring_position = 30)
        p4 = fixture.add_remote_partition(tbl, ring_position = 40)
        p5 = fixture.add_local_partition( tbl, ring_position = 50)

        self.p0 = UUID(p0.uuid)
        self.p1 = UUID(p1.uuid)
        self.p2 = UUID(p2.uuid)
        self.p3 = UUID(p3.uuid)
        self.p4 = UUID(p4.uuid)
        self.p5 = UUID(p5.uuid)

        self.table = Table(fixture.state.table[0], fixture.server_uuid, None)

    def test_explicit_routing(self):

        route = RouteState()

        # Subtest 1: only a primary-partition is set
        route.set_ring_position(25)
        route.set_primary_partition_uuid(self.p5)

        route.load_route_state(self.table)

        # local partition 50 returned as primary,
        #   though it appears second in ring-order
        self.assertEquals(route.get_primary_partition(
            ).get_ring_position(), 50)

        # other local remote partition are peers
        self.assertEquals([30, 40],
            [p.get_ring_position() for p in route.get_peer_partitions()])

        route.reset_route_state()

        # Subtest 2: only peer-partitions are set
        route.set_ring_position(60)
        route.add_peer_partition_uuid(self.p0)
        route.add_peer_partition_uuid(self.p1)
        route.add_peer_partition_uuid(self.p2)

        route.load_route_state(self.table)

        # no primary partition set
        self.assertFalse(route.get_primary_partition())
        
        # peers are properly set
        self.assertEquals([0, 10, 20],
            [p.get_ring_position() for p in route.get_peer_partitions()])

        route.reset_route_state()

        # Subtest 3: both primary- & peer-partitions are set
        route.set_ring_position(25)
        route.set_primary_partition_uuid(self.p3)
        route.add_peer_partition_uuid(self.p4)
        route.add_peer_partition_uuid(self.p5)

        route.load_route_state(self.table)

        # local partition 30 returned as primary
        self.assertEquals(route.get_primary_partition(
            ).get_ring_position(), 30)

        # other local remote partition are peers
        self.assertEquals([40, 50],
            [p.get_ring_position() for p in route.get_peer_partitions()])

    def test_basic_routing(self):

        route = RouteState()

        # position 5 is mapped onto ring starting at 10
        route.set_ring_position(5)
        route.load_route_state(self.table)

        # local partition 30 returned as primary
        self.assertEquals(route.get_primary_partition(
            ).get_ring_position(), 30)

        # replication factor of 3 => 10, 20 also returned
        self.assertEquals([10, 20],
            [p.get_ring_position() for p in route.get_peer_partitions()])

        route.reset_route_state()

        # position 25 is mapped onto ring starting at 30
        route.set_ring_position(25)
        route.load_route_state(self.table)

        # local partition 30 is selected as primary,
        #  as it's first in ring-order
        self.assertEquals(route.get_primary_partition(
            ).get_ring_position(), 30)

        # validate secondary partitions
        self.assertEquals([40, 50],
            [p.get_ring_position() for p in route.get_peer_partitions()])

        route.reset_route_state()

        # position 35 is mapped onto ring starting at 40, and wrapping
        route.set_ring_position(35)
        route.load_route_state(self.table)

        # local partition 50 returned as primary
        self.assertEquals(route.get_primary_partition(
            ).get_ring_position(), 50)

        # validate secondary partitions
        self.assertEquals([40, 0],
            [p.get_ring_position() for p in route.get_peer_partitions()])

        route.reset_route_state()

        # position 55 is mapped onto the ring starting at 0
        route.set_ring_position(55)
        route.load_route_state(self.table)

        # no primary partition
        self.assertFalse(route.get_primary_partition())

        # validate secondary partitions
        self.assertEquals([0, 10, 20],
            [p.get_ring_position() for p in route.get_peer_partitions()])

    def test_error_cases(self):

        route = RouteState()

        # non-local primary partition
        route.set_ring_position(25)
        route.set_primary_partition_uuid(self.p4)
        route.add_peer_partition_uuid(self.p3)
        route.add_peer_partition_uuid(self.p5)

        with self.assertRaisesRegexp(StateException, 'not local'):
            route.load_route_state(self.table)

        route.reset_route_state()

        # unknown primary partition
        route.set_ring_position(25)
        route.set_primary_partition_uuid(UUID.from_random())

        with self.assertRaisesRegexp(StateException, 'not found'):
            route.load_route_state(self.table)

        route.reset_route_state()

        # unknown peer partition
        route.set_ring_position(25)
        route.set_primary_partition_uuid(self.p3)
        route.add_peer_partition_uuid(UUID.from_random())
        route.add_peer_partition_uuid(self.p5)

        with self.assertRaisesRegexp(StateException, 'is absent'):
            route.load_route_state(self.table)

        route.reset_route_state()

        # only peer-partitions are set with local partition
        #  (a primary is dynamically routed, so only two peers are expected)
        route.set_ring_position(25)
        route.add_peer_partition_uuid(self.p3)
        route.add_peer_partition_uuid(self.p4)
        route.add_peer_partition_uuid(self.p5)

        with self.assertRaisesRegexp(StateException, 'expected exactly'):
            route.load_route_state(self.table)

        route.reset_route_state()

        # too-few peer-partitions => expected partition not found
        route.set_ring_position(25)
        route.set_primary_partition_uuid(self.p3)
        route.add_peer_partition_uuid(self.p4)

        with self.assertRaisesRegexp(StateException, 'is absent'):
            route.load_route_state(self.table)

        route.reset_route_state()

