
import unittest

from samoa.core.uuid import UUID
from samoa.server.table import Table
from samoa.request.table_state import TableState
from samoa.request.state_exception import StateException

from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestTableState(unittest.TestCase):

    def setUp(self):

        self.table_uuid = UUID.from_random()

        fixture = ClusterStateFixture()
        fixture.add_table(name = 'table', uuid = tbl)
        fixture.state.table[0].set_replication_factor(3)


        self.p0 = UUID(p0.uuid)
        self.p1 = UUID(p1.uuid)
        self.p2 = UUID(p2.uuid)
        self.p3 = UUID(p3.uuid)
        self.p4 = UUID(p4.uuid)
        self.p5 = UUID(p5.uuid)

        self.table = Table(fixture.state.table[0], fixture.server_uuid, None)

    def testFoo(self):
        self.assertFalse(True)
