
import unittest

from samoa.core.uuid import UUID
from samoa.server.table_set import TableSet
from samoa.request.table_state import TableState
from samoa.request.state_exception import StateException

from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestTableState(unittest.TestCase):

    def setUp(self):

        self.table_uuid = UUID.from_random()

        fixture = ClusterStateFixture()
        fixture.add_table(name = 'test_table', uuid = self.table_uuid)
        fixture.add_table(name = 'other_table')

        self.table_set = TableSet(fixture.state, None)

    def test_lookups(self):

        table = TableState()

        # lookup by UUID
        table.set_table_uuid(self.table_uuid)
        table.load_table_state(self.table_set)

        self.assertEquals(table.get_table_name(), 'test_table')
        self.assertEquals(table.get_table_uuid(), self.table_uuid)
        self.assertEquals(table.get_table().get_uuid(), self.table_uuid)

        table.reset_table_state()

        # lookup by name
        table.set_table_name('test_table')
        table.load_table_state(self.table_set)

        self.assertEquals(table.get_table_name(), 'test_table')
        self.assertEquals(table.get_table_uuid(), self.table_uuid)
        self.assertEquals(table.get_table().get_uuid(), self.table_uuid)

        table.reset_table_state()

        # both set; UUID is used
        table.set_table_uuid(self.table_uuid)
        table.set_table_name('other_table')
        table.load_table_state(self.table_set)

        self.assertEquals(table.get_table_name(), 'test_table')
        self.assertEquals(table.get_table_uuid(), self.table_uuid)
        self.assertEquals(table.get_table().get_uuid(), self.table_uuid)

    def test_error_cases(self):

        table = TableState()

        # neither UUID or name
        with self.assertRaises(StateException):
            table.load_table_state(self.table_set)

        table.reset_table_state() 

        # invalid table name
        with self.assertRaises(StateException):
            table.set_table_name('')

        table.reset_table_state()

        # invalid table uuid
        with self.assertRaises(StateException):
            table.set_table_uuid(UUID.from_nil())

        table.reset_table_state()

        # unknown UUID
        table.set_table_uuid(UUID.from_random())
        with self.assertRaisesRegexp(StateException, 'table-uuid.*not found'):
            table.load_table_state(self.table_set)

        table.reset_table_state()

        # unknown name
        table.set_table_name('unknown_name')
        with self.assertRaisesRegexp(StateException, 'table-name.*not found'):
            table.load_table_state(self.table_set)

