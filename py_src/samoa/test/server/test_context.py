
import unittest
import functools

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.context import Context
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestContext(unittest.TestCase):

    def setUp(self):

        proactor = Proactor.get_proactor()

        gen = ClusterStateFixture()

        tbl = gen.add_table()
        gen.add_remote_partition(tbl)
        gen.add_local_partition(tbl)

        self.context = Context(gen.state)

    def test_cluster_state_transaction(self):

        proactor = Proactor.get_proactor()

        def callback(should_commit, state):

            gen = ClusterStateFixture(state = state)

            # add a new table & local partition
            tbl = gen.add_table(name = 'new_table',
                uuid = UUID.from_name('new_table'))
            gen.add_local_partition(tbl,
                uuid = UUID.from_name('new_part'))

            return should_commit

        def test():

            # start a transaction which rolls back
            yield self.context.cluster_state_transaction(
                functools.partial(callback, False))

            table_set = self.context.get_cluster_state().get_table_set()

            # can't query table by UUID
            self.assertFalse(table_set.get_table(UUID.from_name('new_table')))

            # can't query table by name
            self.assertFalse(table_set.get_table_by_name('new_table'))

            # start a transaction which commits
            yield self.context.cluster_state_transaction(
                functools.partial(callback, True))

            table_set = self.context.get_cluster_state().get_table_set()

            # table can be queried by name or UUID
            table = table_set.get_table(UUID.from_name('new_table'))
            table = table_set.get_table_by_name('new_table')

            # partition can be queried by UUID
            part = table.get_partition(UUID.from_name('new_part'))

            proactor.shutdown()
            yield

        proactor.spawn(test)
        proactor.run()

