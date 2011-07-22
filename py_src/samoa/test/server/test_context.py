
import unittest
import functools

from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.context import Context
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestContext(unittest.TestCase):

    def test_cluster_state_transaction(self):

        gen = ClusterStateFixture()

        tbl = gen.add_table()
        gen.add_remote_partition(tbl.uuid)
        gen.add_local_partition(tbl.uuid)

        context = Context(gen.state)

        def callback(should_commit, state):

            gen = ClusterStateFixture(state = state)

            # add a new table & local partition
            tbl = gen.add_table(name = 'new_table',
                uuid = UUID.from_name('new_table'))
            gen.add_local_partition(tbl.uuid,
                uuid = UUID.from_name('new_part'))

            return should_commit

        def test():

            # start a transaction which rolls back
            yield context.cluster_state_transaction(
                functools.partial(callback, False))

            table_set = context.get_cluster_state().get_table_set()

            # can't query table by UUID
            self.assertFalse(table_set.get_table(UUID.from_name('new_table')))

            # can't query table by name
            self.assertFalse(table_set.get_table_by_name('new_table'))

            # start a transaction which commits
            yield context.cluster_state_transaction(
                functools.partial(callback, True))

            table_set = context.get_cluster_state().get_table_set()

            # table can be queried by name or UUID
            table = table_set.get_table(UUID.from_name('new_table'))
            table = table_set.get_table_by_name('new_table')

            # partition can be queried by UUID
            part = table.get_partition(UUID.from_name('new_part'))

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

