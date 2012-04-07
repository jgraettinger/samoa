
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestEventualConsistency(unittest.TestCase):

    def test_basic_consistency(self):

        # two-peer fixture with divergent values
        # postcondition: peers have synchronized

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B'])

        part_A = cluster.add_partition(table_uuid, 'peer_A')
        part_B = cluster.add_partition(table_uuid, 'peer_B')

        # shared key with divergent values
        key = common_fixture.generate_bytes()
        value_A = common_fixture.generate_bytes()
        value_B = common_fixture.generate_bytes()

        cluster.start_server_contexts()
        proactor = Proactor.get_proactor()

        def test():
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), value_A)
            yield cluster.persisters[part_A].put(None, key, record)

            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), value_B)
            yield cluster.persisters[part_B].put(None, key, record)

            yield cluster.persisters[part_A].bottom_up_compaction()
            yield proactor.wait_until_idle()

            for part_uuid in part_A, part_B:
                record = yield cluster.persisters[part_uuid].get(key)

                # peers are now consistent
                self.assertItemsEqual(record.blob_value, [value_A, value_B])

            cluster.stop_server_contexts()
            yield

        proactor.run(test())

    def test_move_succeeds_dropped_partition(self):

        # three-peer fixture (factor of 2), where the record
        #  fixture is placed on a dropped partition
        # postcondition: two live partitions have the record
        #  fixture; dropped partition doesn't

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        cluster = PeeredCluster(common_fixture,
            server_names = ['peer_drop', 'peer_A', 'peer_B'])

        part_drop = cluster.add_partition(table_uuid, 'peer_drop')
        part_A = cluster.add_partition(table_uuid, 'peer_A')
        part_B = cluster.add_partition(table_uuid, 'peer_B')

        cluster.fixtures['peer_drop'].get_partition(
            table_uuid, part_drop).set_dropped(1)

        key = common_fixture.generate_bytes()
        value = common_fixture.generate_bytes()

        cluster.start_server_contexts()
        proactor = Proactor.get_proactor()

        def test():
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), value)
            yield cluster.persisters[part_drop].put(None, key, record)

            yield cluster.persisters[part_drop].bottom_up_compaction()
            yield proactor.wait_until_idle()

            for part_uuid in part_A, part_B:
                # part_A & part_B have part_drop's value
                record = yield cluster.persisters[part_uuid].get(key)
                self.assertItemsEqual(record.blob_value, [value])

            # part_drop no longer does
            record = yield cluster.persisters[part_drop].get(key)
            self.assertIsNone(record)

            cluster.stop_server_contexts()
            yield

        proactor.run(test())

    def test_move_fails_dropped_partition(self):

        # three-peer fixture (factor of 2), where the record
        #  fixture is placed on a dropped partition; one
        #  of the peers is additionally unreachable
        # postcondition: fixture is on both remaining live
        #  and dropped partition (insufficient quorum to move)

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        # unreachable remote partition
        common_fixture.add_remote_partition(table_uuid)

        cluster = PeeredCluster(common_fixture,
            server_names = ['peer_drop', 'peer_A'])

        part_drop = cluster.add_partition(table_uuid, 'peer_drop')
        part_A = cluster.add_partition(table_uuid, 'peer_A')

        cluster.fixtures['peer_drop'].get_partition(
            table_uuid, part_drop).set_dropped(1)

        key = common_fixture.generate_bytes()
        value = common_fixture.generate_bytes()

        cluster.start_server_contexts()
        proactor = Proactor.get_proactor()

        def test():
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(), value)
            yield cluster.persisters[part_drop].put(None, key, record)

            yield cluster.persisters[part_drop].bottom_up_compaction()
            yield proactor.wait_until_idle()

            # both part_A & part_drop should have part_drop's value
            for part_uuid in part_A, part_drop:
                record = yield cluster.persisters[part_uuid].get(key)
                self.assertItemsEqual(record.blob_value, [value])

            cluster.stop_server_contexts()
            yield

        proactor.run(test())

    def test_move_succeeds_with_divergent_partitions(self):

        # three-peer fixture (factor of 2), where each peer
        #  has a divergent fixture record
        # postcondition: two of three have merged records;
        #  one has no value

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B', 'peer_C'])

        part_A = cluster.add_partition(table_uuid, 'peer_A')
        part_B = cluster.add_partition(table_uuid, 'peer_B')
        part_C = cluster.add_partition(table_uuid, 'peer_C')

        key = common_fixture.generate_bytes()
        part_values = dict((p, common_fixture.generate_bytes()) \
            for p in [part_A, part_B, part_C])

        cluster.start_server_contexts()
        proactor = Proactor.get_proactor()

        def test():
            for part_uuid, value in part_values.items():

                # place divergent fixtures under each peer partition
                record = PersistedRecord()
                Blob.update(record, ClockUtil.generate_author_id(), value)
                yield cluster.persisters[part_uuid].put(None, key, record)

            for part_uuid in part_values.keys():
                yield cluster.persisters[part_uuid].bottom_up_compaction()

            yield proactor.wait_until_idle()

            # two of three peers (replication factor) should have
            #  all values, and one shouldn't have the key
            record_count = 0
            for part_uuid in part_values.keys():
                record = yield cluster.persisters[part_uuid].get(key)

                if record is None: continue

                self.assertItemsEqual(set(record.blob_value),
                    part_values.values())

                record_count += 1

            self.assertEquals(record_count, 2)

            cluster.stop_server_contexts()
            yield

        proactor.run(test())

