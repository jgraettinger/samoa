
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestPartitionUpkeep(unittest.TestCase):

    def test_basic(self):

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 1).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['peer_1', 'peer_2'])

        self.part_uuid_1 = self.cluster.add_partition(table_uuid, 'peer_1')
        self.part_uuid_2 = self.cluster.add_partition(table_uuid, 'peer_2')

        self.cluster.start_server_contexts()

        
        
        



"""
    def _build_fixture(self):
        "" "
        Builds a test-table with a single partition, and two peers:

        main: has a partition with preset value
        forwarder: has no partition
        "" "

        common_fixture = ClusterStateFixture()
        table_uuid = UUID(
            common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 2).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['peer_1', 'peer_2', 'peer_3'])

        self.partition_1 = self.cluster.add_partition(table_uuid, 'peer_1')
        self.partition_2 = self.cluster.add_partition(table_uuid, 'peer_2')
        self.partition_3 = self.cluster.add_partition(table_uuid, 'peer_3')

        self.cluster.start_server_contexts()


        for i in xrange(100):
            key = 'key-%d' % i

            record = PersistedRecord()
            record.add_blob_value('peer_1-value')
            ClockUtil.tick(record.mutable_cluster_clock(),
                self.partition1_uuid)

            raw_record = self.peer1_hash.prepare_record(key,
                record.ByteSize())
            raw_record.set_value(record.SerializeToBytes())
            self.peer1_hash.commit_record()

            record = PersistedRecord()
            record.add_blob_value('peer_2-value')
            ClockUtil.tick(record.mutable_cluster_clock(),
                self.partition2_uuid)

            raw_record = self.peer2_hash.prepare_record(key,
                record.ByteSize())
            raw_record.set_value(record.SerializeToBytes())
            self.peer2_hash.commit_record()
"""

