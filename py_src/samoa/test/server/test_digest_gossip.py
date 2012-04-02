
import random
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.core.protobuf import PersistedRecord
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil
from samoa.datamodel.blob import Blob
from samoa.server.local_partition import LocalPartition

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture

import samoa.persistence.persister

class TestDigestGossip(unittest.TestCase):

    def setUp(self):

        self.common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            self.common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 3).uuid)

        # create a fixture with a 'main' partition and three peers,
        #  each with partitions; with replication factor of 3, 4
        #  will track main's digest, and one will not

        self.peers = ['peer_1', 'peer_2', 'peer_3', 'peer_4', 'peer_5']
        self.cluster = PeeredCluster(self.common_fixture,
            server_names = ['main'] + self.peers)

        self.partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')

        for peer in self.peers:
            self.cluster.add_partition(self.table_uuid, peer)

        self.cluster.start_server_contexts()

    def test_digest_gossip(self):

        test_element = (random.randint(0, 1<<63), random.randint(0, 1<<63))
        test_element = (random.randint(0, 1<<63), random.randint(0, 1<<63))

        def test():

            partition = self.cluster.contexts['main'].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.partition_uuid)

            partition.get_digest().add(test_element)

            LocalPartition.poll_digest_gossip(self.cluster.contexts['main'],
                self.table_uuid, self.partition_uuid)


            # Next: write two records; vet we have to perform two compactions before another gossip occurs
            #key = self.common_fixture.generate_bytes()

            #record = PersistedRecord()
            #Blob.update(record, ClockUtil.generate_author_id(),
            #    self.common_fixture.generate_bytes())

            #persister = self.cluster.persisters[self.partition_uuid]
            yield

        def validate():

            matched = 0
            for peer in self.peers:

                partition = self.cluster.contexts[peer].get_cluster_state(
                    ).get_table_set(
                    ).get_table(self.table_uuid
                    ).get_partition(self.partition_uuid)

                if partition.get_digest().test(test_element):
                    matched += 1

            self.assertEquals(matched, len(self.peers) - 1)
            self.cluster.stop_server_contexts()

        Proactor.get_proactor().run_test([test, validate])

