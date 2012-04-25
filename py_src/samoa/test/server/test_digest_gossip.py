
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
        #  each with partitions; with replication factor of three,
        #  four will track main's digest, and one will not

        self.peers = ['peer_1', 'peer_2', 'peer_3', 'peer_4', 'peer_5']
        self.cluster = PeeredCluster(self.common_fixture,
            server_names = ['main'] + self.peers)

        self.partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')

        self.peer_partitions = {}
        for peer in self.peers:
            peer_part_uuid = self.cluster.add_partition(self.table_uuid, peer)
            self.peer_partitions[peer_part_uuid] = peer

        self.cluster.start_server_contexts()

    def test_digest_gossip(self):

        def validate(self, test_entry, should_be_set):

            ring = self.cluster.contexts['main'].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_ring()

            # enumerate peer names, and whether we expect test_entry to be set
            expected = dict((p, should_be_set) for p in self.peers)

            for ind, part in enumerate(ring):
                if part.get_uuid() == self.partition_uuid:

                    # this partition isn't replicated to from main
                    peer_part_uuid = ring[(ind + 3) % len(ring)].get_uuid()
                    expected[self.peer_partitions[peer_part_uuid]] = False
                    break

            for peer, expect in expected.items():

                partition = self.cluster.contexts[peer].get_cluster_state(
                    ).get_table_set(
                    ).get_table(self.table_uuid
                    ).get_partition(self.partition_uuid)

                if expect:
                    self.assertTrue(partition.get_digest().test(test_entry))
                else:
                    # partition won't have a digest if it's untracked
                    self.assertFalse(partition.get_digest() \
                        and partition.get_digest().test(test_entry))

        def test():

            partition = self.cluster.contexts['main'].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.partition_uuid)

            # As the persister is empty, the initial digest gossip threshold is 0.
            #  Write two records; we'll have to perform two compactions before
            #  another gossip occurs
            persister = partition.get_persister()

            yield persister.put(None,
                self.common_fixture.generate_bytes(), PersistedRecord())
            yield persister.put(None,
                self.common_fixture.generate_bytes(), PersistedRecord())

            def random_entry():
                return (random.randint(0, 1<<63), random.randint(0, 1<<63))

            # set a test digest entry
            test_entry = random_entry()
            partition.get_digest().add(test_entry)

            # peers don't know of the entry
            validate(self, test_entry, False)

            # initiate digest gossip
            LocalPartition.poll_digest_gossip(self.cluster.contexts['main'],
                self.table_uuid, self.partition_uuid)
            yield Proactor.get_proactor().wait_until_idle()

            # assert that replication peers (only) now know of the entry
            validate(self, test_entry, True)

            # set a new test digest entry
            test_entry = random_entry()
            partition.get_digest().add(test_entry)

            # one round of compaction isn't enough to go over threshold
            yield persister.bottom_up_compaction()

            LocalPartition.poll_digest_gossip(self.cluster.contexts['main'],
                self.table_uuid, self.partition_uuid)
            yield Proactor.get_proactor().wait_until_idle()

            validate(self, test_entry, False)

            # another round of compaction puts us over threshold
            yield persister.bottom_up_compaction()

            LocalPartition.poll_digest_gossip(self.cluster.contexts['main'],
                self.table_uuid, self.partition_uuid)
            yield Proactor.get_proactor().wait_until_idle()

            # again, assert that replication peers now know of the entry
            validate(self, test_entry, True)

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

