
import unittest

from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.core.protobuf import CommandType, PersistedRecord
from samoa.core.server_time import ServerTime

from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil
from samoa.datamodel.blob import Blob
from samoa.persistence.rolling_hash.element import Element

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestDigestUpdates(unittest.TestCase):

    def setUp(self):

        self.common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            self.common_fixture.add_table(
                data_type = DataType.BLOB_TYPE,
                replication_factor = 3).uuid)

        # add an unreachable remote partition
        self.down_partition_uuid = UUID(
            self.common_fixture.add_remote_partition(self.table_uuid).uuid)

        self.cluster = PeeredCluster(self.common_fixture,
            server_names = ['main', 'peer'])

        self.main_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')
        self.peer_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'peer')

        self.cluster.start_server_contexts()

        self.persister = self.cluster.persisters[self.main_partition_uuid]

    def test_compaction_updates(self):
        def test():

            # place a pruneable record fixture in 'main' persister
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(),
                self.common_fixture.generate_bytes())
            _, old_cs = yield self.persister.put(None,
                self.common_fixture.generate_bytes(), record)

            # skip forward in time, to trigger a pruning
            ServerTime.set_time(ServerTime.get_time() + \
                60 * 60 * 24 * 265)

            # trigger leaf compaction, & wait for replication to finish
            yield self.persister.bottom_up_compaction()
            yield Proactor.get_proactor().wait_until_idle()

            # assert that old checksum isn't in the local digest
            self.assertFalse(
                self.cluster.contexts['main'
                ].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.main_partition_uuid
                ).get_digest().test(old_cs))

            # dig out new (pruned) content checksum of the written record
            hash_ring = self.persister.layer(0)
            new_cs = Element(hash_ring, hash_ring.head()).content_checksum()

            # content checksum was altered, with pruning
            self.assertNotEquals(new_cs, old_cs)

            # new checksum was written to local & peer digests
            self._validate_post_replication_digests(new_cs)

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_partition_write_updates(self):
        def test():

            request = yield self.cluster.schedule_request('main')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.SET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.common_fixture.generate_bytes())
            request.add_data_block(self.common_fixture.generate_bytes())

            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())

            # allow replication to finish
            yield Proactor.get_proactor().wait_until_idle()

            # dig out content checksum of the written record
            hash_ring = self.persister.layer(0)
            checksum = Element(hash_ring, hash_ring.head()).content_checksum()

            self._validate_post_replication_digests(checksum)

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def test_partition_read_updates(self):
        def test():

            key = self.common_fixture.generate_bytes()

            # place a record fixture in 'main' persister
            record = PersistedRecord()
            Blob.update(record, ClockUtil.generate_author_id(),
                self.common_fixture.generate_bytes())
            _, checksum = yield self.persister.put(None, key, record)

            # issue a quorum read to peer
            request = yield self.cluster.schedule_request('peer')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(key)
            samoa_request.set_requested_quorum(3)

            response = yield request.flush_request()
            self.assertFalse(response.get_error_code())
            self.assertEquals(response.get_message().replication_success, 2)

            # assert that checksum is now in peer's local digest
            self.assertTrue(
                self.cluster.contexts['peer'
                ].get_cluster_state(
                ).get_table_set(
                ).get_table(self.table_uuid
                ).get_partition(self.peer_partition_uuid
                ).get_digest().test(checksum))

            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run(test())

    def _validate_post_replication_digests(self, checksum):

        # 'main' server has 'main' & 'peer' digests marked, but not 'down'
        #  (replication to 'down' failed)
        table = self.cluster.contexts['main'
            ].get_cluster_state(
            ).get_table_set(
            ).get_table(self.table_uuid)

        self.assertTrue(table.get_partition(
            self.main_partition_uuid).get_digest().test(checksum))
        self.assertTrue(table.get_partition(
            self.peer_partition_uuid).get_digest().test(checksum))
        self.assertFalse(table.get_partition(
            self.down_partition_uuid).get_digest().test(checksum))

        # 'peer' server has all of 'main', 'peer', & 'down' digests marked
        #  (replication to 'down' is assumed by peer to have suceeded)
        table = self.cluster.contexts['peer'
            ].get_cluster_state(
            ).get_table_set(
            ).get_table(self.table_uuid)

        self.assertTrue(table.get_partition(
            self.main_partition_uuid).get_digest().test(checksum))
        self.assertTrue(table.get_partition(
            self.peer_partition_uuid).get_digest().test(checksum))
        self.assertTrue(table.get_partition(
            self.down_partition_uuid).get_digest().test(checksum))

