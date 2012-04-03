
import getty
import random
import unittest

from samoa.core.protobuf import CommandType, DigestProperties
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server

from samoa.server.digest import Digest
from samoa.server.local_digest import LocalDigest

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestDigestSync(unittest.TestCase):

    def setUp(self):

        Digest.set_default_byte_length(1024)
        Digest.set_directory("/tmp/")

        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

        self.table_uuid = UUID(self.fixture.add_table().uuid)

        self.partition_uuid = UUID(
            self.fixture.add_remote_partition(self.table_uuid).uuid)

        self.listener = self.injector.get_instance(Listener)
        self.context = self.listener.get_context()

    def test_digest_sync(self):

        obj1, obj2 = self._make_obj(), self._make_obj()

        def test():

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)
            partition = table.get_partition(self.partition_uuid)

            # precondition: test objects not in partition digest
            self.assertFalse(partition.get_digest().test(obj1))
            self.assertFalse(partition.get_digest().test(obj2))

            # create digest with obj1 (only) set
            digest = LocalDigest(partition.get_uuid())
            digest.add(obj1)

            # request to replace the partition's digest
            response = yield self._make_request(digest,
                table.get_uuid(), partition.get_uuid())

            self.assertFalse(response.get_error_code())
            response.finish_response()

            table = self.context.get_cluster_state().get_table_set(
                ).get_table(self.table_uuid)
            partition = table.get_partition(self.partition_uuid)

            # postcondition: obj1 (only) is in partition digest
            self.assertTrue(partition.get_digest().test(obj1))
            self.assertFalse(partition.get_digest().test(obj2))

            # cleanup
            self.context.shutdown()
            yield

        Proactor.get_proactor().run(test())

    def test_error_cases(self):

        def test():

            digest = LocalDigest(self.partition_uuid)

            # missing table
            response = yield self._make_request(digest,
                None, self.partition_uuid)
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # wrong table
            response = yield self._make_request(digest,
                UUID.from_random(), self.partition_uuid)
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # missing partition
            response = yield self._make_request(digest,
                self.table_uuid, None)
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # wrong partition
            response = yield self._make_request(digest,
                self.table_uuid, UUID.from_random())
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # missing digest properties
            response = yield self._make_request(digest,
                self.table_uuid, self.partition_uuid, False)
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing digest
            response = yield self._make_request(None,
                self.table_uuid, self.partition_uuid, digest.get_properties())
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            self.context.shutdown()
            yield

        Proactor.get_proactor().run(test())

    def _make_request(self, digest, table_uuid, partition_uuid,
            digest_properties = None):

        if digest_properties is None:
            digest_properties = digest.get_properties()

        server = yield Server.connect_to(
            self.listener.get_address(), self.listener.get_port())
        request = yield server.schedule_request()

        request.get_message().set_type(CommandType.DIGEST_SYNC)

        if table_uuid:
            request.get_message().set_table_uuid(table_uuid.to_bytes())
        if partition_uuid:
            request.get_message().set_partition_uuid(partition_uuid.to_bytes())

        if digest_properties:
            request.get_message().mutable_digest_properties().CopyFrom(
                digest_properties)
            request.get_message().mutable_digest_properties().clear_filter_path()

        if digest:
            request.add_data_block(digest.get_memory_map().raw_region_str())

        response = yield request.flush_request()
        yield response

    def _make_obj(self):
        return (random.randint(0, 1<<63), random.randint(0, 1<<63))

