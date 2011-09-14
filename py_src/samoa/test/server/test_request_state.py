
import getty
import unittest

from samoa.core.protobuf import CommandType, SamoaRequest
from samoa.core.uuid import UUID
from samoa.server.context import Context
from samoa.server.request_state import RequestState

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestRequestState(unittest.TestCase):

    def test_basic(self):

        injector = TestModule().configure(getty.Injector())
        fixture = injector.get_instance(ClusterStateFixture)

        table = fixture.add_table()
        partition = fixture.add_local_partition(table.uuid)

        listener = injector.get_instance(Listener)
        context = listener.get_context()

        key = fixture.generate_bytes()
        record = PersistedRecord()
        record.add_blob_value('test-value')
        record = record.SerializeToBytes()

        class Handler(BasicReplicateHandler):
            def replicate(self_i, client, table, partition, key_i, record_i):
                self.assertEquals(key_i, key)
                self.assertEquals(record_i.blob_value[0], 'test-value')
                client.finish_response()

        listener.get_protocol().set_command_handler(
            CommandType.TEST, Handler())

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            response = yield self._make_request(
                server, table.uuid, partition.uuid, key, record)

            # request succeeded
            self.assertFalse(response.get_error_code())
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        injector = TestModule().configure(getty.Injector())
        fixture = injector.get_instance(ClusterStateFixture)

        local_table = fixture.add_table()
        local_partition = fixture.add_local_partition(local_table.uuid)

        remote_table = fixture.add_table()
        remote_partition = fixture.add_remote_partition(remote_table.uuid)

        listener = injector.get_instance(Listener)
        context = listener.get_context()

        key = fixture.generate_bytes()
        record = PersistedRecord().SerializeToBytes()

        class Handler(BasicReplicateHandler):
            def replicate(self_i, client, table, partition, key, record):
                self.assertTrue(False)

        listener.get_protocol().set_command_handler(
            CommandType.TEST, Handler())

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing request message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.TEST)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing request payload
            response = yield self._make_request(
                server, local_table.uuid, local_partition.uuid, key, None)

            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # invalid table
            response = yield self._make_request(
                server, UUID.from_random(), local_partition.uuid, key, record)

            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # invalid partition
            response = yield self._make_request(
                server, local_table.uuid, UUID.from_random(), key, record)

            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # non-local partition
            response = yield self._make_request(
                server, remote_table.uuid, remote_partition.uuid, key, record)

            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # non-parsable record
            response = yield self._make_request(
                server, local_table.uuid, local_partition.uuid, key, 'bad')
            
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def _make_request(self, server, table_uuid, partition_uuid, key, record):

        request = yield server.schedule_request()
        request.get_message().set_type(CommandType.TEST)

        repl_req = request.get_message().mutable_replication()
        repl_req.set_table_uuid(UUID(table_uuid).to_bytes())
        repl_req.set_partition_uuid(UUID(partition_uuid).to_bytes())
        repl_req.set_key(key)

        if record is not None:
            request.get_message().add_data_block_length(len(record))
            request.start_request()
            request.write_interface().queue_write(record)

        yield (yield request.finish_request())

