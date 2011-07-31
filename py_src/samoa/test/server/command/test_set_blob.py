
import getty
import unittest

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.persistence.data_type import DataType
from samoa.datamodel.blob import Blob
from samoa.datamodel.cluster_clock import ClusterClock, ClockAncestry

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestSetBlob(unittest.TestCase):

    def _common_bootstrap(self):
        # not in setUp(), because not all tests need it
    
        self.injector = TestModule().configure(getty.Injector())
        self.fixture = self.injector.get_instance(ClusterStateFixture)

        self.test_table = self.fixture.add_table(
            data_type = DataType.BLOB_TYPE)
        self.test_partition = self.fixture.add_local_partition(
            self.test_table.uuid)

        self.listener = self.injector.get_instance(Listener)
        self.context = self.listener.get_context()

        self.key = self.fixture.generate_name()
        self.value = self.fixture.generate_bytes()

        self.rolling_hash = self.context.get_cluster_state(
            ).get_table_set(
            ).get_table(UUID(self.test_table.uuid)
            ).get_partition(UUID(self.test_partition.uuid)
            ).get_persister(
            ).get_layer(0)

    def test_basic(self):

        self._common_bootstrap()

        def test():

            server = yield Server.connect_to(
                self.listener.get_address(), self.listener.get_port())

            response = yield self._make_request(server)

            # request succeeded
            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().set_blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # directly parse stored record
        stored_clock, stored_values = Blob.read_blob_value(
            self.rolling_hash.get(self.key))

        expected_clock = ClusterClock()
        expected_clock.tick(UUID(self.test_partition.uuid))

        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(expected_clock, stored_clock))

        self.assertEquals(stored_values, [self.value])

    def test_forwarding(self):

        self._common_bootstrap()

        # peer server bootstrap
        peer_injector = TestModule().configure(getty.Injector())
        peer_fixture = peer_injector.get_instance(ClusterStateFixture)

        peer_fixture.add_peer(uuid = self.fixture.state.local_uuid,
            port = self.listener.get_port())

        peer_listener = peer_injector.get_instance(Listener)
        peer_context = peer_listener.get_context()

        def test():

            server = yield Server.connect_to(
                peer_listener.get_address(), peer_listener.get_port())

            response = yield self._make_request(server)

            # request succeeded
            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().set_blob.success)
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            peer_context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # directly parse record stored in _main_ context
        stored_clock, stored_values = Blob.read_blob_value(
            self.rolling_hash.get(self.key))

        self.assertEquals(stored_values, [self.value])

    def test_version_tag_semantics(self):

        self._common_bootstrap()

        # set an existing record, with non-empty cluster-clock
        expected_clock = ClusterClock()
        expected_clock.tick(UUID.from_random())

        record = self.rolling_hash.prepare_record(self.key,
             Blob.serialized_length(len(self.value)))

        Blob.write_blob_value(expected_clock, self.value, record)
        self.rolling_hash.commit_record()

        def test():

            server = yield Server.connect_to(
                self.listener.get_address(), self.listener.get_port())

            # first request: pass empty tag (won't succeed)
            response = yield self._make_request(server, version_tag = '')
            self.assertFalse(response.get_error_code())
            self.assertFalse(response.get_message().set_blob.success)

            version_tag = response.get_message().set_blob.version_tag

            self.assertEquals(response.get_response_data_blocks(),
                [self.value])

            self.assertEquals(ClockAncestry.EQUAL,
                ClusterClock.compare(expected_clock,
                    ClusterClock.from_string(version_tag)))

            response.finish_response()

            # second request: pass previously returned tag (succeeds)
            response = yield self._make_request(server,
                version_tag = version_tag)

            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().set_blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])

            response.finish_response()

            # third request: don't pass a tag at all (succeeds)
            response = yield self._make_request(server, version_tag = None)

            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().set_blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])

            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # query for runtime record
        stored_clock, stored_values = Blob.read_blob_value(
            self.rolling_hash.get(self.key))

        # two writes from test_partition occurred during the test
        expected_clock.tick(UUID(self.test_partition.uuid))
        expected_clock.tick(UUID(self.test_partition.uuid))

        self.assertEquals(ClockAncestry.EQUAL,
            ClusterClock.compare(expected_clock, stored_clock))

    def test_error_cases(self):

        injector = TestModule().configure(getty.Injector())
        fixture = injector.get_instance(ClusterStateFixture)

        test_table = fixture.add_table(
            data_type = DataType.BLOB_TYPE)
        fixture.add_local_partition(test_table.uuid)

        table_no_partitions = fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        table_remote_not_available = fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        fixture.add_remote_partition(
            table_remote_not_available.uuid)

        listener = injector.get_instance(Listener)
        context = listener.get_context()

        key = fixture.generate_name()
        value = fixture.generate_bytes()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing request message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # missing request payload
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_set_blob()
            sb.set_table_name(test_table.name)
            sb.set_key(key)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # too many request payloads
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_set_blob()
            sb.set_table_name(test_table.name)
            sb.set_key(key)

            request.get_message().add_data_block_length(len(value))
            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_set_blob()
            sb.set_table_name('invalid table')
            sb.set_key(key)

            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # no table partitions
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_set_blob()
            sb.set_table_name(table_no_partitions.name)
            sb.set_key(key)

            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # remote partitions not available
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_set_blob()
            sb.set_table_name(table_remote_not_available.name)
            sb.set_key(key)

            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 503)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_replication(self):
        pass

    def _make_request(self, server, version_tag = None):

        request = yield server.schedule_request()
        request.get_message().set_type(CommandType.SET_BLOB)

        sb = request.get_message().mutable_set_blob()
        sb.set_table_name(self.test_table.name)
        sb.set_key(self.key)

        if version_tag is not None:
            sb.set_version_tag(version_tag)

        request.get_message().add_data_block_length(len(self.value))
        request.start_request()
        request.write_interface().queue_write(self.value)

        response = yield request.finish_request()
        yield response


