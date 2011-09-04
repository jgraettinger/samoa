
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.client.server import Server
from samoa.server.listener import Listener
from samoa.persistence.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry

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
            self.assertTrue(response.get_message().blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # directly parse stored record
        test_record = PersistedRecord()
        test_record.ParseFromBytes(self.rolling_hash.get(self.key).value)

        expected_clock = ClusterClock()
        ClockUtil.tick(expected_clock, UUID(self.test_partition.uuid))

        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(expected_clock, test_record.cluster_clock))

        self.assertEquals(len(test_record.blob_value), 1)
        self.assertEquals(test_record.blob_value[0], self.value)

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
            self.assertTrue(response.get_message().blob.success)
            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            peer_context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # directly validate against record stored in _main_ context
        persisted_record = PersistedRecord()
        persisted_record.ParseFromBytes(self.rolling_hash.get(self.key).value)

        self.assertEquals(persisted_record.blob_value[0], self.value)

    def test_cluster_clock_semantics(self):

        self._common_bootstrap()

        # set an existing record, with non-empty cluster-clock
        expected_clock = ClusterClock()
        ClockUtil.tick(expected_clock, UUID.from_random())

        persisted_record = PersistedRecord()
        persisted_record.mutable_cluster_clock().CopyFrom(expected_clock)
        persisted_record.add_blob_value("previous-value")

        record = self.rolling_hash.prepare_record(
            self.key, persisted_record.ByteSize())
        record.set_value(persisted_record.SerializeToBytes())

        self.rolling_hash.commit_record()

        def test():

            server = yield Server.connect_to(
                self.listener.get_address(), self.listener.get_port())

            # first request: pass empty clock (won't succeed)
            response = yield self._make_request(server,
                cluster_clock = ClusterClock())

            self.assertFalse(response.get_error_code())
            self.assertFalse(response.get_message().blob.success)

            self.assertEquals(response.get_response_data_blocks(),
                ['previous-value'])

            response_clock = response.get_message().blob.cluster_clock
            self.assertEquals(ClockAncestry.EQUAL,
                ClockUtil.compare(expected_clock, response_clock))

            response.finish_response()

            # second request: pass previously returned tag (succeeds)
            response = yield self._make_request(server,
                cluster_clock = response_clock)

            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])

            response.finish_response()

            # third request: don't pass a tag at all (succeeds)
            response = yield self._make_request(server, cluster_clock = None)

            self.assertFalse(response.get_error_code())
            self.assertTrue(response.get_message().blob.success)
            self.assertEquals(response.get_response_data_blocks(), [])

            response.finish_response()

            # cleanup
            self.context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

        # query for runtime record
        persisted_record = PersistedRecord()
        persisted_record.ParseFromBytes(self.rolling_hash.get(self.key).value)

        # two writes from test_partition occurred during the test
        ClockUtil.tick(expected_clock, UUID(self.test_partition.uuid))
        ClockUtil.tick(expected_clock, UUID(self.test_partition.uuid))

        self.assertEquals(ClockAncestry.EQUAL,
            ClockUtil.compare(expected_clock, persisted_record.cluster_clock))

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

            sb = request.get_message().mutable_blob()
            sb.set_table_name(test_table.name)
            sb.set_key(key)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # too many request payloads
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_blob()
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

            # invalid cluster clock
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            blob_request = request.get_message().mutable_blob()
            blob_request.set_table_name(test_table.name)
            blob_request.set_key(key)

            p_clock = blob_request.mutable_cluster_clock().add_partition_clock()
            p_clock.set_partition_uuid('b' * 16)
            p_clock.set_unix_timestamp(0)
            p_clock.set_lamport_tick(0)

            p_clock = blob_request.mutable_cluster_clock().add_partition_clock()
            p_clock.set_partition_uuid('a' * 16)
            p_clock.set_unix_timestamp(0)
            p_clock.set_lamport_tick(0)

            request.get_message().add_data_block_length(len(value))
            request.start_request()
            request.write_interface().queue_write(value)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.SET_BLOB)

            sb = request.get_message().mutable_blob()
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

            sb = request.get_message().mutable_blob()
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

            sb = request.get_message().mutable_blob()
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

    def _make_request(self, server, cluster_clock = None):

        request = yield server.schedule_request()
        request.get_message().set_type(CommandType.SET_BLOB)

        blob_request = request.get_message().mutable_blob()
        blob_request.set_table_name(self.test_table.name)
        blob_request.set_key(self.key)

        if cluster_clock is not None:
            blob_request.mutable_cluster_clock().CopyFrom(cluster_clock)

        request.get_message().add_data_block_length(len(self.value))
        request.start_request()
        request.write_interface().queue_write(self.value)

        response = yield request.finish_request()
        yield response


