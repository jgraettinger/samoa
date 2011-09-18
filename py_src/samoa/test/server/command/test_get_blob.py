
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry

from samoa.test.module import TestModule
from samoa.test.cluster_state_fixture import ClusterStateFixture

class TestGetBlob(unittest.TestCase):

    def test_get_blob(self):

        # two servers - one hosting the table partition,
        #  and one knowing of the partition via peer-discovery

        srv_injector = TestModule().configure(getty.Injector())
        srv_fixture = srv_injector.get_instance(ClusterStateFixture)

        test_table = srv_fixture.add_table(data_type = DataType.BLOB_TYPE)
        test_part = srv_fixture.add_local_partition(test_table.uuid)

        srv_listener = srv_injector.get_instance(Listener)
        srv_context = srv_listener.get_context()

        # query for runtime partition rolling_hash
        rolling_hash = srv_context.get_cluster_state(
            ).get_table_set(
            ).get_table(UUID(test_table.uuid)
            ).get_partition(UUID(test_part.uuid)
            ).get_persister(
            ).get_layer(0)

        # set a test record

        expected_clock = ClusterClock()
        ClockUtil.tick(expected_clock, UUID(test_part.uuid))

        test_record = PersistedRecord()
        test_record.mutable_cluster_clock().CopyFrom(expected_clock)
        test_record.add_blob_value("a-test-value")

        record = rolling_hash.prepare_record(
            'a-test-key', test_record.ByteSize())
        record.set_value(test_record.SerializeToBytes())

        rolling_hash.commit_record()

        # bootstrap forwarding server, telling it of srv_listener
        fwd_injector = TestModule().configure(getty.Injector())
        fwd_fixture = fwd_injector.get_instance(ClusterStateFixture)
        fwd_fixture.add_peer(uuid = srv_fixture.state.local_uuid,
            port = srv_listener.get_port())

        fwd_listener = fwd_injector.get_instance(Listener)
        fwd_context = fwd_listener.get_context()

        # test operation of GET_BLOB, when requesting directly from the
        #  host server, and when proxying through the fowarding server

        def test():

            for tst_listener in (srv_listener, fwd_listener):

                server = yield Server.connect_to(
                    tst_listener.get_address(), tst_listener.get_port())

                # issue blob get request (missing key)
                request = yield server.schedule_request()

                request.get_message().set_type(CommandType.GET_BLOB)

                blob_request = request.get_message().mutable_blob()
                blob_request.set_table_name(test_table.name)
                blob_request.set_key('a-missing-key')

                response = yield request.finish_request()
                self.assertFalse(response.get_error_code())

                msg = response.get_message()
                self.assertEquals(len(msg.data_block_length), 0)
                self.assertFalse(msg.blob.success)

                response.finish_response()

                # issue blob get request (present key)
                request = yield server.schedule_request()

                request.get_message().set_type(CommandType.GET_BLOB)

                blob_request = request.get_message().mutable_blob()
                blob_request.set_table_name(test_table.name)
                blob_request.set_key('a-test-key')

                response = yield request.finish_request()
                self.assertFalse(response.get_error_code())

                msg = response.get_message()
                self.assertEquals(len(msg.data_block_length), 1)

                self.assertEquals(response.get_response_data_blocks(),
                    ['a-test-value'])

                # ensure the expected clock is present in the response
                self.assertEquals(ClockAncestry.EQUAL,
                    ClockUtil.compare(expected_clock, msg.blob.cluster_clock))

                response.finish_response()

            # cleanup
            srv_context.get_tasklet_group().cancel_group()
            fwd_context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

    def test_error_cases(self):

        injector = TestModule().configure(getty.Injector())
        fixture = injector.get_instance(ClusterStateFixture)

        table_no_partitions = fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        table_remote_not_available = fixture.add_table(
            data_type = DataType.BLOB_TYPE)

        fixture.add_remote_partition(
            table_remote_not_available.uuid)

        listener = injector.get_instance(Listener)
        context = listener.get_context()

        def test():

            server = yield Server.connect_to(
                listener.get_address(), listener.get_port())

            # missing request message
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # non-existent table 
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_blob()
            dp.set_table_name('invalid table')
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # no table partitions
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_blob()
            dp.set_table_name(table_no_partitions.name)
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 404)
            response.finish_response()

            # remote partitions not available
            request = yield server.schedule_request()
            request.get_message().set_type(CommandType.GET_BLOB)

            dp = request.get_message().mutable_blob()
            dp.set_table_name(table_remote_not_available.name)
            dp.set_key('test-key')

            response = yield request.finish_request()
            self.assertEquals(response.get_error_code(), 503)
            response.finish_response()

            # cleanup
            context.get_tasklet_group().cancel_group()
            yield

        Proactor.get_proactor().run_test(test)

