
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.server.listener import Listener
from samoa.client.server import Server
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob
from samoa.persistence.persister import Persister

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class TestGetBlob(unittest.TestCase):

    def test_simple_read_A(self):

        populate = self._build_fixture()

        def test():

            author_A = yield populate()
            request = yield self.cluster.schedule_request('main')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)

            self.assertEquals(len(samoa_response.data_block_length), 1)
            self.assertEquals(response.get_response_data_blocks(),
                [self.value_A])

            expected_clock = ClusterClock()
            ClockUtil.tick(expected_clock, author_A, None)

            # ensure expected clock is present in response; use a large horizon
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_simple_read_nil(self):

        populate = self._build_fixture()

        def test():

            yield populate()
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertFalse(response.get_error_code())
            self.assertEquals(samoa_response.replication_success, 1)
            self.assertEquals(samoa_response.replication_failure, 0)

            self.assertEquals(len(samoa_response.data_block_length), 0)

            expected_clock = ClusterClock()
            expected_clock.set_clock_is_pruned(False)

            # ensure expected clock is present in response
            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                expected_clock, samoa_response.cluster_clock))

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_get_blob_error_cases(self):

        populate = self._build_fixture()

        def test():

            yield populate()

            # missing table
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_key(self.key)

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing key
            request = yield self.cluster.schedule_request('forwarder')

            samoa_request = request.get_message()
            samoa_request.set_type(CommandType.GET_BLOB)
            samoa_request.set_table_uuid(self.table_uuid.to_bytes())

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

