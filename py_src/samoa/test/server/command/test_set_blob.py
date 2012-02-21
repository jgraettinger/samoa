import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

from write_like_command_test_mixin import WriteLikeCommandTestMixin

class TestSetBlob(unittest.TestCase, WriteLikeCommandTestMixin):

    data_type = DataType.BLOB_TYPE
    command_type = CommandType.SET_BLOB

    def _build_simple_fixture(self):

        record = PersistedRecord()
        Blob.update(record, ClockUtil.generate_author_id(), 'preset')

        # in support of test_wrong_clock, test_right_clock
        self.preset_clock = ClusterClock()
        self.preset_clock.CopyFrom(record.cluster_clock)

        return WriteLikeCommandTestMixin._build_simple_fixture(self, record)

    def _build_diverged_fixture(self):

        record_A = PersistedRecord()
        Blob.update(record_A, ClockUtil.generate_author_id(), 'preset-A')

        record_B = PersistedRecord()
        Blob.update(record_B, ClockUtil.generate_author_id(), 'preset-B')

        return WriteLikeCommandTestMixin._build_diverged_fixture(
            self, record_A, record_B)

    def _augment_request(self, request):
        request.add_data_block('new-value')

    # validation

    def _validate_response(self, response, server_name):
        self.assertFalse(response.get_message().has_cluster_clock())
        yield

    def _validate_persisters(self, persisters, expected_divergence):

        values = ['new-value']
        if 'A' in expected_divergence:
            values.append('preset-A')
        if 'B' in expected_divergence:
            values.append('preset-B')

        for persister in persisters:
            record = yield persister.get(self.key)
            record_values = Blob.value(record)
            for value in values:
            	self.assertIn(value, record_values)

        yield

    # additional blob-specific test cases

    def test_missing_data_block(self):
        populate = self._build_simple_fixture()
        def test():
            yield populate()

            def augment_request(request):
                pass

            self._augment_request = augment_request
            request = yield self._make_request('main')
            response = yield request.flush_request()

            self.assertEquals(response.get_error_code(), 400)
            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_wrong_clock(self):
        populate = self._build_simple_fixture()
        def test():
            yield populate()

            def augment_request(request):
                request.add_data_block('new-value')

                ClockUtil.tick(request.get_message().mutable_cluster_clock(),
                    ClockUtil.generate_author_id(), None)

            self._augment_request = augment_request
            request = yield self._make_request('main')
            response = yield request.flush_request()

            self.assertTrue(response.get_message().has_cluster_clock())

            # current value and clock are sent with response
            self.assertEquals(response.get_response_data_blocks()[0], 'preset')

            self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
                response.get_message().cluster_clock, self.preset_clock))

            response.finish_response()
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_right_clock(self):
        def augment_request(request):
            request.add_data_block('new-value')
            request.get_message().mutable_cluster_clock(
                ).CopyFrom(self.preset_clock)

        self._augment_request = augment_request
        self.test_direct_existing_key()

