
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.blob import Blob

from read_like_command_test_mixin import ReadLikeCommandTestMixin

class TestGetBlob(unittest.TestCase, ReadLikeCommandTestMixin):

    data_type = DataType.BLOB_TYPE
    command_type = CommandType.GET_BLOB

    def _build_simple_fixture(self):

        record = PersistedRecord()
        Blob.update(record, ClockUtil.generate_author_id(), 'test-value')

        self.expected = record
        return ReadLikeCommandTestMixin._build_simple_fixture(self, record)

    def _build_diverged_fixture(self):

        record_A = PersistedRecord()
        Blob.update(record_A, ClockUtil.generate_author_id(), 'test-value-A')

        record_B = PersistedRecord()
        Blob.update(record_B, ClockUtil.generate_author_id(), 'test-value-B')

        self.expected = PersistedRecord(record_A)
        Blob.merge(self.expected, record_B, 1<<31)

        return ReadLikeCommandTestMixin._build_diverged_fixture(
            self, record_A, record_B)

    def _validate_response_hit(self, response):

        samoa_response = response.get_message()
        self.assertItemsEqual(response.get_response_data_blocks(),
            self.expected.blob_value)

        self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
            self.expected.cluster_clock, samoa_response.cluster_clock))

    def _validate_response_miss(self, response):

        samoa_response = response.get_message()
        self.assertFalse(samoa_response.success)

        self.assertItemsEqual(response.get_response_data_blocks(), [])

        expected_clock = ClusterClock()
        expected_clock.set_clock_is_pruned(False)

        self.assertEquals(ClockAncestry.CLOCKS_EQUAL, ClockUtil.compare(
            expected_clock, samoa_response.cluster_clock))

