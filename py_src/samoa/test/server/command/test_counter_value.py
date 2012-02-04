
import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil
from samoa.datamodel.counter import Counter

from read_like_command_test_mixin import ReadLikeCommandTestMixin

class TestCounterValue(unittest.TestCase, ReadLikeCommandTestMixin):

    data_type = DataType.COUNTER_TYPE
    command_type = CommandType.COUNTER_VALUE

    def _build_simple_fixture(self):

        record = PersistedRecord()
        Counter.update(record, ClockUtil.generate_author_id(), 42)

        return ReadLikeCommandTestMixin._build_simple_fixture(self, record)

    def _build_diverged_fixture(self):

        record_A = PersistedRecord()
        Counter.update(record_A, ClockUtil.generate_author_id(), 15) 

        record_B = PersistedRecord()
        Counter.update(record_B, ClockUtil.generate_author_id(), 27)

        return ReadLikeCommandTestMixin._build_diverged_fixture(
            self, record_A, record_B)

    def _validate_response_hit(self, response):

        samoa_response = response.get_message()
        self.assertEquals(samoa_response.counter_value, 42)

    def _validate_response_miss(self, response):

        samoa_response = response.get_message()
        self.assertEquals(samoa_response.counter_value, 0)

