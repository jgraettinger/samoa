import getty
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.counter import Counter

from write_like_command_test_mixin import WriteLikeCommandTestMixin

class TestUpdateCounter(unittest.TestCase, WriteLikeCommandTestMixin):

    data_type = DataType.COUNTER_TYPE
    command_type = CommandType.UPDATE_COUNTER

    def _build_simple_fixture(self):

        record = PersistedRecord()
        Counter.update(record, ClockUtil.generate_author_id(), 0b110)

        return WriteLikeCommandTestMixin._build_simple_fixture(self, record)

    def _build_diverged_fixture(self):

        record_A = PersistedRecord()
        Counter.update(record_A, ClockUtil.generate_author_id(), 0b010) 

        record_B = PersistedRecord()
        Counter.update(record_B, ClockUtil.generate_author_id(), 0b100)

        return WriteLikeCommandTestMixin._build_diverged_fixture(
            self, record_A, record_B)

    def _augment_request(self, request):
        request.get_message().set_counter_update(0b001)

    # validation

    def _validate_response(self, response, server_name):
        value = response.get_message().counter_value

        # this write is always included
        self.assertTrue(0b001 & value)

        # depending on the handling server, a preset may be included
        if not self.is_new and server_name in ('main', 'forwarder', 'A'):
            self.assertTrue(0b010 & value)

        yield

    def _validate_persisters(self, persisters, expected_divergence):

        for persister in persisters:
            record = yield persister.get(self.key)
            value = Counter.value(record)

            # this write was included
            self.assertTrue(0b001 & value)

            if not self.is_new and 'A' not in expected_divergence:
            	self.assertTrue(0b010 & value)

            if not self.is_new and 'B' not in expected_divergence:
            	self.assertTrue(0b100 & value)
        yield

