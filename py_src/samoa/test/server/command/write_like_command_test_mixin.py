
import sys
import getty
import functools
import unittest

from samoa.core.protobuf import CommandType, PersistedRecord, ClusterClock
from samoa.core.uuid import UUID
from samoa.core.proactor import Proactor
from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil, ClockAncestry
from samoa.datamodel.counter import Counter

from samoa.test.module import TestModule
from samoa.test.peered_cluster import PeeredCluster
from samoa.test.cluster_state_fixture import ClusterStateFixture


class WriteLikeCommandTestMixin(object):

    def _build_simple_fixture(self, fixture_record):
        """
        Builds a test-table with two partitions & three peers.

        'main', 'peer' each have a partition with preset value.
        'forwarder' has no partition.
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = self.data_type,
                replication_factor = 2).uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['main', 'peer', 'forwarder'])

        # make peers explicitly known to forwarder; otherwise, it's
        #  a race as to whether discovery takes 2 or 1 iteration
        self.cluster.set_known_peer('forwarder', 'main')
        self.cluster.set_known_peer('forwarder', 'peer') 

        self.main_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'main')
        self.peer_partition_uuid = self.cluster.add_partition(
            self.table_uuid, 'peer')

        self.cluster.start_server_contexts()
        self.main_persister = self.cluster.persisters[self.main_partition_uuid]
        self.peer_persister = self.cluster.persisters[self.peer_partition_uuid]

        self.key = common_fixture.generate_bytes()
        self.is_new = True

        # building most of the fixture must be done prior to
        #  Proactor.run_test(), to allow discovery to run;
        # populating persisters cannot be, so pass back a callable
        #  to be invoked within the Proactor context

        def populate():
            self.is_new = False
            yield self.main_persister.put(None, self.key, fixture_record)
            yield self.peer_persister.put(None, self.key, fixture_record)
            yield

        return populate

    def test_direct_existing_key(self):
        populate = self._build_simple_fixture()
        def test():
            yield populate()

            request = yield self._make_request('main')
            response = yield request.flush_request()

            yield self._validate_response(response, 'main')
            yield self._validate_persisters([self.main_persister], [])

            response.finish_response()
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_forwarded_existing_key(self):
        populate = self._build_simple_fixture()
        def test():
            yield populate()

            request = yield self._make_request('forwarder')
            response = yield request.flush_request()

            yield self._validate_response(response, 'forwarder')

            response.finish_response()
            yield

        Proactor.get_proactor().run_test([
            test,
            # run as separate test step, to allow for replication delay
            functools.partial(self._validate_persisters,
                [self.main_persister, self.peer_persister], []),
            self.cluster.stop_server_contexts])

    def test_direct_new_key(self):
        populate = self._build_simple_fixture()
        def test():
            request = yield self._make_request('main')
            response = yield request.flush_request()

            yield self._validate_response(response, 'main')
            yield self._validate_persisters([self.main_persister], [])

            response.finish_response()
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def test_common_error_cases(self):
        populate = self._build_simple_fixture()
        def test():
            yield populate()

            # missing table
            request = yield self._make_request('main')
            request.get_message().clear_table_uuid()

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # missing key
            request = yield self._make_request('main')
            request.get_message().clear_key()

            response = yield request.flush_request()
            samoa_response = response.get_message()
            self.assertEquals(response.get_error_code(), 400)

            response.finish_response()

            # cleanup
            self.cluster.stop_server_contexts()
            yield

        Proactor.get_proactor().run_test(test)

    def _build_diverged_fixture(self, fixture_record_A, fixture_record_B):
        """
        Builds a test-table with replication-factor 4, and five peers:

            peer A: has a partition with preset-value A
            peer B: has a partition with preset-value B
            peer nil: has a partition, but no value
            unreachable: a remote partition which is unavailable
        """

        common_fixture = ClusterStateFixture()
        self.table_uuid = UUID(
            common_fixture.add_table(
                data_type = self.data_type,
                replication_factor = 4).uuid)

        # add a partition owned by an unreachable peer
        common_fixture.add_remote_partition(self.table_uuid)

        self.cluster = PeeredCluster(common_fixture,
            server_names = ['peer_A', 'peer_B', 'peer_nil'])

        self.part_A = self.cluster.add_partition(self.table_uuid, 'peer_A')
        self.part_B = self.cluster.add_partition(self.table_uuid, 'peer_B')
        self.part_nil = self.cluster.add_partition(self.table_uuid, 'peer_nil')

        self.cluster.start_server_contexts()
        self.persisters = self.cluster.persisters

        self.key = common_fixture.generate_bytes()
        self.is_new = False

        def populate():

            yield self.persisters[self.part_A].put(
                None, self.key, fixture_record_A)
            yield self.persisters[self.part_B].put(
                None, self.key, fixture_record_B)
            yield

        return populate

    def test_diverged_write_A(self):
        populate = self._build_diverged_fixture()
        def test():
            yield populate()

            request = yield self._make_request('peer_A', 1)
            response = yield request.flush_request()

            yield self._validate_response(response, 'A')
            response.finish_response()
            yield

        Proactor.get_proactor().run_test([
            test,
            # run as separate step, to allow for replication delay
            functools.partial(self._validate_persisters, [
                    self.persisters[self.part_A],
                    self.persisters[self.part_B],
                    self.persisters[self.part_nil]],
                ['B']),
            self.cluster.stop_server_contexts])

    def test_diverged_write_nil(self):
        populate = self._build_diverged_fixture()
        def test():
            yield populate()

            request = yield self._make_request('peer_nil', 1)
            response = yield request.flush_request()

            yield self._validate_response(response, 'nil')
            response.finish_response()
            yield

        Proactor.get_proactor().run_test([
            test,
            # run as separate step, to allow for replication delay
            functools.partial(self._validate_persisters, [
                    self.persisters[self.part_A],
                    self.persisters[self.part_B],
                    self.persisters[self.part_nil]],
                ['A', 'B']),
            self.cluster.stop_server_contexts])

    def test_quorum_write_nil(self):
        populate = self._build_diverged_fixture()
        def test():
            yield populate()

            request = yield self._make_request('peer_nil', 4)
            response = yield request.flush_request()

            samoa_response = response.get_message()
            self.assertEquals(samoa_response.replication_success, 3)
            self.assertEquals(samoa_response.replication_failure, 1)

            yield self._validate_response(response, 'nil')
            response.finish_response()

            # all persisters immediately have the write
            yield self._validate_persisters([
                    self.persisters[self.part_A],
                    self.persisters[self.part_B],
                    self.persisters[self.part_nil]],
                [])
            yield

        Proactor.get_proactor().run_test([
            test,
            # run as separate step, to allow for replication delay
            functools.partial(self._validate_persisters, [
                    self.persisters[self.part_A],
                    self.persisters[self.part_B],
                    self.persisters[self.part_nil]],
                ['A', 'B']),
            self.cluster.stop_server_contexts])

    def _make_request(self, server_name, quorum = 1):

        request = yield self.cluster.schedule_request(server_name)

        samoa_request = request.get_message()
        samoa_request.set_type(self.command_type)
        samoa_request.set_table_uuid(self.table_uuid.to_bytes())
        samoa_request.set_key(self.key)
        samoa_request.set_requested_quorum(quorum)

        self._augment_request(request)
        yield request

