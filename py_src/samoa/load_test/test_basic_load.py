
import sys
import unittest

from samoa.core.uuid import UUID
from samoa.core.protobuf import CommandType
from samoa.core.proactor import Proactor

from samoa.datamodel.data_type import DataType
from samoa.datamodel.clock_util import ClockUtil

import samoa.persistence.rolling_hash.hash_ring

from samoa.test.cluster_state_fixture import ClusterStateFixture
from samoa.test.peered_cluster import PeeredCluster

class TestBasicLoad(unittest.TestCase):

    server_count = 10
    partition_count = 10
    replication_factor = 3 

    value_average_size = 1024

    partition_layers = [
        [1<<18, 256], # 262K
        [1<<22, 1<<12], # 4M, 4K
    ]

    #partition_layers = [
    #    [1<<19, 1<<9], # 525K, 512
    #    [1<<16, 1<<14], # 16M, 16K
    #]

    def setUp(self):

        ClockUtil.clock_jitter_bound = 5 # seconds

        cluster_state_fixture = ClusterStateFixture()
        self.rnd = cluster_state_fixture.rnd

        self.server_names = ['root'] + \
            ['peer_%d' % i for i in range(self.server_count - 1)]
        self.cluster = PeeredCluster(cluster_state_fixture, self.server_names)

        for i in range(self.server_count - 1):
            # 'root' tracks all other peers
        	self.cluster.set_known_peer('root', 'peer_%i' % i, seed = True)

        self.cluster.start_server_contexts()

    def _create_table(self):

        request = yield self.cluster.schedule_request('root')

        request.get_message().set_type(CommandType.CREATE_TABLE)
        ct = request.get_message().mutable_create_table()
        ct.set_name('test_table')
        ct.set_data_type(DataType.BLOB_TYPE.name)
        ct.set_replication_factor(self.replication_factor)
        ct.set_consistency_horizon(5) # seconds

        response = yield request.flush_request()
        self.assertFalse(response.get_error_code())
        response.finish_response()
        yield

    def _create_partition(self, server_name, ring_position):

        request = yield self.cluster.schedule_request(server_name)

        request.get_message().set_type(CommandType.CREATE_PARTITION)
        request.get_message().set_table_name('test_table')

        cp = request.get_message().mutable_create_partition()
        cp.set_ring_position(ring_position)

        tmpfile = UUID.from_random().to_hex()

        for layer, (region, index) in enumerate(self.partition_layers):
            rl = cp.add_ring_layer()
            rl.set_storage_size(region)
            rl.set_index_size(index)
            rl.set_file_path('/tmp/part_%s_%d.hash' % (tmpfile, layer))

        response = yield request.flush_request()
        self.assertFalse(response.get_error_code())
        response.finish_response()
        yield

    def _test_basic_load(self):

        proactor = Proactor.get_proactor()

        def test():

            yield self._create_table()

            # replicate to cluster
            yield proactor.wait_until_idle()

            # build persisters
            for partition_ind in range(self.partition_count):
                for server_ind, server_name in enumerate(self.server_names):
                    ind = partition_ind * len(self.server_names) + server_ind
                    ring_position = ((1<<64) - 1) * ind / \
                        (self.server_count * self.partition_count + 1)

                    yield self._create_partition(server_name, ring_position)

            yield proactor.wait_until_idle()

            conns = []
            for srv_name in self.server_names:
                conns.append((yield self.cluster.get_connection(srv_name)))

            # run load
            for i in xrange(3000000):

                request = yield self.rnd.choice(conns).schedule_request()

                msg = request.get_message()
                msg.set_type(CommandType.SET_BLOB)
                msg.set_table_name('test_table')
                msg.set_key(UUID.from_name(str(i)).to_bytes())

                request.add_data_block(UUID.from_name(str(i+1)).to_hex())

                response = yield request.flush_request()
                code = response.get_error_code()
                #self.assertFalse(response.get_error_code())
                response.finish_response()
                if code:
                    print "MADE IT TO ITERATION", i
                    break

            """
            for srv_name in self.server_names:

                print >> sys.stderr, srv_name

                state = self.cluster.contexts[srv_name].get_cluster_state()
                pb_state = state.get_protobuf_description()

                for pb_table in pb_state.table:
                    for pb_part in pb_table.partition:
                        if pb_part.server_uuid != pb_state.local_uuid:
                            continue

                        persister = state.get_table_set(
                            ).get_table(UUID(pb_table.uuid)
                            ).get_partition(UUID(pb_part.uuid)
                            ).get_persister()

                        print >> sys.stderr, pb_part

                        for layer in range(persister.layer_count()):
                            print >> sys.stderr, persister.layer(layer)
                            print >> sys.stderr
            """

            """
            # validate
            for i in xrange(1000):

                request = yield self.cluster.schedule_request(
                    self.rnd.choice(self.server_names))

                msg = request.get_message()
                msg.set_type(CommandType.GET_BLOB)
                msg.set_table_name('test_table')
                msg.set_key(UUID.from_name(str(i)).to_bytes())

                response = yield request.flush_request()
                self.assertFalse(response.get_error_code())

                self.assertItemsEqual(response.get_response_data_blocks(),
                    [UUID.from_name(str(i+1)).to_hex()])

                response.finish_response()

                del response
                del request
                print i
            """

            self.cluster.stop_server_contexts()
            yield

        proactor.run(test())

