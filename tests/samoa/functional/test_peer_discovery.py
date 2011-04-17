
import socket
import random
import unittest
import getty

import samoa.module
import samoa.model.meta
import samoa.model.server
import samoa.model.table
import samoa.model.partition
import samoa.core
import samoa.client
import samoa.server.context
import samoa.core.protobuf
import samoa.command.shutdown
import samoa.command.cluster_state
import samoa.persistence


class TestPeerDiscovery(unittest.TestCase):

    def setUp(self):
        self.proactor = samoa.core.Proactor()

    def _server_bootstrap(self, server_uuid, port, local_partitions, peers):

        class _Server(object): pass

        module = samoa.module.TestModule(server_uuid, port, self.proactor)
        injector = module.configure(getty.Injector())

        meta = injector.get_instance(samoa.model.meta.Meta)
        session = meta.new_session()

        # add records for known peers
        for peer_uuid, peer_port in peers:
            session.add(samoa.model.server.Server(uuid = peer_uuid,
                hostname = 'localhost', port = peer_port))

        session.flush()

        table_names = set()

        # add local partitions
        for tbl_name, local_uuid, ring_pos in local_partitions:

            tbl_uuid = samoa.core.UUID.from_name_str(tbl_name)

            if tbl_name not in table_names:
                session.add(samoa.model.table.Table(
                    uuid = tbl_uuid, name = tbl_name,
                    data_type = samoa.persistence.BLOB_TYPE,
                    replication_factor = 2, lamport_consistency_bound = 60))
                table_names.add(tbl_name)
                session.flush()

            session.add(samoa.model.partition.Partition(
                uuid = local_uuid,
                table_uuid = tbl_uuid,
                server_uuid = server_uuid,
                ring_position = ring_pos,
                consistent_range_begin = ring_pos,
                consistent_range_end = ring_pos,
                lamport_ts = 1,
                storage_path = '/tmp/%s' % local_uuid.to_hex_str(),
                storage_size = (1 << 24), # 1 MB
                index_size = 1000))

        session.commit()

        # Cause the context to spring into life
        injector.get_instance(
            samoa.server.context.Context)

        return meta

    def test_basic(self):

        ports = []
        for i in xrange(4):

            # determine an available port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('localhost', 0))
            ports.append(sock.getsockname()[1])
            sock.close()

        max_ring_pos = samoa.model.Table.RING_SIZE

        u = samoa.core.UUID.from_name_str

        server_meta = []
        server_meta.append(self._server_bootstrap(u('server_0'), ports[0],
            # local partitions
            [('tbl0', u('srv0_tbl0_part1'), random.randint(0, max_ring_pos)),
             ('tbl0', u('srv0_tbl0_part2'), random.randint(0, max_ring_pos)),
             ('tbl0', u('srv0_tbl0_part3'), random.randint(0, max_ring_pos))],
            # no known peers
            []))

        server_meta.append(self._server_bootstrap(u('server_1'), ports[1],
            # local partitions
            [('tbl0', u('srv1_tbl0_part1'), random.randint(0, max_ring_pos)),
             ('tbl1', u('srv1_tbl1_part2'), random.randint(0, max_ring_pos))],
            # knows of server 0
            [(u('server_0'), ports[0])]))

        server_meta.append(self._server_bootstrap(u('server_2'), ports[2],
            # local partitions
            [('tbl0', u('srv2_tbl0_part1'), random.randint(0, max_ring_pos))],
            # knows of server 0
            [(u('server_0'), ports[0])]))

        server_meta.append(self._server_bootstrap(u('server_3'), ports[3],
            # local partitions
            [('tbl1', u('srv3_tbl1_part1'), random.randint(0, max_ring_pos))],
            # knows of server 2
            [(u('server_2'), ports[2])]))

        self.proactor.run_later(self.proactor.shutdown, 1000)
        self.proactor.run()

        # check all servers know of all partitions
        for meta in server_meta:
            session = meta.new_session()
            self.assertEquals(session.query(samoa.model.Partition).count(), 7)

        return

