
import logging
logging.basicConfig(level = logging.INFO)

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
import samoa.server.protocol
import samoa.server.context
import samoa.server.listener
import samoa.core.protobuf
import samoa.command.shutdown
import samoa.command.cluster_state


class TestPeerDiscovery(unittest.TestCase):

    def setUp(self):

        self.proactor = samoa.core.Proactor()

        self.protocol = samoa.server.protocol.Protocol()
        self.protocol.set_command_handler(
            samoa.core.protobuf.CommandType.SHUTDOWN,
            samoa.command.shutdown.Shutdown())
        self.protocol.set_command_handler(
            samoa.core.protobuf.CommandType.CLUSTER_STATE,
            samoa.command.cluster_state.ClusterState())

        return

    def _server_bootstrap(self, server_uuid, port, local_partitions, peers):

        class _Server(object): pass

        srv = _Server()
        module = samoa.module.Module(':memory:', self.proactor)
        srv.injector = module.configure(getty.Injector())

        srv.injector.bind_instance(getty.Config, server_uuid,
            with_annotation = 'server_uuid')            

        srv.meta = srv.injector.get_instance(samoa.model.meta.Meta)

        session = srv.meta.new_session()

        # add a record for this server
        session.add(samoa.model.server.Server(uuid = server_uuid,
            hostname = 'localhost', port = port))

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
                    uuid = tbl_uuid, name = tbl_name))
                table_names.add(tbl_name)
                session.flush()

            session.add(samoa.model.partition.Partition(
                uuid = local_uuid,
                table_uuid = tbl_uuid,
                server_uuid = server_uuid,
                ring_position = ring_pos,
                storage_path = '/tmp/%s' % local_uuid.to_hex_str(),
                storage_size = (1 << 24), # 1 MB
                index_size = 1000))

        session.commit()

        srv.context = srv.injector.get_instance(
            samoa.server.context.Context)

        srv.listener = samoa.server.listener.Listener(
            'localhost', str(port), 1, srv.context, self.protocol)

        return srv

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

        servers = []
        servers.append(self._server_bootstrap(u('server_0'), ports[0],
            # local partitions
            [('tbl0', u('srv0_tbl0_part1'), random.randint(0, max_ring_pos)),
             ('tbl0', u('srv0_tbl0_part2'), random.randint(0, max_ring_pos)),
             ('tbl0', u('srv0_tbl0_part3'), random.randint(0, max_ring_pos))],
            # no known peers
            []))

        servers.append(self._server_bootstrap(u('server_1'), ports[1],
            # local partitions
            [('tbl0', u('srv1_tbl0_part1'), random.randint(0, max_ring_pos)),
             ('tbl1', u('srv1_tbl1_part2'), random.randint(0, max_ring_pos))],
            # knows of server 0
            [(u('server_0'), ports[0])]))

        servers.append(self._server_bootstrap(u('server_2'), ports[2],
            # local partitions
            [('tbl0', u('srv2_tbl0_part1'), random.randint(0, max_ring_pos))],
            # knows of server 0
            [(u('server_0'), ports[0])]))

        servers.append(self._server_bootstrap(u('server_3'), ports[3],
            # local partitions
            [('tbl1', u('srv3_tbl1_part1'), random.randint(0, max_ring_pos))],
            # knows of server 2
            [(u('server_2'), ports[2])]))

        self.proactor.run_later(self.proactor.shutdown, 1000)
        self.proactor.run()

        # check all servers know of all partitions
        for srv in servers:
            session = srv.meta.new_session()
            self.assertEquals(session.query(samoa.model.Partition).count(), 7)

        return

