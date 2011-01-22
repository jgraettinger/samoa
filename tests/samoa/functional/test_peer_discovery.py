
import logging
logging.basicConfig(level = logging.INFO)

import socket
import random
import unittest
import getty

import samoa.module
import samoa.model
import samoa.core
import samoa.client
import samoa.server
import samoa.command.echo
import samoa.command.shutdown


class TestPeerDiscovery(unittest.TestCase):

    def setUp(self):

        self.proactor = samoa.core.Proactor()

        self.protocol = samoa.server.SimpleProtocol()
        self.protocol.add_command_handler('echo',
            samoa.command.echo.Echo())
        self.protocol.add_command_handler('shutdown',
            samoa.command.shutdown.Shutdown())

        self.table_uid = 'test_table'
        return

    def _start_server_bootstrap(self, local_partitions):

        class _Server(object): pass

        srv = _Server()
        srv.injector = samoa.module.Module(
            ':memory:', self.proactor).configure(getty.Injector())

        srv.meta = srv.injector.get_instance(samoa.model.Meta)

        # determine an open port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))
        srv.port = sock.getsockname()[1]
        sock.close()

        session = srv.meta.new_session()

        session.add(samoa.model.Table(uid = self.table_uid))
        session.flush()

        for local_uid, ring_pos in local_partitions:
            session.add(samoa.model.LocalPartition(
                table_uid = self.table_uid,
                uid = local_uid,
                ring_pos = ring_pos,
                storage_path = '/tmp/%s' % local_uid,
                storage_size = (1 << 24), # 1 MB
                index_size = 1000))

        session.commit()
        return srv

    def _finish_server_bootstrap(self, srv, remote_partitions):

        session = srv.meta.new_session()

        for rpart, rport in remote_partitions:
            session.add(samoa.model.RemotePartition(
                table_uid = self.table_uid,
                uid = rpart.uid,
                ring_pos = rpart.ring_pos,
                remote_host = 'localhost',
                remote_port = rport))

        session.commit()

        srv.context = srv.injector.get_instance(samoa.server.Context)

        srv.listener = samoa.server.Listener(
            '0.0.0.0', str(srv.port), 1, srv.context, self.protocol)

        return

    def test_basic(self):

        max_ring_pos = samoa.model.Table.DEFAULT_RING_SIZE

        servers = []
        servers.append(self._start_server_bootstrap(
            [('srv1_part1', random.randint(0, max_ring_pos)),
             ('srv1_part2', random.randint(0, max_ring_pos)),
             ('srv1_part3', random.randint(0, max_ring_pos))]))

        servers.append(self._start_server_bootstrap(
            [('srv2_part1', random.randint(0, max_ring_pos)),
             ('srv2_part2', random.randint(0, max_ring_pos))]))

        servers.append(self._start_server_bootstrap(
            [('srv3_part1', random.randint(0, max_ring_pos))]))

        servers.append(self._start_server_bootstrap(
            [('srv4_part1', random.randint(0, max_ring_pos))]))

        # select a local partition from each server
        server_parts = [srv.meta.new_session().query(
            samoa.model.LocalPartition).first() for srv in servers]

        # server 0 knows of no remote partitions
        self._finish_server_bootstrap(servers[0], [])

        # server 1 knows of a server 0 partition
        self._finish_server_bootstrap(servers[1],
            [(server_parts[0], servers[0].port)])

        # server 2 knows of a server 0 partition
        self._finish_server_bootstrap(servers[2],
            [(server_parts[0], servers[0].port)])

        # server 3 knows of a server 2 partition
        self._finish_server_bootstrap(servers[3],
            [(server_parts[2], servers[2].port)])

        def shutdown():
            [srv.listener.cancel() for srv in servers]

        self.proactor.run_later(shutdown, 1000)
        self.proactor.run()

        # check all servers know of all partitions

        return

