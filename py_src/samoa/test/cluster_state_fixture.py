
import random
import socket

from samoa.core.uuid import UUID
from samoa.core import protobuf as pb
from samoa.persistence.data_type import DataType

class ClusterStateFixture(object):

    def __init__(self, random_seed = None, state = None):

        self.rnd = random.Random(random_seed)

        if state:
            self.state = state
            self.server_uuid = UUID(self.state.local_uuid)
            self.server_port = self.state.local_port

        else:
            self.server_uuid = self.generate_uuid()
            self.server_port = self.generate_port()

            self.state = pb.ClusterState()
            self.state.set_local_uuid(self.server_uuid.to_hex())
            self.state.set_local_hostname('localhost')
            self.state.set_local_port(self.server_port)

        self.rebuild_index()

    def get_peer(self, uuid):
        return self._peer_ind.get(UUID(uuid))

    def get_table(self, uuid):
        return self._table_ind.get(UUID(uuid))

    def get_table_by_name(self, name):
        return self._table_name_ind.get(name)

    def get_partition(self, table_uuid, part_uuid):
        return self._part_ind.get((UUID(table_uuid), UUID(part_uuid)))

    def clone_peer(self, uuid = None):

        cln = ClusterStateFixture(
            random_seed = self.rnd.randint(0, 1<<32))

        cln.server_uuid = self._coerce_uuid(uuid or cln.server_uuid)

        cln.state = pb.ClusterState(self.state)
        cln.state.set_local_uuid(cln.server_uuid.to_hex())
        cln.state.set_local_port(cln.server_port)

        cln.rebuild_index()
        return cln

    def rebuild_index(self):

        self._peer_ind = {}
        self._table_ind = {}
        self._table_name_ind = {}
        self._part_ind = {}

        for peer in self.state.peer:
            self._peer_ind[UUID(peer.uuid)] = peer

        for table in self.state.table:

            self._table_ind[UUID(table.uuid)] = table
            self._table_name_ind[table.name] = table

            for part in table.partition:
                self._part_ind[(UUID(table.uuid), UUID(part.uuid))] = part

    def generate_name(self):
        return ''.join(self.rnd.choice('abcdefghijklmnopqrstuvwxyz') \
            for i in xrange(0, self.rnd.randint(4, 10)))

    def generate_uuid(self):
        return UUID.from_name(self.generate_name())

    def generate_port(self):
        while True:
            port = self.rnd.randint(1024, 65536)

            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('', port))
                return port
            except:
                continue

    def add_peer(self, uuid = None, hostname = 'localhost', port = None):

        uuid = self._coerce_uuid(uuid)
        port = port or self.generate_port()

        peer = pb.add_peer(self.state, uuid)
        peer.set_hostname(hostname)
        peer.set_port(port)

        self._peer_ind[uuid] = peer
        return peer

    def add_table(self, uuid = None, name = None, is_dropped = False):

        uuid = self._coerce_uuid(uuid)
        name = name or self.generate_name()
        repl_factor = self.rnd.randint(1, 10)
        lamport_ts = self.rnd.randint(1, 256)

        tbl = pb.add_table(self.state, uuid)

        if is_dropped:
            tbl.set_dropped(True)
        else:
            tbl.set_data_type(DataType.BLOB_TYPE.name)
            tbl.set_name(name)
            tbl.set_replication_factor(repl_factor)
            tbl.set_lamport_ts(lamport_ts)

        self._table_ind[UUID(tbl.uuid)] = tbl
        self._table_name_ind[tbl.name] = tbl
        return tbl

    def add_dropped_partition(self, table, uuid = None, ring_pos = None):

        uuid = self._coerce_uuid(uuid)
        ring_pos = ring_pos or self.rnd.randint(0, 1<<32)

        part = pb.add_partition(table, uuid, ring_pos)
        part.set_dropped(True)

        self._part_ind[(UUID(table.uuid), uuid)] = part
        return part

    def add_local_partition(self, table, uuid = None, ring_pos = None):

        uuid = self._coerce_uuid(uuid)
        ring_pos = ring_pos or self.rnd.randint(0, 1<<32)
        lamport_ts = self.rnd.randint(1, 256)

        part = pb.add_partition(table, uuid, ring_pos)
        part.set_server_uuid(self.server_uuid.to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(lamport_ts)

        self._part_ind[(UUID(table.uuid), uuid)] = part
        return part

    def add_remote_partition(self, table, uuid = None, ring_pos = None,
        server_uuid = None):

        if not server_uuid:
            # select a remote peer to 'own' the partition
            #  add one if none yet exists

            if len(self.state.peer) == 0:
                server_uuid = UUID(self.add_peer().uuid)
            else:
                server_uuid = UUID(self.rnd.choice(self.state.peer).uuid)

        uuid = self._coerce_uuid(uuid)
        ring_pos = ring_pos or self.rnd.randint(0, 1<<32)
        server_uuid = self._coerce_uuid(server_uuid)
        lamport_ts = self.rnd.randint(1, 256)

        part = pb.add_partition(table, uuid, ring_pos)
        part.set_server_uuid(server_uuid.to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(lamport_ts)

        self._part_ind[(UUID(table.uuid), uuid)] = part
        return part

    def _coerce_uuid(self, uuid):

        if not uuid:
            return self.generate_uuid() 

        return UUID(uuid)

