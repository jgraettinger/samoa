
import uuid
import struct
import random
import gevent

import gevent.socket
import gevent.event

import samoa.protobuf as spb

class RemoteError(Exception):
    def __init__(self, message):
        self.code = message.error.code
        self.message = message.error.message

    def __repr__(self):
        return 'RemoteError(%s, %s)' % (self.code, self.message)
    def __str__(self):
        return 'RemoteError(%s, %r)' % (self.code, self.message)

class Server(object):

    def __init__(self, hostname, port):
        self._sock = gevent.socket.create_connection((hostname, port))
        self._sfile = self._sock.makefile()

        self._next_request_id = 1
        self._pending = {}

        self._daemon_loop = gevent.spawn(self._daemon_loop)

    def cluster_state(self):
        request = spb.SamoaRequest()
        request.set_type(spb.CommandType.CLUSTER_STATE)
        return self._make_request(request, self._on_cluster_state)

    def _on_cluster_state(self, future, message, data_blocks):
        state = spb.ClusterState()
        state.ParseFromBytes(data_blocks[0])
        future.set(state)

    def create_table(self, name, data_type,
        replication_factor = None, consistency_horizon = None):

        request = spb.SamoaRequest()
        request.set_type(spb.CommandType.CREATE_TABLE)

        create_request = request.mutable_create_table()
        create_request.set_name(name)
        create_request.set_data_type(data_type)

        if replication_factor is not None:
            create_request.set_replication_factor(replication_factor)

        if consistency_horizon is not None:
            create_request.set_consistency_horizon(consistency_horizon)

        return self._make_request(request, self._on_create_table)

    def _on_create_table(self, future, message, data_blocks):
        future.set(uuid.UUID(message.table_uuid))

    def create_partition(self, table_name, layers, ring_position = None):

        request = spb.SamoaRequest()
        request.set_type(spb.CommandType.CREATE_PARTITION)
        request.set_table_name(table_name)

        create_request = request.mutable_create_partition()

        if ring_position is not None:
            create_request.set_ring_position(ring_position)
        else:
            create_request.set_ring_position(random.randint(0, (1<<64) - 1))

        for storage_size, index_size, file_path in layers:
            layer_request = create_request.add_ring_layer()
            layer_request.set_storage_size(storage_size)
            layer_request.set_index_size(index_size)
            layer_request.set_file_path(file_path)

        return self._make_request(request, self._on_create_partition)

    def _on_create_partition(self, future, message, data_blocks):
        future.set(uuid.UUID(message.partition_uuid))

    def _make_request(self, message, response_handler, data_blocks = []):
        message.set_request_id(self._next_request_id)
        self._next_request_id += 1

        for block in data_blocks:
            message.add_data_block_length(len(block))

        message_bytes = message.SerializeToBytes()
        message_header = struct.pack('>H', len(message_bytes))

        future = gevent.event.AsyncResult()
        self._pending[message.request_id] = (response_handler, future)

        try:
            self._sock.sendall(message_header)
            self._sock.sendall(message_bytes)

            for block in data_blocks:
                self._sock.sendall(block)

        except socket.error, e:
            self.on_error()

        return future

    def _read_response(self):

        message_header = self._sfile.read(2)
        message_length = struct.unpack('>H', message_header)[0]
        message_bytes = self._sfile.read(message_length)

        message = spb.SamoaResponse()
        message.ParseFromBytes(message_bytes)

        data_blocks = []
        for block_length in message.data_block_length:
            data_blocks.append(self._sfile.read(block_length))

        response_handler, future = self._pending[message.request_id]
        del self._pending[message.request_id]

        if message.type == spb.CommandType.ERROR:
            future.set_exception(RemoteError(message))
        else:
            response_handler(future, message, data_blocks)

    def _daemon_loop(self):
        try:
            while self._sock:
                self._read_response()
        except Exception, e:
            self._on_error(e)
            raise

    def _on_error(self, exception):
        self._sock.close()
        self._sfile = None
        self._sock = None

        for request_id, (handler, future) in self._pending.iteritems():
            future.set_exception(exception)

