
import os
import getty

import samoa.command
from samoa.core import protobuf
from samoa.server import cluster_state

class CreatePartition(samoa.command.Command):

    def __init__(self, table_uuid, ring_position,
            storage_size, index_size):
        samoa.command.Command.__init__(self)

        self.table_uuid = table_uuid
        self.ring_position = ring_position
        self.storage_size = storage_size
        self.index_size = index_size

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.CREATE_PARTITION
        part_req = request.mutable_create_partition()

        part_req.table_uuid = self.table_uuid.to_hex_str()
        part_req.ring_position = self.ring_position
        part_req.storage_size = self.storage_size
        part_req.index_size = self.index_size
        yield

    def _read_response(self, response, server):
        part_req_cpy = protobuf.CreatePartitionResponse()
        part_req_cpy.CopyFrom(response.create_partition)
        yield part_req_cpy

class CreatePartitionHandler(samoa.command.CommandHandler):

    @getty.requires(
        partition_path = getty.Config('partition_path'))
    def __init__(self, partition_path):
        samoa.command.CommandHandler.__init__(self)

        self._partition_path = partition_path
        if not os.access(self._partition_path, os.W_OK):
            raise RuntimeError('%s is not writable' % self._partition_path)

    def _create_partition_transaction(self, context, session, client):

        part_req = client.get_request().create_partition

        part_uuid = samoa.core.UUID.from_random()
        table_uuid = samoa.core.UUID.from_hex_str(part_req.table_uuid)

        storage_path = os.path.join(self._partition_path,
            '%s.part' % part_uuid.to_hex_str())

        model = samoa.model.Partition(
            uuid = part_uuid,
            table_uuid = table_uuid,
            server_uuid = context.get_server_uuid(),
            ring_position = part_req.ring_position,
            consistent_range_begin = part_req.ring_position,
            consistent_range_end = part_req.ring_position,
            lamport_ts = 0,
            storage_path = storage_path,
            storage_size = part_req.storage_size,
            index_size = part_req.index_size)

        session.add(model)
        context.log.info('%s added new partition %r (table %r)' % (
            client, model.uuid, model.table_uuid))

        part_resp = client.get_response().mutable_create_partition()
        part_resp.uuid = part_uuid.to_hex_str()

        # is dirty, and should notify peers
        yield True, True

    def _handle(self, client):

        context = client.get_context()
        part_req = client.get_request().create_partition

        table = context.get_table(
            samoa.core.UUID.from_hex_str(part_req.table_uuid))

        if not table:
            client.set_error("create_partition", "no such table", False)
            yield

        yield context.cluster_state_transaction(
                self._create_partition_transaction, client)
        yield

