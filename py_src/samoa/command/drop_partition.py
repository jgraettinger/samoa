
import os
import samoa.command
from samoa.core import protobuf
from samoa.server import cluster_state

class DropPartition(samoa.command.Command):

    def __init__(self, table_uuid, uuid):
        samoa.command.Command.__init__(self)

        self.table_uuid = table_uuid
        self.uuid = uuid

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.DROP_PARTITION
        part_req = request.mutable_drop_partition()

        part_req.table_uuid = self.table_uuid.to_hex_str()
        part_req.uuid = self.uuid.to_hex_str()
        yield

    def _read_response(self, response, server):
        part_req_cpy = protobuf.DropPartitionResponse()
        part_req_cpy.CopyFrom(response.drop_partition)
        yield part_req_cpy

class DropPartitionHandler(samoa.command.CommandHandler):

    def _drop_partition_transaction(self, context, session, client):

        part_req = client.get_request().drop_partition
        part_uuid = samoa.core.UUID.from_hex_str(part_req.uuid)

        model = session.query(samoa.model.Partition).filter_by(
            uuid = part_uuid).one()

        model.dropped = True

        context.log.info('%s dropped partition %r (table %r)' % (
            client, model.uuid, model.table_uuid))

        part_resp = client.get_response().mutable_drop_partition()

        # is dirty, and should notify peers
        yield True, True

    def _handle(self, client):

        context = client.get_context()
        part_req = client.get_request().drop_partition

        table_uuid = samoa.core.UUID.from_hex_str(part_req.table_uuid)
        part_uuid = samoa.core.UUID.from_hex_str(part_req.uuid)

        table = context.get_table(table_uuid)

        if not table:
            client.set_error("drop_partition", "no such table", False)
            yield

        part = table.get_partition(part_uuid)

        if not part:
            client.set_error("drop_partition", "no such partition", False)
            yield

        yield context.cluster_state_transaction(
                self._drop_partition_transaction, client)
        yield

