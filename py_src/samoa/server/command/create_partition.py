
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler

class CreatePartitionHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, client, local_state):

        req = client.get_request().create_partition

        table = protobuf.find_table(local_state,
            UUID(req.table_uuid))

        if not table:
            raise KeyError('table %s' % req.table_uuid)

        part = protobuf.add_partition(table,
            UUID.from_random(), req.ring_position)

        part.set_server_uuid(client.get_context().get_server_uuid().to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(1)

        for req_rlayer in req.ring_layer:
            ring_layer = part.add_ring_layer()
            ring_layer.CopyFrom(req_rlayer)

        self.log.info('created partition %s (table %s)' % (
            part.uuid, table.uuid))

        # update repsonse with new UUID, & return
        part_resp = client.get_response().mutable_create_partition()
        part_resp.set_partition_uuid(part.uuid)
        return True

    def handle(self, client):

        req = client.get_request().create_partition

        if not req:
            client.send_error(400, 'create_partition missing')
            yield

        if not UUID.check_hex(req.table_uuid):
            client.send_error(400, 'malformed UUID %s' % req.table_uuid)
            yield

        if not len(req.ring_layer):
            client.send_error(400, 'ring_layer missing')
            yield

        try:
            commit = yield client.get_context().cluster_state_transaction(
                functools.partial(self._transaction, client))
        except KeyError, exc:
            client.send_error(404, exc.message)
            yield

        if commit:
            # TODO: notify peers of change
            pass

        client.finish_response()
        yield

