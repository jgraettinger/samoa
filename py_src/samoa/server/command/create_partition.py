
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
            # race condition
            client.set_error(404, 'table %s' % req.table_uuid)
            return False

        part = protobuf.add_partition(table,
            UUID.from_random(), req.ring_position)

        part.set_server_uuid(client.get_context().get_server_uuid().to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(1)

        self.log.info('created partition %s (table %s)' % (
            part.uuid, table.uuid))

        # update repsonse with new UUID, & return
        part_resp = client.get_response().mutable_create_partition()
        part_resp.set_partition_uuid(part.uuid)
        return True

    def handle(self, client):

        req = client.get_request().create_partition

        if not req:
            client.set_error('400', 'create_partition missing')
            client.finish_response()
            yield

        if not UUID.check_hex(req.table_uuid):
            client.set_error(400, 'malformed UUID %s' % req.table_uuid)
            client.finish_response()
            yield

        commit = yield client.get_context().cluster_state_transaction(
            functools.partial(self._transaction, client))

        if commit:
            # TODO: notify peers of change
            pass

        client.finish_response()
        yield

