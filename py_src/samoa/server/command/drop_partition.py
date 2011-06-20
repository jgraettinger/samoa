
import time
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler

class DropPartitionHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, client, local_state):

        req = client.get_request().drop_partition

        table = protobuf.find_table(local_state,
            UUID(req.table_uuid))

        if not table:
            # race condition
            client.set_error(404, 'table %s' % req.table_uuid)
            return False

        part = protobuf.find_partition(table, UUID(req.partition_uuid))

        if not part:
            # race condition
            client.set_error(404, 'partition %s' % req.partition_uuid)
            return False

        ring_pos = part.ring_position

        part.Clear()
        part.set_uuid(req.partition_uuid)
        part.set_ring_position(ring_pos)
        part.set_dropped(True)
        part.set_dropped_timestamp(int(time.time()))

        self.log.info('dropped partition %s (table %s)' % (
            part.uuid, table.uuid))

        return True

    def handle(self, client):

        req = client.get_request().drop_partition

        if not req:
            client.set_error(400, 'drop_partition missing')
            client.finish_response()
            yield

        if not UUID.check_hex(req.table_uuid):
            client.set_error(400, 'malformed UUID %s' % req.table_uuid)
            client.finish_response()
            yield

        if not UUID.check_hex(req.partition_uuid):
            client.set_error(400, 'malformed UUID %s' % req.partition_uuid)
            client.finish_response()
            yield

        cluster_state = client.get_context().get_cluster_state()
        table = cluster_state.get_table_set().get_table(UUID(req.table_uuid))

        if not table:
            client.set_error(404, 'table %s' % req.table_uuid)
            client.finish_response()
            yield

        if not table.get_partition(UUID(req.partition_uuid)):
            client.set_error(404, 'partition %s' % req.partition_uuid)
            client.finish_response()
            yield

        commit = yield client.get_context().cluster_state_transaction(
            functools.partial(self._transaction, client))

        if commit:
            # TODO: notify peers of change
            pass

        client.finish_response()
        yield

