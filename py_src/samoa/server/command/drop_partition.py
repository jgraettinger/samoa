
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
            raise KeyError('table %s' % req.table_uuid)

        part = protobuf.find_partition(table, UUID(req.partition_uuid))

        if not part:
            raise KeyError('partition %s' % req.partition_uuid)

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
            client.send_error(400, 'drop_partition missing')
            yield

        if not UUID.check_hex(req.table_uuid):
            client.send_error(400, 'malformed UUID %s' % req.table_uuid)
            yield

        if not UUID.check_hex(req.partition_uuid):
            client.send_error(400, 'malformed UUID %s' % req.partition_uuid)
            yield

        cluster_state = client.get_context().get_cluster_state()
        table = cluster_state.get_table_set().get_table(UUID(req.table_uuid))

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

