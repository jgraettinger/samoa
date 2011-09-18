
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

    def _transaction(self, rstate, local_state):

        table_uuid = rstate.get_table().get_uuid()
        part_uuid = rstate.get_primary_partition().get_uuid()
        
        pb_table = protobuf.find_table(local_state, table_uuid)

        if not pb_table:
            # race condition check
            raise KeyError('table %s' % table_uuid)

        pb_part = protobuf.find_partition(pb_table, part_uuid)

        if not pb_part:
            # race condition check
            raise KeyError('partition %s' % part_uuid)

        ring_pos = pb_part.ring_position

        pb_part.Clear()
        pb_part.set_uuid(part_uuid.to_hex())
        pb_part.set_ring_position(ring_pos)
        pb_part.set_dropped(True)
        pb_part.set_dropped_timestamp(int(time.time()))

        self.log.info('dropped partition %s (table %s)' % (
            part_uuid, table_uuid))

        return True

    def handle(self, rstate):

        if not rstate.get_table():
            rstate.send_client_error(400, 'expected table name / UUID')
            yield

        if not rstate.get_primary_partition():
            rstate.send_client_error(400, 'expected partition UUID')
            yield

        try:
            commit = yield rstate.get_context().cluster_state_transaction(
                functools.partial(self._transaction, rstate))
        except KeyError, exc:
            rstate.send_client_error(404, exc.message)
            yield

        if commit:
            # TODO: notify peers of change
            pass

        rstate.finish_client_response()
        yield

