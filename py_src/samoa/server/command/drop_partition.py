
import time
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler
from samoa.request.state_exception import StateException

class DropPartitionHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, rstate, local_state):

        table_uuid = rstate.get_table().get_uuid()

        part_uuid_bytes = rstate.get_samoa_request().partition_uuid

        if not part_uuid_bytes:
            raise StateException(400, 'expected partition UUID')

        part_uuid = UUID.from_bytes(part_uuid_bytes)

        if part_uuid.is_nil():
            raise StateException(400, 'invalid UUID %r' % part_uuid_bytes)

        pb_table = protobuf.find_table(local_state, table_uuid)

        if not pb_table:
            # race condition check
            raise StateError(404, "table already dropped")

        pb_part = protobuf.find_partition(pb_table, part_uuid)

        if not pb_part:
            # race condition check
            raise StateError(404, "partition already dropped")

        pb_part.set_dropped(True)
        pb_part.set_dropped_timestamp(int(time.time()))

        self.log.info('dropped partition %s (table %s)' % (
            UUID(part_uuid), UUID(table_uuid)))
        return True

    def handle(self, rstate):

        try:

            rstate.load_table_state()

            yield rstate.get_context().cluster_state_transaction(
                functools.partial(self._transaction, rstate))

            # notify peers of the change
            rstate.get_peer_set().begin_peer_discovery()

            rstate.flush_response()
            yield

        except StateException, e:
            rstate.send_error(e.code, e.message)
            yield

