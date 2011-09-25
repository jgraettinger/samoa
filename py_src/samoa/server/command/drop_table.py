
import time
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler

class DropTableHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, rstate, local_state):

        table_uuid = rstate.get_table().get_uuid()

        pb_table = protobuf.find_table(local_state, table_uuid)

        if not pb_table:
            # race condition check
            raise KeyError(req.table_uuid)

        pb_table.Clear()
        pb_table.set_uuid(table_uuid.to_hex())
        pb_table.set_dropped(True)
        pb_table.set_dropped_timestamp(int(time.time()))

        self.log.info('dropped table %s' % table_uuid)

        return True

    def handle(self, rstate):

        if not rstate.get_table():
            rstate.send_client_error(400, 'expected table name / UUID')
            yield

        try:
            commit = yield rstate.get_context().cluster_state_transaction(
                functools.partial(self._transaction, rstate))
        except KeyError:
            rstate.send_client_error(404, 'table %s' % req.table_uuid)
            yield

        if commit:
            # TODO: notify peers of change
            pass

        rstate.flush_client_response()
        yield

