
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

    def _transaction(self, client, local_state):

        req = client.get_request().drop_table

        table = protobuf.find_table(local_state, UUID(req.table_uuid))

        if not table:
            # race condition
            client.set_error(404, 'table %s' % req.table_uuid)
            return False

        table.Clear()
        table.set_uuid(req.table_uuid)
        table.set_dropped(True)
        table.set_dropped_timestamp(int(time.time()))

        self.log.info('dropped table %s' % table.uuid)

        return True

    def handle(self, client):

        req = client.get_request().drop_table

        if not req:
            client.set_error(400, 'drop_table missing')
            client.finish_response()
            yield

        if not UUID.check_hex(req.table_uuid):
            client.set_error(400, 'malformed UUID %s' % req.table_uuid)
            client.finish_response()
            yield

        cluster_state = client.get_context().get_cluster_state()
        table = cluster_state.get_table_set().get_table(UUID(req.table_uuid))

        if not table:
            client.set_error(404, 'table %s' % req.table_uuid)
            client.finish_response()
            yield

        commit = yield client.get_context().cluster_state_transaction(
            functools.partial(self._transaction, client))

        if commit:
            # TODO: notify peers of change
            pass

        client.finish_response()
        yield

