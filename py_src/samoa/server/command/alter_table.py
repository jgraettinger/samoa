
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler
from samoa.request.state_exception import StateException

class AlterTableHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, rstate, local_state):

        req = rstate.get_samoa_request().alter_table
        table_uuid = rstate.get_table().get_uuid()

        pb_table = protobuf.find_table(local_state, table_uuid)

        if not pb_table:
            # subtle race condition here
            raise StateException(404, "table already dropped")

        modified = False

        # check for name change
        if req.has_name() and req.name != pb_table.name:
            self.log.info('altered table %s (name %r => %r)' % (
                table_uuid, pb_table.name, req.name))

            pb_table.set_name(req.name)
            modified = True

        # check for replication_factor change
        if req.has_replication_factor() and \
            req.replication_factor != pb_table.replication_factor:

            self.log.info('altered table %s (replication factor %r => %r)' % (
                table_uuid, pb_table.replication_factor,
                req.replication_factor))

            pb_table.set_replication_factor(req.replication_factor)
            modified = True

        # check for consistency_horizon change
        if req.has_consistency_horizon() and \
            req.consistency_horizon != pb_table.consistency_horizon:

            self.log.info('altered table %s (consistency horizon %r => %r)' % (
                table_uuid, pb_table.consistency_horizon,
                req.consistency_horizon))

            pb_table.set_consistency_horizon(req.consistency_horizon)
            modified = True

        if modified:
            pb_table.set_lamport_ts(pb_table.lamport_ts + 1)

        return modified

    def handle(self, rstate):

        try:

            rstate.load_table_state()

            if not rstate.get_samoa_request().alter_table:
                raise StateException(400, 'alter_table missing')

            commit = yield rstate.get_context().cluster_state_transaction(
                functools.partial(self._transaction, rstate))

            if commit:
                # TODO: notify peers of change
                pass

            rstate.flush_response()
            yield

        except StateException, e:
            rstate.send_error(e.code, e.message)
            yield

