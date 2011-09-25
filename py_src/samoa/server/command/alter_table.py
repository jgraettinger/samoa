
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler

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
            raise KeyError('table %s' % table_uuid)

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

        if not rstate.get_samoa_request().alter_table:
            rstate.send_client_error(400, 'alter_table missing')
            yield

        if not rstate.get_table():
            rstate.send_client_error(400, 'expected table name or UUID')
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

        rstate.flush_client_response()
        yield

