
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

    def _transaction(self, rstate, local_state):

        req = rstate.get_samoa_request().create_partition
        table_uuid = rstate.get_table().get_uuid()

        pb_table = protobuf.find_table(local_state, table_uuid)

        if not pb_table:
            # subtle race condition here
            raise KeyError('table %s' % req.table_uuid)

        part = protobuf.add_partition(pb_table,
            UUID.from_random(), req.ring_position)

        part.set_server_uuid(rstate.get_context().get_server_uuid().to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(1)

        for req_rlayer in req.ring_layer:
            ring_layer = part.add_ring_layer()
            ring_layer.CopyFrom(req_rlayer)

        self.log.info('created partition %s (table %s)' % (
            part.uuid, table_uuid))

        # update repsonse with new UUID, & return
        part_resp = rstate.get_samoa_response().set_partition_uuid(
            UUID(part.uuid).to_hex())
        return True

    def handle(self, rstate):

        if not rstate.get_table():
            rstate.send_client_error(400, "expected table name or UUID")
            yield

        if not rstate.get_samoa_request().create_partition:
            rstate.send_client_error(400, "create_partition missing")
            yield

        if not len(rstate.get_samoa_request().create_partition.ring_layer):
            rstate.send_client_error(400, 'ring_layer missing')
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

