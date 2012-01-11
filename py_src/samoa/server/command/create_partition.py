
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler
from samoa.request.state_exception import StateException

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
            # race condition check
            raise StateException(404, 'table already dropped')

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

        try:

            rstate.load_table_state()

            create_partition = rstate.get_samoa_request().create_partition

            if not create_partition:
                raise StateException(400, "create_partition missing")

            if not len(create_partition.ring_layer):
                raise StateException(400, 'ring_layer missing')

            yield rstate.get_context().cluster_state_transaction(
                functools.partial(self._transaction, rstate))

            # notify peers of the change
            rstate.get_peer_set().begin_peer_discovery()

            rstate.flush_response()
            yield

        except StateException, e:
            rstate.send_error(e.code, e.message)
            yield

