
import functools
import getty

from samoa.server.command_handler import CommandHandler
from samoa.core import protobuf
from samoa.core.uuid import UUID

class CreatePartitionHandler(CommandHandler):

    """
    @getty.requires(
        partition_path = getty.Config('partition_path'))
    def __init__(self, partition_path):
        samoa.command.CommandHandler.__init__(self)

        self._partition_path = partition_path
        if not os.access(self._partition_path, os.W_OK):
            raise RuntimeError('%s is not writable' % self._partition_path)
    """

    def _transaction(self, client, cluster_state):

        part_req = client.get_request().create_partition

        table = protobuf.find_table(cluster_state,
            UUID(part_req.table_uuid))

        if not table:
            client.set_error(404, "table %s" % part_req.table_uuid, False)
            return False

        part = protobuf.add_partition(table,
            UUID.from_random(), part_req.ring_position)

        part.set_server_uuid(client.get_context().get_server_uuid().to_hex())
        part.set_consistent_range_begin(part.ring_position)
        part.set_consistent_range_end(part.ring_position)
        part.set_lamport_ts(1)

        # notification to client
        part_resp = client.get_response().mutable_create_partition()
        part_resp.set_uuid(part.uuid)

        # TODO: notify peers of change
        return True

    def handle(self, client):

        yield client.get_context().cluster_state_transaction(
            functools.partial(self._transaction, client))

        client.finish_response()
        yield

