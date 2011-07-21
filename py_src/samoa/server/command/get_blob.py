
import logging
import functools
import getty

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.server.local_partition import LocalPartition
from samoa.server.command_handler import CommandHandler

class GetBlobHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _persister_callback(self, client, record):

        msg = client.get_response()
        get_blob = msg.mutable_get_blob()

        if not record:
            get_blob.set_found(0)
            return

        get_blob.set_found(1)
        msg.add_data_block_length(record.value_length())

        client.start_response()
        client.write_interface().queue_write(record.value)
        return

    def handle(self, client):
        req = client.get_request().get_blob

        if not req:
            client.set_error(400, 'get_blob missing')
            client.finish_response()
            yield

        if not UUID.check_hex(req.table_uuid):
            client.set_error(400, 'malformed UUID %s' % req.table_uuid)
            client.finish_response()
            yield

        cluster_state = client.get_context().get_cluster_state()
        table = cluster_state.get_table_set().get_table(
            UUID(req.table_uuid))

        if not table:
            client.set_error(404, 'table %s' % req.table_uuid)
            client.finish_response()
            yield

        partitions = table.route_key(req.key)

        if not partitions:
            client.set_error(404, 'no table partitions')
            client.finish_response()
            yield

        # are any partitions local?
        for part in partitions:
            if not isinstance(part, LocalPartition):
                continue

            persister = part.get_persister()

            yield persister.get(functools.partial(
                self._persister_callback, client), req.key)

            # all done... ?
            client.finish_response()
            yield

        # route to closest peer
        peer_srv = None

        peer_set = cluster_state.get_peer_set()
        for part in partitions:
            srv = peer_set.get_server(part.get_server_uuid())

            if not srv:
                continue

            if not peer_srv or \
                peer_srv.get_latency_ms() > srv.get_latency_ms():

                peer_srv = srv

        if not peer_srv:
            client.set_error(503, 'no peer available')
            client.finish_response()
            yield

        peer_req = yield peer_srv.schedule_request()
        peer_req.get_message().set_type(CommandType.GET_BLOB)
        peer_req.get_message().mutable_get_blob().CopyFrom(req)
        peer_resp = yield peer_req.finish_request()

        client.get_response().CopyFrom(peer_resp.get_message())

        block_lengths = list(peer_resp.get_message().data_block_length)
        client.start_response()

        for b_len in block_lengths:
            block = yield peer_resp.read_data(b_len)
            client.queue_write(block)
            yield client.write_queued()

        client.finish_response()
        yield


