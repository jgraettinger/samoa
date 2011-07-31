
from _command import SetBlobHandler

"""
import logging
import functools
import getty

from samoa.core.protobuf import CommandType
from samoa.core.uuid import UUID
from samoa.server.local_partition import LocalPartition
from samoa.server.command_handler import CommandHandler

class SetBlobHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _persister_callback(self, client, partition_uuid,
        blob_value, old_record, new_record):

        


        msg = client.get_response()
        set_blob = msg.mutable_set_blob()



        if not record:
            set_blob.set_found(0)
            return

        set_blob.set_found(1)
        msg.add_data_block_length(record.value_length())

        client.start_response()
        client.write_interface().queue_write(record.value)
        return

    def handle(self, client):

        msg = client.get_request()
        set_blob = msg.set_blob

        if not set_blob:
            client.set_error(400, 'set_blob missing')
            client.finish_response()
            yield

        if not UUID.check_hex(set_blob.table_uuid):
            client.set_error(400, 'malformed UUID %s' % set_blob.table_uuid)
            client.finish_response()
            yield

        cluster_state = client.get_context().get_cluster_state()
        table = cluster_state.get_table_set().get_table(
            UUID(set_blob.table_uuid))

        if not table:
            client.set_error(404, 'table %s' % set_blob.table_uuid)
            client.finish_response()
            yield

        partitions = table.route_key(set_blob.key)

        if not partitions:
            client.set_error(404, 'no table partitions')
            client.finish_response()
            yield

        if len(client.get_request().data_blob_length) != 1:
            client.set_error(400, 'expected exactly 1 data blob')
            client.finish_response()
            yield

        # read the data blob
        blob_value = yield client.read_interface().read_data(
            client.get_request().data_blob_length[0])

        # are any partitions local?
        for part in partitions:
            if not isinstance(part, LocalPartition):
                continue

            persister = part.get_persister()

            yield persister.set(functools.partial(
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
        client.start_response()

        for b_len in peer_resp.get_message().data_block_length:
            block = yield peer_resp.read_interface().read_data(b_len)
            client.write_interface().queue_write(block)

        peer_resp.finish_response()
        client.finish_response()
        yield
"""
