
from samoa.core import protobuf

import command

class Get(command.Command):

    def __init__(self, table_uuid, key):
        self.table_uuid = table_uuid
        self.key = key

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.GET
        get_req = request.mutable_get_request()

        get_req.table_uuid = self.table_uuid.to_hex_str()
        get_req.key = self.key
        yield

    def _read_response(self, response, server):
        if not response.get.found:
            yield None

        value = yield server.read_data(response.get.value_length)
        yield value


class GetHandler(command.CommandHandler):

    def _handle(self, client):

        get_req = client.get_request().get

        cluster_state = client.get_context().get_cluster_state()

        table = cluster_state.get_table(
            samoa.core.UUID.from_hex_str(get.table_uuid))

        if not table:
            client.set_error("get", "no such table", False)
            yield

        partitions = table.route_key(get_req.key)

        # try to find a local partition to handle the request
        for part in partitions:
            if part.is_local and partition.is_online:
                yield partition.get(get_req.key)

        # else, route remotely
        cmd = Get(table.uuid, get_req.key).request_of(
        result = yield cluster_state.get_peer_set().request_of(
            partitions[0].server_uuid, cmd)
        yield result

