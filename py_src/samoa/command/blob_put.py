
import samoa.command

import samoa.core
from samoa.core import protobuf

class BlobPut(object):

    def __init__(self, table_uuid, key, value, version_tag):
        samoa.command.Command.__init__(self)

        self.table_uuid = table_uuid
        self.key = key
        self.value = value
        self.version_tag = version_tag

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.BLOB_PUT
        put_req = request.mutable_blob_put()

        put_req.table_uuid = self.table_uuid
        put_req.key = self.key
        put_req.value_length = len(self.value)
        put_req.version_tag = self.version_tag

        server.start_request()
        server.write_interface().queue_write(self.value)
        yield 

    def _read_response(self, response, server):
        if response.blob_put.success == True:
            yield True
        yield False


class BlobPutHandler(samoa.command.CommandHandler):

    def _handle(self, client):

        context = client.get_context()
        request = client.get_request()
        response = client.get_response()

        put_req = client.get_request().blob_put

        table = context.get_table(
            samoa.core.UUID.from_hex_str(put_req.table_uuid))

        if not table:
            client.set_error("blob_put", "no such table", False)
            yield

        


