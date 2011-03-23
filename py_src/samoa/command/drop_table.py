
import samoa.command
from samoa.core import protobuf
from samoa.server import cluster_state

class DropTable(samoa.command.Command):

    def __init__(self, uuid):
        samoa.command.Command.__init__(self)
        self.uuid = uuid

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.DROP_TABLE
        drop_tbl = request.mutable_drop_table()
        drop_tbl.uuid = self.uuid.to_hex_str()
        yield

    def _read_response(self, response, server):
        drop_tbl_cpy = protobuf.DropTableResponse()
        drop_tbl_cpy.CopyFrom(response.drop_table)
        yield drop_tbl_cpy

class DropTableHandler(samoa.command.CommandHandler):

    def _drop_table_transaction(self, context, session, client):

        table_uuid = samoa.core.UUID.from_hex_str(
            client.get_request().drop_table.uuid)

        # if table doesn't exist, return an error & rollback
        if not context.get_table(table_uuid):
            client.set_error("drop_table", "no such table", False)
            yield False, False

        # drop table
        model = session.query(samoa.model.Table).filter_by(
            uuid = table_uuid).one()
        model.dropped = True

        # drop table partitions as well
        session.query(samoa.model.Partition).filter_by(
            table_uuid = table_uuid).update({'dropped': True})

        context.log.info('%s dropped table %r (%r)' % (
            client, model.name, model.uuid))

        drop_resp = client.get_response().mutable_drop_table()

        # is dirty, and should notify peers
        yield True, True

    def _handle(self, client):

        context = client.get_context()
        yield context.cluster_state_transaction(
            self._drop_table_transaction, client)
        yield

