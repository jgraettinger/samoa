
import samoa.command

from samoa.persistence.data_type import DataType
from samoa.core import protobuf

class DeclareTableHandler(samoa.command.CommandHandler):

    def _add_table_transaction(self, context, session, client):

        tbl_req = client.get_request().declare_table

        # check table wasn't created while waiting to enter transaction
        if context.get_table_by_name(tbl_req.name):
            yield False, False

        model = samoa.model.Table(
            uuid = samoa.core.UUID.from_random(),
            name = tbl_req.name,
            data_type = DataType.names[tbl_req.data_type],
            replication_factor = tbl_req.replication_factor)

        session.add(model)
        context.log.info('%s added new table %r (%r)' % (
            client, model.name, model.uuid))

        # is dirty, and should notify peers
        yield True, True

    def _handle(self, client):

        context = client.get_context()
        tbl_req = client.get_request().declare_table
        tbl_resp = client.get_response().mutable_declare_table()

        if tbl_req.data_type not in DataType.names:
            client.set_error("no such data-type", tbl_req.data_type, False)
            yield

        table = context.get_table_by_name(tbl_req.name)

        if not table and not tbl_req.create_if_not_exists:
            client.set_error("declare_table", "no such table", False)
            yield

        tbl_resp.created = False

        if not table:
            # table doesn't currently exist, but should be created
            yield context.cluster_state_transaction(
                self._add_table_transaction, client)

            table = context.get_table_by_name(tbl_req.name)
            tbl_resp.created = True

        tbl_resp.uuid = table.get_uuid().to_hex_str()
        tbl_resp.name = table.get_name()
        tbl_resp.data_type = table.get_data_type().name
        tbl_resp.replication_factor = table.get_replication_factor()
        yield

