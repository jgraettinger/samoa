
import samoa.command
from samoa.persistence import data_types_str_to_int, data_types_int_to_str
from samoa.core import protobuf
from samoa.server import cluster_state

class DeclareTable(samoa.command.Command):

    def __init__(self, name, data_type = 'blob', replication_factor = 1,
        lamport_consistency_bound = 3 * 24 * 60 * 60,
        create_if_not_exists = True):

        samoa.command.Command.__init__(self)
        self.name = name
        self.data_type = data_type
        self.replication_factor = replication_factor
        self.lamport_consistency_bound = lamport_consistency_bound
        self.create_if_not_exists = create_if_not_exists

    def _write_request(self, request, server):
        request.type = protobuf.CommandType.DECLARE_TABLE
        decl_tbl = request.mutable_declare_table()

        decl_tbl.name = self.name
        decl_tbl.data_type = self.data_type
        decl_tbl.replication_factor = self.replication_factor
        decl_tbl.lamport_consistency_bound = self.lamport_consistency_bound
        decl_tbl.create_if_not_exists = self.create_if_not_exists
        yield

    def _read_response(self, response, server):
        decl_tbl_cpy = protobuf.DeclareTableResponse()
        decl_tbl_cpy.CopyFrom(response.declare_table)
        yield decl_tbl_cpy

class DeclareTableHandler(samoa.command.CommandHandler):

    def _add_table_transaction(self, context, session, client):

        tbl_req = client.get_request().declare_table

        # check table wasn't created while waiting to enter transaction
        if context.get_table_by_name(tbl_req.name):
            yield False, False

        model = samoa.model.Table(
            uuid = samoa.core.UUID.from_random(),
            name = tbl_req.name,
            data_type = data_types_str_to_int[tbl_req.data_type],
            replication_factor = tbl_req.replication_factor,
            lamport_consistency_bound = tbl_req.lamport_consistency_bound)

        session.add(model)
        context.log.info('%s added new table %r (%r)' % (
            client, model.name, model.uuid))

        # is dirty, and should notify peers
        yield True, True

    def _handle(self, client):

        context = client.get_context()
        tbl_req = client.get_request().declare_table
        tbl_resp = client.get_response().mutable_declare_table()

        if tbl_req.data_type not in data_types_str_to_int:
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

        tbl_resp.uuid = table.uuid.to_hex_str()
        tbl_resp.name = table.name
        tbl_resp.data_type = data_types_int_to_str[table.data_type]
        tbl_resp.replication_factor = table.replication_factor
        tbl_resp.lamport_consistency_bound = table.lamport_consistency_bound
        yield

