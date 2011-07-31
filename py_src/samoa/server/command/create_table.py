
import logging
import functools
import getty

from samoa.core import protobuf
from samoa.core.uuid import UUID
from samoa.server.command_handler import CommandHandler
from samoa.persistence.data_type import DataType

class CreateTableHandler(CommandHandler):

    @getty.requires(log = logging.Logger)
    def __init__(self, log):
        CommandHandler.__init__(self)
        self.log = log

    def _transaction(self, client, local_state):

        tbl_req = client.get_request().create_table

        # check if another table exists with this name
        cluster_state = client.get_context().get_cluster_state()
        if cluster_state.get_table_set().get_table_by_name(tbl_req.name):
            raise NameError('table %s exists' % tbl_req.name)

        table = protobuf.add_table(local_state, UUID.from_random())
        table.set_data_type(tbl_req.data_type)
        table.set_name(tbl_req.name)
        table.set_replication_factor(tbl_req.replication_factor)
        table.set_lamport_ts(1)

        self.log.info('created table %s' % table.uuid)

        # update response with new UUID, & return
        tbl_resp = client.get_response().mutable_create_table()
        tbl_resp.set_table_uuid(table.uuid)
        return True

    def handle(self, client):

        tbl_req = client.get_request().create_table

        if not tbl_req:
            client.send_error(400, 'create_table missing')
            yield

        if tbl_req.data_type not in DataType.names:
            client.send_error(406, 'invalid data type %s' % tbl_req.data_type)
            yield

        try:
            commit = yield client.get_context().cluster_state_transaction(
                functools.partial(self._transaction, client))
        except NameError, exc:
            client.send_error(409, exc.message)
            yield

        if commit:
            # TODO: notify peers of change
            pass

        client.finish_response()
        yield

