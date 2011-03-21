
import getty

from samoa.core import UUID

import samoa.model.meta
import samoa.model.table
from table import Table

class TableSet(object):

    def __init__(self, context, session, prev_table_set):

        self._tables = {}
        self._dropped = set()

        # Pull table state from database
        session = context.new_session()
        for model in session.query(samoa.model.Table):

            if model.dropped:
                self._dropped.add(model.uuid)
                continue

            self._tables[model.uuid] = Table(model, context,
                prev_table_set and prev_table_set.get_table(model.uuid))

    def get_tables(self):
        return self._tables.values()

    def get_table(self, table_uuid):
        return self._tables.get(table_uuid)

    def was_dropped(self, table_uuid):
        return table_uuid in self._dropped

    def get_dropped_table_uuids(self):
        return self._dropped

