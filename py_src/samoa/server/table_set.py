
import getty

from samoa.core import UUID

import samoa.model.meta
import samoa.model.table
from table import Table

class TableSet(object):

    def __init__(self, context, session, prev_table_set):

        self._tables = {}
        self._by_name = {}
        self._dropped = set()

        # Pull table state from database
        for model in session.query(samoa.model.Table):

            if model.dropped:
                self._dropped.add(model.uuid)
                continue

            table = Table.build_table(model, context,
                prev_table_set and prev_table_set.get_table(model.uuid))

            self._tables[model.uuid] = table
            self._by_name[model.name] = table

    def get_tables(self):
        return self._tables.values()

    def get_table(self, table_uuid):
        return self._tables.get(table_uuid)

    def get_table_by_name(self, table_name):
        return self._by_name.get(table_name)

    def was_dropped(self, table_uuid):
        return table_uuid in self._dropped

    def get_dropped_table_uuids(self):
        return self._dropped

