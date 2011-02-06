
import getty

import samoa.model
from table import Table

class TableSet(object):

    @getty.requires(
        meta = samoa.model.Meta)
    def __init__(self, meta):

        self.meta = meta
        self._tables = {}
        self._dropped = set()

        # Pull table state from database
        session = meta.new_session()
        for db_table in session.query(samoa.model.Table):

            if db_table.dropped:
                self._dropped.add(db_table.uid)
                continue

            self._tables[db_table.uid] = Table(db_table)

        return

    @property
    def tables(self):
        return self._tables.values()

    def get(self, table_uid):
        return self._tables[table_uid]

