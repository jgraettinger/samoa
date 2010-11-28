
import getty
import samoa

from meta_db import MetaDB
from runtime.table import Table

class TableSet(object):

    @getty.requires(meta_db = meta_db.MetaDB)
    def __init__(self, meta_db):
        self.meta_db = meta_db
        self._tables = {}
        self._dropped = set()

        for model in meta_db.session.query(Table):
            if model.dropped:
                self._dropped.add(model.uid)
                continue

            table = Table 


            self._tables[table.uid] = table
            table.start()

        return

    def get(self, table_uid):
        return self._tables[table_uid]
