
import sqlalchemy as sa
import samoa
from meta_db import MetaTableBase

from table import Table

class LocalPartition(MetaTableBase):
    __tablename__ = 'local_partition'

    # mapped attributes
    uid = sa.Column(sa.String, primary_key = True)
    ring_pos = sa.Column(sa.Integer, nullable = True)
    table_path = sa.Column(sa.String, nullable = True)
    table_size = sa.Column(sa.Integer, nullable = True)
    index_size = sa.Column(sa.Integer, nullable = True)

    table = sa.relationship(Table,
        backref = sa.backref('local_partitions'))

    # default initialization
    is_local = True
    _table = None

    def __init__(
            self,
            uid,
            ring_pos,
            table_path,
            table_size = (1<<22),
            index_size = 25000
        ):
        self.uid = uid
        self.ring_pos = ring_pos
        self.table_path = table_path
        self.table_size = table_size
        self.index_size = index_size
        return

    def start(self, server):
        self._table = samoa.MappedRollingHash(
            self.table_path, self.table_size, self.index_size)

    def close(self):
        del self._table
        self._table = None

    def get(self, key):
        return self._table.get(key)

    def set(self, key, value):
        self._table.migrate_head()
        self._table.migrate_head()
        self._table.set(key, value)

