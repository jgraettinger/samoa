
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

