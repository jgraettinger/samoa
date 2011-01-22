
import sqlalchemy as sa
import sqlalchemy.orm

from base import ModelBase
from table import Table

class LocalPartition(ModelBase):
    __tablename__ = 'local_partition'

    # mapped attributes
    table_uid = sa.Column(sa.String, sa.ForeignKey('table.uid'))
    uid = sa.Column(sa.String, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)
    ring_pos = sa.Column(sa.Integer, nullable = True)
    storage_path = sa.Column(sa.String, nullable = True)
    storage_size = sa.Column(sa.Integer, nullable = True)
    index_size = sa.Column(sa.Integer, nullable = True)

    table = sa.orm.relationship(Table,
        backref = sa.orm.backref('local_partitions'))

    def __init__(self, table_uid, uid, ring_pos, storage_path, 
            storage_size = (1<<22), index_size = 25000):

        self.table_uid = table_uid
        self.uid = uid
        self.ring_pos = ring_pos
        self.storage_path = storage_path
        self.storage_size = storage_size
        self.index_size = index_size
        return

