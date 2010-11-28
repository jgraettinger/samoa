

import sqlalchemy as sa
from meta_db import MetaTableBase

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(MetaTableBase):
    __tablename__ = 'table'

    # mapped attributes
    uid = sa.Column(sa.String, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)
    ring_size = sa.Column(sa.Integer, nullable = False)
    repl_factor = sa.Column(sa.Integer, nullable = False) 

    # LocalPartition maps relationship 'table', with
    #   backreference 'local_partitions'
    # RemotePartition maps relationship 'table', with
    #   backreference 'remote_partitions'

    def __init__(self, uid, ring_size, repl_factor, dropped = False):
        self.uid = uid
        self.dropped = dropped
        self.ring_size = ring_size
        self.repl_factor = repl_factor
        return

