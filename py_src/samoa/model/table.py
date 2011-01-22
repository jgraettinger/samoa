
import sqlalchemy as sa

from base import ModelBase

class Table(ModelBase):
    __tablename__ = 'table'

    DEFAULT_RING_SIZE = (1 << 32)

    # mapped attributes
    uid = sa.Column(sa.String, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)
    ring_size = sa.Column(sa.Integer, nullable = False)
    repl_factor = sa.Column(sa.Integer, nullable = False) 

    # LocalPartition maps relationship 'table', with
    #   backreference 'local_partitions'
    # RemotePartition maps relationship 'table', with
    #   backreference 'remote_partitions'

    def __init__(self, uid, repl_factor = 1,
        ring_size = DEFAULT_RING_SIZE, dropped = False):

        self.uid = uid
        self.dropped = dropped
        self.ring_size = ring_size
        self.repl_factor = repl_factor
        return

