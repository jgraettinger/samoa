
import sqlalchemy as sa
import samoa
from meta_db import MetaTableBase

from table import Table

class RemotePartition(MetaTableBase):
    __tablename__ = 'remote_partion'

    # mapped attributes
    uid = sa.Column(sa.String, primary_key = True)
    ring_pos = sa.Column(sa.Integer, nullable = True)
    remote_host = sa.Column(sa.String, nullable = True)
    remote_port = sa.Column(sa.Integer, nullable = True)

    table = sa.relationship(Table,
        backref = sa.backref('remote_partitions'))

    def __init__(
            self,
            uid,
            ring_pos,
            remote_host,
            remote_port
        ):
        self.uid = uid
        self.ring_pos = ring_pos
        self.remote_host = remote_host
        self.remote_port = remote_port
        return

