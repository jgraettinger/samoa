
import sqlalchemy as sa
import sqlalchemy.orm

from base import ModelBase
from table import Table

class RemotePartition(ModelBase):
    __tablename__ = 'remote_partion'

    # mapped attributes
    table_uid = sa.Column(sa.String, sa.ForeignKey('table.uid'))
    uid = sa.Column(sa.String, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)
    ring_pos = sa.Column(sa.Integer, nullable = True)
    remote_host = sa.Column(sa.String, nullable = True)
    remote_port = sa.Column(sa.Integer, nullable = True)

    table = sa.orm.relationship(Table,
        backref = sa.orm.backref('remote_partitions'))

    def __init__(self, table_uid, uid, ring_pos, remote_host, remote_port):

        self.table_uid = table_uid
        self.uid = uid
        self.ring_pos = ring_pos
        self.remote_host = remote_host
        self.remote_port = remote_port
        return

