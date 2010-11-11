
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

    # default initialization
    is_local = False
    _peer = None

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

    def start(self, server):
        key = (self.remote_host, self.remote_port)

        self._peer = server.peers.get(key)
        if not self._peer:
            self._peer = server.peers[key] = samoa.RemoteServer(
                self.remote_host, self.remote_port)
        return

