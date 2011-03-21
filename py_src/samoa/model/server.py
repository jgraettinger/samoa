
import sqlalchemy as sa

from base import ModelBase, UUIDType

class Server(ModelBase):
    __tablename__ = 'server'

    # mapped attributes
    uuid = sa.Column(UUIDType, primary_key = True)
    hostname = sa.Column(sa.String)
    port = sa.Column(sa.Integer)

    # Partition maps relationship 'server', with
    #   backreference 'partitions'

    def __init__(self, uuid, hostname, port):
        self.uuid = uuid
        self.hostname = hostname
        self.port = port

