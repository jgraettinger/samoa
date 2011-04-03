
import sqlalchemy as sa
import sqlalchemy.orm

import samoa.core
from base import ModelBase, UUIDType

import table
import server

class Partition(ModelBase):
    __tablename__ = 'partition'

    # mapped attributes
    uuid = sa.Column(UUIDType, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)

    # immutable fields
    table_uuid = sa.Column(UUIDType, sa.ForeignKey('table.uuid'))
    server_uuid = sa.Column(UUIDType, sa.ForeignKey('server.uuid'))
    ring_position = sa.Column(sa.Integer, nullable = False)

    # mutable fields, tagged with lamport_ts
    consistent_range_begin = sa.Column(sa.Integer, nullable = False)
    consistent_range_end = sa.Column(sa.Integer, nullable = False)
    lamport_ts = sa.Column(sa.Integer, nullable = False)

    # local fields
    storage_path = sa.Column(sa.String, nullable = True)
    storage_size = sa.Column(sa.Integer, nullable = True)
    index_size = sa.Column(sa.Integer, nullable = True)

    # foreign-key relationships

    table = sa.orm.relationship(table.Table,
        backref = sa.orm.backref('partitions'))

    server = sa.orm.relationship(server.Server,
        backref = sa.orm.backref('server'))

    def __init__(self, uuid, table_uuid, server_uuid, ring_position,
        consistent_range_begin, consistent_range_end, lamport_ts,
        storage_path, storage_size = (1<<22), index_size = 25000):

        self.uuid = uuid
        self.table_uuid = table_uuid
        self.server_uuid = server_uuid
        self.ring_position = ring_position

        self.consistent_range_begin = consistent_range_begin
        self.consistent_range_end = consistent_range_end
        self.lamport_ts = lamport_ts

        self.storage_path = storage_path
        self.storage_size = storage_size
        self.index_size = index_size

