
import samoa.core

import sqlalchemy as sa
from base import ModelBase, UUIDType

class Table(ModelBase):
    __tablename__ = 'table'

    RING_SIZE = (1 << 32)

    # mapped attributes
    uuid = sa.Column(UUIDType, primary_key = True)
    dropped = sa.Column(sa.Boolean, default = False)
    name = sa.Column(sa.String, nullable = False)
    data_type = sa.Column(sa.String, nullable = False)

    replication_factor = sa.Column(sa.Integer, nullable = False) 

    # Partition maps relationship 'table', with
    #   backreference 'partitions'

    def __init__(self,
            uuid,
            name,
            data_type,
            replication_factor):

        self.uuid = uuid
        self.name = name
        self.data_type = data_type.name
        self.replication_factor = replication_factor

    @classmethod
    def compute_ring_update(cls, tracked_partitions, unknown_partitions):
        """
        Given a collection of protobuf partition descriptions,
        e.g. from a ClusterState message, determine what updates
        (if any) to the locally tracked ring should be made.

        This may involve adding partitions from descriptions, and
        removing currently tracked remote partitions.

        Inputs:

          A list of protobuf partition descriptions

        Outputs:

          A list of protobuf partitions to be added to the ring.
          A list of server.RemotePartition to be dropped from the ring.
        """

        return (unknown_partitions, []) 

