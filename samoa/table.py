
import bisect
import getty

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(object):
    __tablename__ = 'table'

    # mapped attributes
    uid = sa.Column(sa.String, primary_key = True)
    name = sa.Column(sa.String, nullable = False)
    ring_size = sa.Column(sa.Integer, nullable = False)

    # LocalPartition maps relationship 'table', with
    #   backreference 'local_partitions'
    # RemotePartition maps relationship 'table', with
    #   backreference 'remote_partitions'

    def __init__(self, uid, name, ring_size):
        self.uid = uid
        self.name = name
        self.ring_size = ring_size
        return

    def start(self):

        self._ring = []

        for partition in self.local_partitions:
            partition.start()
            self._ring.append((partition.ring_pos, partition.uid, partition))

        for partition in self.remote_partitions:
            partition.start()
            self._ring.append((partition.ring_pos, partition.uid, partition))

        self._ring.sort()
        return

    def route_key(self, key):
        



