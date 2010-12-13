
import bisect
import getty

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(object):

    def __init__(self, model, peer_pool):

        self.uid = model.uid
        self.ring_size = model.ring_size
        self.repl_factor = model.repl_factor
        self._ring = []

        for partition_model in model.local_partitions:
            if partition_model.dropped:
                continue

            partition = LocalPartition(partition_model)
            self._ring.append((partition.ring_pos, partition.uid, partition))

        for partition in model.remote_partitions:
            if partition_model.dropped:
                continue

            partition = RemotePartition(partition_model)
            self._ring.append((partition.ring_pos, partition.uid, partition))

        self._ring.sort()
        return

    def route_key(self, key):
        start_ind = bisect.bisect_left(self._ring, (hash(key),))

        for i in xrange(self.repl_factor):
            yield self._ring[(start_ind + i) % len(self._ring)][2]

