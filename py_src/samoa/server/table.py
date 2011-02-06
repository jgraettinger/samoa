
import bisect
import getty

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(object):

    def __init__(self, model):

        self.uid = model.uid
        assert not model.dropped

        self.ring_size = model.ring_size
        self.repl_factor = model.repl_factor
        self._ring = []

        self._local = {}
        self._remote = {}
        self._dropped = set()

        for pmodel in model.local_partitions:
            if pmodel.dropped:
                self._dropped.add(pmodel.uid)
                continue

            partition = LocalPartition(pmodel)

            self._local[partition.uid] = partition
            self._ring.append((partition.ring_pos, partition.uid, partition))

        for pmodel in model.remote_partitions:
            if pmodel.dropped:
                self._dropped.add(pmodel.uid)
                continue

            partition = RemotePartition(pmodel)

            self._remote[partition.uid] = partition
            self._ring.append((partition.ring_pos, partition.uid, partition))

        self._ring.sort()
        return

    @property
    def local_partitions(self):
        return self._local.values()

    @property
    def remote_partitions(self):
        return self._remote.values()

