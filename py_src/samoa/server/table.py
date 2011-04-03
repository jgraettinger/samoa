
import bisect
import getty

from samoa.core import UUID
import samoa.model.partition

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(object):

    def __init__(self, model, context, prev_table):

        self.uuid = model.uuid
        assert not model.dropped
        self.name = model.name
        self.replication_factor = model.replication_factor

        self._ring = []
        self._local = {}
        self._remote = {}
        self._dropped_partitions = set()

        for pmodel in model.partitions:

            if pmodel.dropped:
                self._dropped_partitions.add(pmodel.uuid)
                continue

            prev_part = prev_table and prev_table.get_partition(
                pmodel.uuid)

            if pmodel.server_uuid == context.get_server_uuid():
                # this partition is managed locally

                partition = LocalPartition(pmodel, self, prev_part)
                self._local[partition.uuid] = partition

            else:
                # this partition is managed remotely

                partition = RemotePartition(pmodel, self, prev_part)
                self._remote[partition.uuid] = partition

            self._ring.append((partition.ring_position,
                partition.uuid, partition))

        self._ring.sort()
        return

    def get_partition(self, part_uuid):
        return self._local.get(part_uuid) or self._remote.get(part_uuid)

    def get_partitions(self):
        return self._local.values() + self._remote.values()

    def get_dropped_partition_uuids(self):
        return self._dropped_partitions

    def was_dropped(self, part_uuid):
        return part_uuid in self._dropped_partitions

    def get_ring_description(self):
        return [(pos, uuid, part.is_local) for (pos, uuid, part) in self._ring]

