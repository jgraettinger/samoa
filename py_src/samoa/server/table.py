
import bisect
import getty

from samoa.core import UUID
import samoa.model.partition
import samoa.persistence

import _server

from remote_partition import RemotePartition
from local_partition import LocalPartition

class Table(_server.Table):

    def __init__(self, model, partitions, dropped_partitions):
        assert not model.dropped
        _server.Table.__init__(
            self,
            model.uuid,
            model.name,
            samoa.persistence.DataType.names[model.data_type],
            model.replication_factor,
            partitions,
            dropped_partitions)

    @classmethod
    def build_table(kls, model, context, prev_table):

        partitions = []
        dropped_partitions = []

        for pmodel in model.partitions:

            if pmodel.dropped:
                dropped_partitions.add(pmodel.uuid)
                continue

            prev_part = prev_table and prev_table.get_partition(
                pmodel.uuid)

            if pmodel.server_uuid == context.get_server_uuid():
                # this partition is managed locally
                partitions.append(
                    LocalPartition(pmodel, prev_part))

            else:
                # this partition is managed remotely
                partitions.append(
                    RemotePartition(pmodel, prev_part))

        return kls(model, partitions, dropped_partitions)

