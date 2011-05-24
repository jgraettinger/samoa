
import _server 

class RemotePartition(_server.RemotePartition):

    def __init__(self, model, prev_partition):
        _server.RemotePartition.__init__(
            self,
            model.uuid,
            model.server_uuid,
            model.ring_position,
            model.consistent_range_begin,
            model.consistent_range_end,
            model.lamport_ts)

