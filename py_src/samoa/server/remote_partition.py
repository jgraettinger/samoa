
class RemotePartition(object):

    is_local = False

    def __init__(self, model, table, prev_partition):
        self.uuid = model.uuid
        self.server_uuid = model.server_uuid

        self.ring_position = model.ring_position
        self.consistent_range_begin = model.consistent_range_begin
        self.consistent_range_end = model.consistent_range_end
        self.lamport_ts = model.lamport_ts

        self.storage_path = model.storage_path
        self.storage_size = model.storage_size
        self.index_size = model.index_size

