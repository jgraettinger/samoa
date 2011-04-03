
class RemotePartition(object):

    is_local = False

    def __init__(self, model, table, prev_partition):
        self.uuid = model.uuid
        self.server_uuid = model.server_uuid

        self.ring_position = model.ring_position
        self.storage_path = model.storage_path
        self.storage_size = model.storage_size
        self.index_size = model.index_size

