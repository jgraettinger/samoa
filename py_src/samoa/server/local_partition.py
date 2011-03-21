
class LocalPartition(object):

    is_local = True

    def __init__(self, model):
        self.uuid = model.uuid
        assert not model.dropped

        self.server_uuid = model.server_uuid
        self.ring_position = model.ring_position
        self.storage_path = model.storage_path
        self.storage_size = model.storage_size
        self.index_size = model.index_size

