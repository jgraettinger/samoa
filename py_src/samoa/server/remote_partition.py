
class RemotePartition(object):

    is_local = False

    def __init__(self, model):
        self.uid = model.uid
        self.ring_pos = model.ring_pos
        self.host = model.remote_host
        self.port = model.remote_port
        return

