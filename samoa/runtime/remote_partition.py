
class RemotePartition(object):

    def __init__(self, server, model):
        self.uid = model.uid
        self.ring_pos = model.ring_pos

        self._peer = server.get_peer(
            model.remote_host, model.remote_port)

