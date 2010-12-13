
class RemotePartition(object):

    is_local = False

    def __init__(self, model, peer_pool):
        self.uid = model.uid
        self.ring_pos = model.ring_pos

        self._remote_srv = peer_pool.get_peer(
            model.remote_host, model.remote_port)

    @property
    def is_online(self):
        return self._remote_srv.is_online

