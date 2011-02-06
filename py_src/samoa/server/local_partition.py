
class LocalPartition(object):

    is_local = True

    def __init__(self, model):
        self.uid = model.uid
        self.ring_pos = model.ring_pos
        return

