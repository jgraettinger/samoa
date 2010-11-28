
import samoa

class LocalPartition(object):

    def __init__(self, server, model):
        self.uid = model.uid
        self.ring_pos = model.ring_pos
        self._table = samoa.MappedRollingHash(
            model.table_path, model.table_size, model.index_size)
        return

    def get(self, key):
        return self._table.get(key)

    def set(self, key, value):
        self._table.migrate_head()
        self._table.migrate_head()
        self._table.set(key, value)
        return

