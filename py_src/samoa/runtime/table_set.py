
import getty

import samoa.model
from table import Table
from peer_pool import PeerPool

class TableSet(object):

    @getty.requires(
        meta_db = samoa.model.Meta,
        peer_pool = PeerPool)
    def __init__(self, meta_db, peer_pool):

        self._db = meta_db
        self._peer_pool = peer_pool
        self._tables = {}
        self._dropped = set()

        for model in meta_db.session.query(samoa.model.Table):
            if model.dropped:
                self._dropped.add(model.uid)
                continue

            self._tables[model.uid] = Table(model, self._peer_pool)

        return

    def get(self, table_uid):
        return self._tables[table_uid]

