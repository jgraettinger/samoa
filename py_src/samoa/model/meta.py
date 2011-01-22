
import getty
import sqlalchemy
import sqlalchemy.orm

from base import ModelBase

class Meta(object):

    @getty.requires(db_uri = getty.Config('db_uri'))
    def __init__(self, db_uri):

        self._db_uri = db_uri
        self._engine = sqlalchemy.create_engine(
            'sqlite:///%s' % db_uri, echo = False)

        # Enable enforcement of SQLite Foreign Keys
        self._engine.execute('PRAGMA foreign_keys = ON;')

        self._metadata = ModelBase.metadata
        self._metadata.create_all(self._engine)

        self._sessionmaker = sqlalchemy.orm.sessionmaker(
            bind = self._engine)

        return

    def new_session(self):
        return self._sessionmaker()

