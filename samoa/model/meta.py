
import getty
import sqlalchemy
import sqlalchemy.orm

from base import ModelBase

class Meta(object):

    @getty.requires(db_uri = getty.Config('db_uri'))
    def __init__(self, db_uri):

        self.db_uri = db_uri
        self.engine = sqlalchemy.create_engine(
            'sqlite:///%s' % db_uri,echo = False)#True)

        # Enable enforcement of SQLite Foreign Keys
        self.engine.execute('PRAGMA foreign_keys = ON;')

        self.metadata = ModelBase.metadata
        self.metadata.create_all(self.engine)

        self.session_factory = sqlalchemy.orm.sessionmaker(
            bind = self.engine)

        return

    @property
    def session(self):
        return self.session_factory()

