
import getty
import sqlite3

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.interfaces

from base import ModelBase

class _SQLITESetTextFactory(sqlalchemy.interfaces.PoolListener):
    def connect(self, dbapi_con, con_rec):
        # sqlalchemy already does unicode type management and
        #  coercion. No need for sqlite to do it also.
        dbapi_con.text_factory = str

class Meta(object):

    @getty.requires(db_uri = getty.Config('db_uri'))
    def __init__(self, db_uri):

        self._db_uri = db_uri
        self._engine = sqlalchemy.create_engine('sqlite:///%s' % db_uri,
            listeners = [_SQLITESetTextFactory()],
            echo = False)

        # Enable enforcement of SQLite Foreign Keys
        self._engine.execute('PRAGMA foreign_keys = ON;')

        self._metadata = ModelBase.metadata
        self._metadata.create_all(self._engine)

        self._sessionmaker = sqlalchemy.orm.sessionmaker(
            bind = self._engine)

        return

    def new_session(self):
        return self._sessionmaker()

