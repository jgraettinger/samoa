
import getty
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.declarative

import config

# base class for all mapped table entity types
MetaTableBase = sqlalchemy.ext.declarative.declarative_base() 

class MetaDBPath(object):
    pass

class MetaDB(object):

    @getty.requires(
        db_path = MetaDBPath,
        injector = getty.Injector)
    def __init__(self, db_path, injector):

        print "DBPATH ", db_path

        self.db_path = db_path
        self.engine = sqlalchemy.create_engine(
            'sqlite:///%s' % db_path, echo = True)

        self.metadata = MetaTableBase.metadata
        self.metadata.create_all(self.engine)
        self.session_factory = sqlalchemy.orm.sessionmaker(
            bind = self.engine)

        self.session = self.session_factory()

        config.make_configuration_bindings(
            injector, self.session)

        return

