
import getty

import samoa.model
import samoa.runtime

class Module(object):

    def __init__(self, db_uri):
        self.db_uri = db_uri

    def configure(self, binder):

        binder.bind_instance(getty.Config, self.db_uri,
            with_annotation = 'db_uri')

        binder.bind(samoa.model.Meta,
            to = samoa.model.Meta,
            scope = getty.Singleton)

        binder.bind(samoa.runtime.PeerPool,
            to = samoa.runtime.PeerPool,
            scope = getty.Singleton)

        binder.bind(samoa.runtime.TableSet,
            to = samoa.runtime.TableSet,
            scope = getty.Singleton)

        # bind persisted configuration values
        meta = binder.get_instance(samoa.model.Meta)
        for conf_model in meta.session.query(samoa.model.Config):
            binder.bind_instance(getty.Config, conf_model.value,
                with_annotation = conf_model.name)

        return binder

