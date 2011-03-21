
import uuid
import getty

import samoa.core
import samoa.server.context
import samoa.model.meta
import samoa.model.config

class Module(object):

    def __init__(self, db_uri, proactor = None):
        self.db_uri = db_uri
        self.proactor = proactor

    def configure(self, binder):

        binder.bind_instance(getty.Config, self.db_uri,
            with_annotation = 'db_uri')

        binder.bind(samoa.model.meta.Meta,
            scope = getty.Singleton)

        if self.proactor:
            binder.bind_instance(samoa.core.Proactor, self.proactor)
        else:
            binder.bind(samoa.core.Proactor,
                scope = getty.Singleton)

        binder.bind(samoa.server.context.Context,
            scope = getty.Singleton)

        # bind persisted configuration values
        meta = binder.get_instance(samoa.model.meta.Meta)
        for conf_model in meta.new_session().query(samoa.model.config.Config):
            binder.bind_instance(getty.Config, conf_model.value,
                with_annotation = conf_model.name)

        return binder

