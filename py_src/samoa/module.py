
import uuid
import getty
import logging

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

class TestModule(Module):

    def __init__(self, server_uuid = None, port = 0, proactor = None):
        Module.__init__(self, ':memory:', proactor)
        self.server_uuid = server_uuid or \
            samoa.core.UUID.from_name_str('test_server')
        self.port = port

    def configure(self, binder):
        Module.configure(self, binder)

        binder.bind_instance(getty.Config, self.server_uuid,
            with_annotation = 'server_uuid')
        binder.bind_instance(getty.Config, '/tmp',
            with_annotation = 'partition_path')

        # Add a record for this server
        meta = binder.get_instance(samoa.model.meta.Meta)
        session = meta.new_session()

        session.add(samoa.model.server.Server(uuid = self.server_uuid,
            hostname = 'localhost', port = str(self.port)))

        session.commit()

        logging.basicConfig(level = logging.INFO)
        return binder

