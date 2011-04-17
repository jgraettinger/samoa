
import unittest
import getty

import samoa.module
import samoa.client.server
import samoa.server.context
import samoa.command.error

class TestDropTable(unittest.TestCase):

    def setUp(self):
        module = samoa.module.TestModule()
        self.injector = module.configure(getty.Injector())
        self.context = self.injector.get_instance(samoa.server.context.Context)
        self.proactor = self.context.get_proactor()
        self.port = self.context.get_listener().get_port()

    def test_error(self):

        def test():

            server = yield samoa.client.server.Server.connect_to(
                self.proactor.serial_io_service(), 'localhost', str(self.port))

            # error 'request' causes server to return an error,
            #  which should be raised in the client
            cmd = samoa.command.error.Error('hello', 'world')
            try:
                response = yield cmd.request(server)
                self.assertFalse(True)
            except Exception, e:
                self.assertEquals(e.args,
                    ('type: "hello"\nmessage: "world"\n',))
                self.assertTrue(server.is_open())

            # again, but this time the server should close the connection
            cmd.closing = True
            try:
                response = yield cmd.request(server)
                self.assertFalse(True)
            except Exception, e:
                self.assertEquals(e.args,
                    ('type: "hello"\nmessage: "world"\n',))
                self.assertFalse(server.is_open())

            self.proactor.shutdown()
            yield

        self.proactor.spawn(test)
        self.proactor.run()

