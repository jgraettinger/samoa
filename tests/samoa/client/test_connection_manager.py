
import unittest
import getty

import samoa.module
import samoa.core
import samoa.client
import samoa.server
import samoa.command.echo

class TestConnectionManager(unittest.TestCase):

    def setUp(self):

        module = samoa.module.Module(':memory:')
        self.injector = module.configure(getty.Injector())

        self.proactor = self.injector.get_instance(samoa.core.Proactor)
        self.context = self.injector.get_instance(samoa.server.Context)
        self.protocol = self.injector.get_instance(samoa.server.SimpleProtocol)

        self.protocol.add_command_handler('echo',
            samoa.command.echo.Echo())

        self.listener = samoa.server.Listener(
            '0.0.0.0', '0', 1, self.context, self.protocol)

    def test_basic(self):

        def test():

            conn_mgr = self.injector.get_instance(
                samoa.client.ConnectionManager)

            server = yield conn_mgr.get_connection(
                'localhost', str(self.listener.port))

            cmd = samoa.command.echo.Echo()
            cmd.data = 'ping'
            response = yield cmd.request(server)

            self.assertEquals(response, 'ping')

            server2 = yield conn_mgr.get_connection(
                'localhost', str(self.listener.port))

            self.assertEquals(server, server2)

            self.listener.cancel()

        self.proactor.spawn(test)
        self.proactor.run()

