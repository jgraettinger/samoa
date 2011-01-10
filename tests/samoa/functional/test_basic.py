
import logging
logging.basicConfig(level = logging.INFO)
log = logging.getLogger('foobar')
log.info('test')

logging.error('foobar')

import time
import threading
import unittest
import getty

import samoa.module
import samoa.core
import samoa.client
import samoa.server
import samoa.command.echo
import samoa.command.shutdown

class TestBasic(unittest.TestCase):

    def setUp(self):

        module = samoa.module.Module(':memory:')
        injector = module.configure(getty.Injector())

        self.proactor = injector.get_instance(samoa.core.Proactor)
        context = injector.get_instance(samoa.server.Context)
        protocol = injector.get_instance(samoa.server.SimpleProtocol)

        protocol.add_command_handler('echo', samoa.command.echo.Echo())
        protocol.add_command_handler('shutdown', samoa.command.shutdown.Shutdown())

        self.listener = samoa.server.Listener(
            '0.0.0.0', '0', 1, context, protocol)

        return

    def tearDown(self):
        return

    def test_echo(self):

        def test():

            server = yield samoa.client.Server.connect_to(
                self.proactor, 'localhost', str(self.listener.port))

            cmd = samoa.command.echo.Echo()
            cmd.data = 'ping'
            response = yield cmd.request(server)

            self.assertEquals(response, 'ping')

            self.listener.cancel()

        self.proactor.run_later(test)
        self.proactor.run()

    def test_shutdown(self):
        
        def test():

            server = yield samoa.client.Server.connect_to(
                self.proactor, 'localhost', str(self.listener.port))

            cmd = samoa.command.shutdown.Shutdown()
            response = yield cmd.request(server)

            # Second attempt won't return
            response = yield cmd.request(server)
            self.assertFalse(True)

        self.proactor.run_later(test)
        self.proactor.run()


