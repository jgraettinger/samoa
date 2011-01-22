
import logging
logging.basicConfig(level = logging.INFO)

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
        self.context = injector.get_instance(samoa.server.Context)
        self.protocol = injector.get_instance(samoa.server.SimpleProtocol)

        self.protocol.add_command_handler('echo',
            samoa.command.echo.Echo())
        self.protocol.add_command_handler('shutdown',
            samoa.command.shutdown.Shutdown())

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

        self.listener = samoa.server.Listener(
            '0.0.0.0', '0', 1, self.context, self.protocol)

        self.proactor.run_later(test, 0)
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

        self.listener = samoa.server.Listener(
            '0.0.0.0', '0', 1, self.context, self.protocol)

        self.proactor.run_later(test, 1)
        self.proactor.run()

    def test_run_later(self):

        val = []

        def test():
            val.append(1)

        self.proactor.run_later(test, 1)
        self.proactor.run()
        self.assertTrue(val)

