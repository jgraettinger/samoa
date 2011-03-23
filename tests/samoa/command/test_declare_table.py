
import unittest
import getty

import samoa.module
import samoa.client.server
import samoa.server.context
import samoa.command.declare_table

class TestDeclareTable(unittest.TestCase):

    def setUp(self):
        module = samoa.module.TestModule()
        self.injector = module.configure(getty.Injector())
        self.context = self.injector.get_instance(samoa.server.context.Context)
        self.proactor = self.context.get_proactor()
        self.port = self.context.get_listener().get_port()

    def test_declare_table(self):

        def test():

            server = yield samoa.client.server.Server.connect_to(
                self.proactor, 'localhost', str(self.port))

            cmd = samoa.command.declare_table.DeclareTable(
                name = 'test_table',
                replication_factor = 2,
                create_if_not_exists = False)

            try:
                # should throw, as create_if_not_exists is False
                response = yield cmd.request(server)
                self.assertFalse(True)
            except Exception, e:
                pass

            # This time table is created
            cmd.create_if_not_exists = True
            response = yield cmd.request(server)

            table = self.context.get_table_by_name('test_table')
            self.assertTrue(table)
            self.assertEquals(table.replication_factor, 2)

            self.proactor.shutdown()
            yield

        self.proactor.spawn(test)
        self.proactor.run()

