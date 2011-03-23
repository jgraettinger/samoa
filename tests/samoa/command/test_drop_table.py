
import unittest
import getty

import samoa.module
import samoa.client.server
import samoa.server.context
import samoa.command.declare_table
import samoa.command.drop_table

class TestDropTable(unittest.TestCase):

    def setUp(self):
        module = samoa.module.TestModule()
        self.injector = module.configure(getty.Injector())
        self.context = self.injector.get_instance(samoa.server.context.Context)
        self.proactor = self.context.get_proactor()
        self.port = self.context.get_listener().get_port()

    def test_drop_table(self):

        def test():

            server = yield samoa.client.server.Server.connect_to(
                self.proactor, 'localhost', str(self.port))

            # create a table to drop
            cmd = samoa.command.declare_table.DeclareTable(
                name = 'test_table', replication_factor = 1)
            response = yield cmd.request(server)
            tbl_uuid = samoa.core.UUID.from_hex_str(response.uuid)

            self.assertTrue(self.context.get_table(tbl_uuid))

            # drop that table
            cmd = samoa.command.drop_table.DropTable(uuid = tbl_uuid)
            response = yield cmd.request(server)

            self.assertFalse(self.context.get_table(tbl_uuid))
            self.assertTrue(self.context.get_cluster_state(
                ).get_table_set().was_dropped(tbl_uuid))

            # drop a non-existent table
            try:
                # should throw, as table doesn't exist
                cmd.uuid = samoa.core.UUID.from_random()
                response = yield cmd.request(server)
                self.assertFalse(True)
            except Exception, e:
                print "Caught ", e

            self.proactor.shutdown()
            yield

        self.proactor.spawn(test)
        self.proactor.run()

