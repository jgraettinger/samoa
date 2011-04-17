
import unittest
import getty

import samoa.module
import samoa.client.server
import samoa.server.context
import samoa.command.declare_table
import samoa.command.create_partition

class TestCreatePartition(unittest.TestCase):

    def setUp(self):
        module = samoa.module.TestModule()
        self.injector = module.configure(getty.Injector())
        self.context = self.injector.get_instance(samoa.server.context.Context)
        self.proactor = self.context.get_proactor()
        self.port = self.context.get_listener().get_port()

    def test_create_partition(self):

        def test():

            server = yield samoa.client.server.Server.connect_to(
                self.proactor.serial_io_service(), 'localhost', str(self.port))

            # create a test table
            cmd = samoa.command.declare_table.DeclareTable(
                name = 'test_table',
                replication_factor = 1)
            resp = yield cmd.request(server)

            table_uuid = samoa.core.UUID.from_hex_str(resp.uuid)

            # create a partition
            cmd = samoa.command.create_partition.CreatePartition(
                table_uuid = table_uuid,
                ring_position = 1234567,
                storage_size = (1 << 20),
                index_size = (1 << 12))
            resp = yield cmd.request(server)

            part_uuid = samoa.core.UUID.from_hex_str(resp.uuid)

            # assert partition is live on the server
            partition = self.context.get_table(
                table_uuid).get_partition(part_uuid)
            
            self.assertEquals(partition.ring_position, 1234567)
            self.assertEquals(partition.storage_size, 1 << 20)
            self.assertEquals(partition.index_size, 1 << 12)

            # creating a partition for an unknown table fails
            table_uuid = samoa.core.UUID.from_random()
            cmd.table_uuid = table_uuid

            try:
                response = yield cmd.request(server)
                self.assertFalse(True)
            except Exception, e:
                pass

            self.proactor.shutdown()
            yield

        self.proactor.spawn(test)
        self.proactor.run()

