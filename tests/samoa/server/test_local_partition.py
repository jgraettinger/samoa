
import unittest
from samoa.core import protobuf as pb
from samoa.core.uuid import UUID
from samoa.server.local_partition import LocalPartition

class TestLocalPartition(unittest.TestCase):

    def test_basic(self):

        msg = pb.ClusterState_Table_Partition()
        msg.set_uuid(UUID.from_random().to_hex())
        msg.set_server_uuid(UUID.from_random().to_hex())

        LocalPartition(msg, None)

