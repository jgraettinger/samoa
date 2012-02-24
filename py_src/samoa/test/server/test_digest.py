
import os
import random
import unittest

from samoa.server.digest import Digest
from samoa.server.local_digest import LocalDigest
from samoa.server.remote_digest import RemoteDigest
from samoa.core.uuid import UUID
from samoa.core.memory_map import MemoryMap

class TestDigest(unittest.TestCase):

    def setUp(self):
        Digest.set_default_byte_length(1024)
        Digest.set_directory("/tmp/")

    def test_local_digest(self):
        d = LocalDigest(UUID.from_random())

        obj1 = self._make_obj()
        obj2 = self._make_obj()
        obj3 = self._make_obj()

        self.assertFalse(d.test(obj1))
        self.assertFalse(d.test(obj2))
        self.assertFalse(d.test(obj3))
        self.assertEquals(d.get_properties().element_count, 0)

        d.add(obj1)
        d.add(obj2)

        self.assertTrue(d.test(obj1))
        self.assertTrue(d.test(obj2))
        self.assertFalse(d.test(obj3))
        self.assertEquals(d.get_properties().element_count, 2)

        # destroying cleans up the backing filter file
        tmp_path = d.get_memory_map().get_path()
        self.assertTrue(os.path.exists(tmp_path))
        del d
        self.assertFalse(os.path.exists(tmp_path))

    def test_remote_digest(self):
        partition_uuid = UUID.from_random()
        d = RemoteDigest(partition_uuid)

        obj1 = self._make_obj()
        obj2 = self._make_obj()
        obj3 = self._make_obj()

        self.assertFalse(d.test(obj1))
        self.assertFalse(d.test(obj2))
        self.assertFalse(d.test(obj3))
        self.assertEquals(d.get_properties().element_count, 0)

        d.add(obj1)
        d.add(obj2)

        self.assertTrue(d.test(obj1))
        self.assertTrue(d.test(obj2))
        self.assertFalse(d.test(obj3))
        self.assertEquals(d.get_properties().element_count, 2)

        # close & re-open
        del d
        d = RemoteDigest(partition_uuid)

        # membership persists
        self.assertTrue(d.test(obj1))
        self.assertTrue(d.test(obj2))
        self.assertFalse(d.test(obj3))
        self.assertEquals(d.get_properties().element_count, 2)

    def test_simple_load_test(self):

        d = LocalDigest(UUID.from_random())

        # 1024 keys => 8 bits per key
        for i in xrange(1024):
            d.add(self._make_obj())

        test_total = 1000
        test_hit = 0.0

        for i in xrange(test_total):
            if d.test(self._make_obj()):
                test_hit += 1

        # 8 bits/key & k=2 => 0.0489 false positive rate
        self.assertTrue((test_hit / test_total) < 0.07)

    def _make_obj(self):
        return (random.randint(0, 1<<63), random.randint(0, 1<<63))

