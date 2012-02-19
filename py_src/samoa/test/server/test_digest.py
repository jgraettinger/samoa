
import random
import unittest

from samoa.server.digest import Digest
from samoa.core.uuid import UUID

class TestDigest(unittest.TestCase):

    def test_basic(self):

        Digest.set_default_byte_length(1024)
        Digest.set_path_base("/tmp/")

        partition_uuid = UUID.from_random()

        d = Digest(partition_uuid)

        obj1 = self._make_obj()
        obj2 = self._make_obj()

        self.assertFalse(d.test(obj1))
        self.assertFalse(d.test(obj2))

        d.add(obj1)

        self.assertTrue(d.test(obj1))
        self.assertFalse(d.test(obj2))

        # close & re-open
        del d
        d = Digest(partition_uuid)

        # membership holds
        self.assertTrue(d.test(obj1))
        self.assertFalse(d.test(obj2))

    def test_expected_false_positive_rate(self):
        # Todo: this is sloppy

        Digest.set_default_byte_length(1024)
        Digest.set_path_base("/tmp/")

        d = Digest(UUID.from_random())

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

