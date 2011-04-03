
import unittest
import random
import uuid
from samoa import persistence

class TestRollingHash(unittest.TestCase):

    def test_mapped(self):

        path = '/tmp/%s' % uuid.uuid4()

        h = persistence.MappedRollingHash.open(path, 1 << 16, 100)

        self._set(h, 'foo', 'bar')
        self._set(h, 'bar', 'baz')
        self._set(h, 'baz', 'bing')

        del h

        h = persistence.MappedRollingHash.open(path, 1 << 16, 100)

        self.assertEquals(
            {'foo': 'bar', 'bar': 'baz', 'baz': 'bing'}, self._dict(h))

        return

    def test_record_bounds(self):
        # Checks assumptions about how records are layed out & padded

        h = persistence.HeapRollingHash(1 << 16, 100)

        # 36 bytes table overhead, 400 byte index
        self.assertEquals(36 + 100 * 4, h.used_region_size())

        post = h.used_region_size()

        # 9 bytes overhead, 6 of content, 1 padding
        self._set(h, 'aaaa', 'aa')
        prev, post = post, h.used_region_size()
        self.assertEquals(post - prev, 16)

        # 9 bytes overhead, 7 of content, 0 padding
        self._set(h, 'bbbbb', 'bb')
        prev, post = post, h.used_region_size()
        self.assertEquals(post - prev, 16)

        # 9 bytes overhead, 8 of content, 3 padding
        self._set(h, 'cccccc', 'cc')
        prev, post = post, h.used_region_size()
        self.assertEquals(post - prev, 20)

        # 9 bytes overhead, 175 of content, 0 padding
        self._set(h, 'd' * 23, 'd' * 152)
        prev, post = post, h.used_region_size()
        self.assertEquals(post - prev, 184)

        # 9 bytes overhead, 297 of content, 2 padding
        self._set(h, 'e' * 46, 'e' * 251)
        prev, post = post, h.used_region_size()
        self.assertEquals(post - prev, 308)
        return

    def test_churn(self):

        data = {}

        for i in xrange(1000):
            data[str(uuid.uuid4())] = '=' * int(
                random.expovariate(1.0 / 135))

        data_size = sum(len(i) + len(j) for i,j in data.iteritems())

        h = persistence.HeapRollingHash(
            int(data_size * 1.3), 2000)

        # Set all keys / values
        for key, value in data.items():
            self._set(h, key, value)

        # Randomly churn, dropping & setting keys
        for i in xrange(5 * len(data)):
            drop_key, set_key, get_key = random.sample(data.keys(), 3)

            self._migrate(h)
            h.mark_for_deletion(drop_key)

            self._migrate(h)
            self._set(h, set_key, data[set_key])

            rec = h.get(get_key)
            if rec:
                self.assertEquals(data[get_key], rec.value)

        # Set all keys / values
        for key, value in data.items():
            self._migrate(h)
            self._set(h, key, value)

        # contents of rolling hash should
        #   be identical to data
        self.assertEquals(data, self._dict(h))

    def _set(self, h, key, val):
        rec = h.prepare_record(key, len(val))
        rec.set_value(val)
        h.commit_record(len(val))

    def _dict(self, h):
        res = {}
        rec = h.head()

        while rec:
            res[rec.key] = rec.value
            rec = h.step(rec)

        return res

    def _migrate(self, h):

        while h.head().is_dead():
            h.reclaim_head()

        rec = h.head()
        self._set(h, rec.key, rec.value)

        while h.head().is_dead():
            h.reclaim_head()

