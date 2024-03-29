
import unittest
import random
import uuid
from samoa.persistence.mapped_rolling_hash import MappedRollingHash
from samoa.persistence.heap_rolling_hash import HeapRollingHash

class TestRollingHash(unittest.TestCase):

    def test_mapped(self):

        path = '/tmp/%s' % uuid.uuid4()

        h = MappedRollingHash.open(path, 1 << 16, 100)

        self._set(h, 'foo', 'bar')
        self._set(h, 'bar', 'baz')
        self._set(h, 'baz', 'bing')

        del h

        h = MappedRollingHash.open(path, 1 << 16, 100)

        self.assertEquals(
            {'foo': 'bar', 'bar': 'baz', 'baz': 'bing'}, self._dict(h))

        return

    def test_hash_chaining(self):
        # Excercises worst-case hash chaining
        h = HeapRollingHash(1 << 16, 2)

        self._set(h, 'primer', '0')
        d = {'primer': '0'}

        for i in xrange(1000):

            if random.randint(0, 1):
                # insert a new key
                key = str(uuid.uuid4())

                self._set(h, key, '0')
                d[key] = '0'

            else:
                # check value of old key, & increment it
                key = random.choice(d.keys())
                self.assertEquals(h.get(key).value, d[key])

                d[key] = str(int(d[key]) + 1)
                self._set(h, key, d[key])

        # purge all keys
        for key in d.keys():
            h.mark_for_deletion(key)

        # no live records should remain
        self.assertEquals(h.live_record_count(), 0)

    def test_record_bounds(self):
        # Checks assumptions about how records are layed out & padded

        h = HeapRollingHash(1 << 16, 100)

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

    def test_wrapping(self):
        # Checks assumptions about how records are shifted around the ring,
        #   how wrapping is handled, and how the full condition is handled

        h = HeapRollingHash(1 << 13, 100)

        # 8192 total - 436 bytes overhead = 7756 record region size

        # 56 byte records (36 byte key, 10 byte value, 9 record overhead, 1 padding)
        #   => 138 records, w/ 28 bytes remaining

        # insert & remove some records
        keys = set(str(uuid.uuid4()) for i in xrange(20))
        for key in keys:
            self._set(h, key, key[:10])

        # delete them all
        for key in keys:
            h.mark_for_deletion(key)

        # reclaim space
        while h.head():
            self.assertTrue(h.head().is_dead())
            h.reclaim_head()

        self.assertEquals(h.used_region_size(), 436)

        # insert exactly as many records as the hash can store
        keys = set(str(uuid.uuid4()) for i in xrange(138))
        for i, key in enumerate(keys):

            self._set(h, key, key[:10])
            self.assertEquals(h.used_region_size(), 436 + (i + 1) * 56)

        # no additional records will fit
        self.assertEquals(h.total_region_size() - h.used_region_size(), 28)

        # rotate head excessively
        for i in xrange(138 * 20):
            h.rotate_head()

        # check all expected keys / values are present
        head = h.head()
        while head:

            self.assertEquals(head.value, head.key[:10])
            keys.remove(head.key)
            head = h.step(head)

        self.assertEquals(keys, set())

    def test_churn(self):
        # Synthesizes "normal" usage, with keys being both added & dropped

        data = {}

        for i in xrange(1000):
            data[str(uuid.uuid4())] = '=' * int(
                random.expovariate(1.0 / 135))

        data_size = sum(len(i) + len(j) for i,j in data.iteritems())

        h = HeapRollingHash(
            int(data_size * 1.3), 2000)

        # Set all keys / values
        for key, value in data.items():
            self._set(h, key, value)

        # Randomly churn, dropping & setting keys
        for i in xrange(5 * len(data)):
            drop_key, set_key, get_key = random.sample(data.keys(), 3)

            self._upkeep(h)
            h.mark_for_deletion(drop_key)

            self._upkeep(h)
            self._set(h, set_key, data[set_key])

            rec = h.get(get_key)
            if rec:
                self.assertEquals(data[get_key], rec.value)

        # Set all keys / values
        for key, value in data.items():
            self._upkeep(h)
            self._set(h, key, value)

        # contents of rolling hash should
        #   be identical to data
        self.assertEquals(data, self._dict(h))

    def _set(self, h, key, val):
        rec = h.prepare_record(key, len(val))
        rec.set_value(val)
        h.commit_record()

    def _dict(self, h):
        res = {}
        rec = h.head()

        while rec:
            res[rec.key] = rec.value
            rec = h.step(rec)

        return res

    def _upkeep(self, h):

        if not h.head().is_dead():
            h.rotate_head()

        while h.head().is_dead():
            h.reclaim_head()

