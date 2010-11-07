
import unittest
import samoa
import random
import uuid

class TestRollingHash(unittest.TestCase):

    def test_mapped(self):

        path = '/tmp/%s' % uuid.uuid4()

        h = samoa.MappedRollingHash.open(path, 1 << 16, 100)

        h.set('foo', 'bar')
        h.set('bar', 'baz')
        h.set('baz', 'bing')

        del h

        h = samoa.MappedRollingHash.open(path, 1 << 16, 100)

        self.assertEquals(
            {'foo': 'bar', 'bar': 'baz', 'baz': 'bing'}, dict(h))

        return

    def test_record_bounds(self):
        # Checks assumptions about how records are layed out & padded

        h = samoa.HeapRollingHash(1 << 16, 100)

        # 32 bytes table overhead, 400 byte index
        self.assertEquals(32 + 100 * 4, h.used_region_size)

        post = h.used_region_size

        # 9 bytes overhead, 6 of content, 1 padding
        h.set('aaaa', 'aa')
        prev, post = post, h.used_region_size
        self.assertEquals(post - prev, 16)

        # 9 bytes overhead, 7 of content, 0 padding
        h.set('bbbbb', 'bb')
        prev, post = post, h.used_region_size
        self.assertEquals(post - prev, 16)

        # 9 bytes overhead, 8 of content, 3 padding
        h.set('cccccc', 'cc')
        prev, post = post, h.used_region_size
        self.assertEquals(post - prev, 20)

        # 9 bytes overhead, 175 of content, 0 padding
        h.set('d' * 23, 'd' * 152)
        prev, post = post, h.used_region_size
        self.assertEquals(post - prev, 184)

        # 9 bytes overhead, 297 of content, 2 padding
        h.set('e' * 46, 'e' * 251)
        prev, post = post, h.used_region_size
        self.assertEquals(post - prev, 308)
        return

    def test_churn(self):

        data = {}

        for i in xrange(1000):
            data[str(uuid.uuid4())] = '=' * int(
                random.expovariate(1.0 / 135))

        data_size = sum(len(i) + len(j) for i,j in data.iteritems())

        h = samoa.HeapRollingHash(
            int(data_size * 1.3), 2000)

        # Set all keys / values
        for key, value in data.items():
            h.set(key, value)

        # Randomly churn, dropping & setting keys
        for i in xrange(5 * len(data)):
            drop_key, set_key, get_key = random.sample(data.keys(), 3)

            h.migrate_head()
            h.migrate_head()
            h.drop(drop_key)

            h.migrate_head()
            h.migrate_head()
            h.set(set_key, data[set_key])

            val = h.get(get_key)
            if val:
                self.assertEquals(data[get_key], val)

        # Set all keys / values
        for key, value in data.items():
            h.migrate_head()
            h.migrate_head()
            h.set(key, value)

        # contents of rolling hash should
        #   be identical to data
        self.assertEquals(data, dict(h))

