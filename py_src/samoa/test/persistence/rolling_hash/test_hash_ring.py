
import unittest
import random
import uuid

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.element import Element

from samoa.test.cluster_state_fixture import ClusterStateFixture

# TODO: tests for
#  - bulkhead boundaries
#  - integrity violation detection
#  - multi-packet elements:
#     - key, value, & capacity lengths
#     - key & value gather
#     - value updates
#  - churn, with elements rotated & discarded

class TestHashRing(unittest.TestCase):

    def test_allocate_and_reclaim_with_fixtures(self):

        ring = HeapHashRing.open(16572, 1)

        # simple allocation
        pkt = ring.allocate_packets(100)
        self.assertEquals(pkt.header_length(), 13)

        # 13 of header, 103 of capacity (3 of padding)
        self.assertEquals(pkt.packet_length(), 116)
        self.assertEquals(pkt.capacity(), 103)
        self.assertFalse(pkt.continues_sequence())
        self.assertTrue(pkt.completes_sequence())
        self.assertFalse(ring.next_packet(pkt))
        self.assertEquals(ring.end_offset(), 136)

        # allocation requiring two packets
        pkt = ring.allocate_packets(8193)

        # 13 of header, 8191 of capacity
        self.assertEquals(pkt.packet_length(), 8204)
        self.assertEquals(pkt.capacity(), 8191)
        self.assertFalse(pkt.continues_sequence())
        self.assertFalse(pkt.completes_sequence())

        pkt = ring.next_packet(pkt)

        # 13 of header, 3 of capacity (2 of padding)
        self.assertEquals(pkt.packet_length(), 16)
        self.assertEquals(pkt.capacity(), 3)
        self.assertTrue(pkt.continues_sequence())
        self.assertTrue(pkt.completes_sequence())
        self.assertFalse(ring.next_packet(pkt))
        self.assertEquals(ring.end_offset(), 8356)

        # allocation which fails (one byte to large)
        self.assertFalse(ring.allocate_packets(8191))

        # allocation which could fit in one packet,
        #  but is split into two so as not to leave a remainder
        #  less than a minimum-length packet in size
        pkt = ring.allocate_packets(8190)

        # 13 of header, 8187 of capacity
        self.assertEquals(pkt.packet_length(), 8200)
        self.assertEquals(pkt.capacity(), 8187)
        self.assertFalse(pkt.continues_sequence())
        self.assertFalse(pkt.completes_sequence())

        pkt = ring.next_packet(pkt)

        # 13 of header, 3 of capacity
        self.assertEquals(pkt.packet_length(), 16)
        self.assertEquals(pkt.capacity(), 3)
        self.assertTrue(pkt.continues_sequence())
        self.assertTrue(pkt.completes_sequence())

        self.assertTrue(ring.is_wrapped())
        self.assertEquals(ring.begin_offset(), ring.end_offset())

        # free the first allocation
        ring.head().set_dead()
        ring.reclaim_head()
        self.assertEquals(ring.begin_offset(), 136)

        # allocation which is sized up into a larger packet,
        #  so as not to leave a remainder less than a
        #  minimum-length packet in size
        pkt = ring.allocate_packets(91)

        # 13 of header, 103 of capacity (12 of padding)
        self.assertEquals(pkt.packet_length(), 116)
        self.assertEquals(pkt.capacity(), 103)
        self.assertFalse(pkt.continues_sequence())
        self.assertTrue(pkt.completes_sequence())
        self.assertEquals(ring.end_offset(), 136)

        # reclaim both packets from allocation of 8193
        ring.head().set_dead()
        ring.next_packet(ring.head()).set_dead()
        ring.reclaim_head()
        self.assertEquals(ring.begin_offset(), 8356)
        self.assertTrue(ring.is_wrapped())

        # reclaim both packets from allocation of 8190; ring unwraps
        ring.head().set_dead()
        ring.next_packet(ring.head()).set_dead()
        ring.reclaim_head()
        self.assertEquals(ring.begin_offset(), ring.ring_region_offset())
        self.assertFalse(ring.is_wrapped())

        pkt = ring.allocate_packets(3967)

        # 13 of header, 3967 of capacity (0 of padding)
        self.assertEquals(pkt.packet_length(), 3980)
        self.assertEquals(pkt.capacity(), 3967)
        self.assertFalse(pkt.continues_sequence())
        self.assertTrue(pkt.completes_sequence())
        self.assertEquals(ring.begin_offset(), 20)
        self.assertEquals(ring.end_offset(), 4116)

        # reclaim packet from allocation of 91
        ring.head().set_dead()
        ring.reclaim_head()
        self.assertEquals(ring.begin_offset(), 136)
        self.assertEquals(ring.end_offset(), 4116)
        self.assertFalse(ring.is_wrapped())

        # reclaim packet from allocation of 3967; ring now empty
        ring.head().set_dead()
        ring.reclaim_head()
        self.assertEquals(ring.begin_offset(), 4116)
        self.assertEquals(ring.end_offset(), 4116)
        self.assertFalse(ring.is_wrapped())
        return

    def test_rotate_with_fixtures(self):

        ring = HeapHashRing.open(1 << 13, 1)

        pkt = self._alloc(ring, 'test key', 'test value', capacity = 4192)

        # 13 of header, 4195 of capacity (3 of padding)
        self.assertEquals(pkt.packet_length(), 4208)
        self.assertEquals(ring.begin_offset(), 20)
        self.assertEquals(ring.end_offset(), 4228)

        ring.rotate_head()
        pkt = ring.head()

        # 13 of header, 19 of capacity (1 of padding)
        self.assertEquals(pkt.packet_length(), 32)
        self.assertEquals(ring.begin_offset(), 4228)
        self.assertEquals(ring.end_offset(), 4260)

        # fill ring remainder
        ring.allocate_packets(8114)
        self.assertEquals(ring.begin_offset(), 4228)
        self.assertEquals(ring.end_offset(), 4228)
        self.assertTrue(ring.is_wrapped())

        # rotate ring, this time under full condition
        #  (packet is 'rotated' in-place, & doesn't move)
        ring.rotate_head()
        self.assertEquals(ring.begin_offset(), 4260)
        self.assertEquals(ring.end_offset(), 4260)
        self.assertTrue(ring.is_wrapped())

        pkt = ring.locate_key('test key').element_head
        self.assertEquals(pkt.packet_length(), 32)
        self.assertEquals(ring.packet_offset(pkt), 4228)
        self.assertEquals(pkt.value(), 'test value')

    def test_locate_key(self):

        fixture = ClusterStateFixture()
        ring = HeapHashRing.open(1 << 14, 2)

        key1 = fixture.generate_bytes()
        val1 = fixture.generate_bytes()
        pkt = self._alloc(ring, key1, val1)

        key2 = fixture.generate_bytes()
        val2 = fixture.generate_bytes()
        self._alloc(ring, key2, val2)

        locator = ring.locate_key(key1)
        self.assertEquals(locator.element_head.key(), key1)
        self.assertEquals(locator.element_head.value(), val1)

        locator = ring.locate_key(key2)
        self.assertEquals(locator.element_head.key(), key2)
        self.assertEquals(locator.element_head.value(), val2)

    def test_location_and_rotation_churn(self):

        fixture = ClusterStateFixture()

        # index size is deliberately small,
        #  to excercise hash chaining
        ring = HeapHashRing.open(1 << 14, 5)

        d = {}

        while True:
            key = fixture.generate_bytes()
            value = fixture.generate_bytes()

            pkt = self._alloc(ring, key, value)
            if not pkt:
                break

            d[key] = value

        for i in xrange(2 * len(d)):
            ring.rotate_head()

        for expected_key, expected_value in d.iteritems():

            locator = ring.locate_key(expected_key)
            self.assertTrue(locator.element_head)

            element = Element(ring, locator.element_head)
            self.assertEquals(element.key(), expected_key)
            self.assertEquals(element.value(), expected_value)

    def test_large_elements(self):

        pattern = ''.join(chr(i % 255) for i in xrange(1<<15))
        ring = HeapHashRing.open(1 << 17, 1)

        head = ring.allocate_packets(len(pattern) * 2)

        # construct in place, with key only
        element = Element(ring, head, pattern)

        self.assertEquals(element.capacity(), len(pattern) * 2)
        self.assertEquals(element.key_length(), 1<<15)
        self.assertEquals(element.value_length(), 0)


    def _alloc(self, ring, key, value = '', capacity = None):
        if capacity is None:
            capacity = len(key) + len(value)

        pkt = ring.allocate_packets(capacity)
        if not pkt:
            return None

        pkt.set_key(key)
        pkt.set_value(value)
        pkt.set_crc_32(pkt.compute_crc_32())

        locator = ring.locate_key(key)
        ring.update_hash_chain(locator, ring.packet_offset(pkt))
        return pkt


"""
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
"""
