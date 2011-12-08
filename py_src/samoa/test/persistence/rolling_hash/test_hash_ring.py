
import unittest

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.element import Element

from samoa.test.cluster_state_fixture import ClusterStateFixture

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

    def test_bulkhead_boundaries_with_fixtures(self):

        # allocate up to bulkhead edge
        ring = HeapHashRing.open(1 << 20, 1)
        ring.allocate_packets((1 << 19) - (1 << 13))

        # allocation is split into two packets, on either side of bulkhead
        head = ring.allocate_packets(1 << 13)

        # 516948 + 7340 == 524288 == 1 << hash_ring::bulkhead_shift
        self.assertEquals(ring.packet_offset(head), 516948)
        self.assertEquals(head.packet_length(), 7340)
        self.assertFalse(head.completes_sequence())

        tail = ring.next_packet(head)

        self.assertEquals(ring.packet_offset(tail), 524288)
        self.assertEquals(tail.packet_length(), 880)
        self.assertTrue(tail.continues_sequence())
        self.assertTrue(tail.completes_sequence())

        # head + tail capacity meets allocation request (plus 2 of padding)
        self.assertEquals(head.capacity() + tail.capacity(), (1 << 13) + 2)

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

    def test_locate_chain_integrity_violation_detection(self):

        # table size of 1 => all keys collide
        ring = HeapHashRing.open(1 << 13, 1)

        # previous record, with an invalid chain-next
        pkt = self._alloc(ring, 'a key', 'test value')
        pkt.set_hash_chain_next(1 << 14)
        pkt.set_crc_32(pkt.compute_crc_32())

        with self.assertRaisesRegexp(RuntimeError, "region_size"):
            ring.locate_key('test key')

    def test_rotate_chain_integrity_violation_detection(self):

        ring = HeapHashRing.open(1 << 13, 1)

        pkt = ring.allocate_packets(4096)
        pkt.set_key('a key')
        pkt.set_value('test value')
        pkt.set_crc_32(pkt.compute_crc_32())

        # we're not inserting the packet into the table index,
        #   intentionally breaking the hash_ring contract for live records 

        with self.assertRaisesRegexp(RuntimeError, "element_head"):
            ring.rotate_head()

    def test_drop_from_chain_head(self):

        fixture = ClusterStateFixture()
        ring = HeapHashRing.open(1 << 14, 1)

        # pkt1's offset is in the table index
        pkt1 = self._alloc(ring, fixture.generate_bytes())
        # pkt2's offset is chained from pkt1
        pkt2 = self._alloc(ring, fixture.generate_bytes())

        pkt1.set_dead()
        ring.drop_from_hash_chain(ring.locate_key(pkt1.key()))

        # pkt2 is still reachable
        locator = ring.locate_key(pkt2.key())
        self.assertEquals(locator.element_head, pkt2)

    def test_drop_from_chain_link(self):

        fixture = ClusterStateFixture()
        ring = HeapHashRing.open(1 << 14, 1)

        # pkt1's offset is in the table index
        pkt1 = self._alloc(ring, fixture.generate_bytes())
        # pkt2's offset is chained from pkt1
        pkt2 = self._alloc(ring, fixture.generate_bytes())
        # pkt3's offset is chained from pkt2
        pkt2 = self._alloc(ring, fixture.generate_bytes())

        pkt2.set_dead()
        ring.drop_from_hash_chain(ring.locate_key(pkt2.key()))

        # pkt3 is still reachable
        locator = ring.locate_key(pkt3.key())
        self.assertEquals(locator.element_head, pkt3)

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

