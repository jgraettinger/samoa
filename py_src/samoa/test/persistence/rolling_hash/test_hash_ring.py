
import os
import tempfile
import unittest

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.mapped_hash_ring import MappedHashRing
from samoa.persistence.rolling_hash.element import Element
from samoa.persistence.rolling_hash.packet import Packet
from samoa.core.murmur_hash import MurmurHash

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
        self.assertEquals(ring.reclaim_head(), 116)
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
        self.assertEquals(ring.reclaim_head(), 8220)
        self.assertEquals(ring.begin_offset(), 8356)
        self.assertTrue(ring.is_wrapped())

        # reclaim both packets from allocation of 8190; ring unwraps
        ring.head().set_dead()
        ring.next_packet(ring.head()).set_dead()
        self.assertEquals(ring.reclaim_head(), 8216)
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
        self.assertEquals(ring.reclaim_head(), 116)
        self.assertEquals(ring.begin_offset(), 136)
        self.assertEquals(ring.end_offset(), 4116)
        self.assertFalse(ring.is_wrapped())

        # reclaim packet from allocation of 3967; ring now empty
        ring.head().set_dead()
        self.assertEquals(ring.reclaim_head(), 3980)
        self.assertEquals(ring.begin_offset(), 4116)
        self.assertEquals(ring.end_offset(), 4116)
        self.assertFalse(ring.is_wrapped())

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

    def test_rotation_churn(self):

        fixture = ClusterStateFixture()

        # index size is deliberately small,
        #  to excercise hash chaining
        ring = HeapHashRing.open(1 << 14, 3)

        d = {}

        while True:
            key = fixture.generate_bytes()
            value = fixture.generate_bytes()

            pkt = self._alloc(ring, key, value)
            if not pkt:
                break

            d[key] = value

        for i in xrange(2 * len(d)):

            # note this mimics persister::leaf_compaction();
            #  it rotates the key/value at ring head, to ring tail
            element = Element(ring, ring.head())

            key = element.key()
            value = element.value()
            locator = ring.locate_key(key)

            hash_chain_next = ring.head().hash_chain_next()

            # kill & reclaim head element
            element.set_dead()
            ring.reclaim_head()

            new_head = ring.allocate_packets(len(key) + len(value))
            self.assertTrue(new_head)

            new_head.set_hash_chain_next(hash_chain_next)

            # write updated value
            Element(ring, new_head, key, value)
            ring.update_hash_chain(locator, ring.packet_offset(new_head))

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
        pkt.set_combined_checksum(
            pkt.compute_combined_checksum(MurmurHash()))

        with self.assertRaisesRegexp(RuntimeError, "region_size"):
            ring.locate_key('test key')

    def test_drop_from_chain_head(self):

        fixture = ClusterStateFixture()
        ring = HeapHashRing.open(1 << 14, 1)

        # pkt1's offset is in the table index
        pkt1 = self._alloc(ring, fixture.generate_bytes())
        # pkt2's offset is chained from pkt1
        pkt2 = self._alloc(ring, fixture.generate_bytes())

        ring.drop_from_hash_chain(ring.locate_key(pkt1.key()))

        # pkt2 is still reachable
        locator = ring.locate_key(pkt2.key())
        self.assertEquals(ring.packet_offset(pkt2),
            ring.packet_offset(locator.element_head))

    def test_drop_from_chain_link(self):

        fixture = ClusterStateFixture()
        ring = HeapHashRing.open(1 << 14, 1)

        # pkt1's offset is in the table index
        pkt1 = self._alloc(ring, fixture.generate_bytes())
        # pkt2's offset is chained from pkt1
        pkt2 = self._alloc(ring, fixture.generate_bytes())
        # pkt3's offset is chained from pkt2
        pkt3 = self._alloc(ring, fixture.generate_bytes())

        ring.drop_from_hash_chain(ring.locate_key(pkt2.key()))

        # pkt3 is still reachable
        locator = ring.locate_key(pkt3.key())
        self.assertEquals(ring.packet_offset(pkt3),
            ring.packet_offset(locator.element_head))

    def test_mapped_hash_ring(self):

        desc, path = tempfile.mkstemp()
        os.close(desc)
        os.remove(path)

        ring = MappedHashRing.open(path, 1 << 14, 10)

        self._alloc(ring, 'foo', 'bar')
        self._alloc(ring, 'baz', 'bing')

        del ring
        ring = MappedHashRing.open(path, 1 << 14, 10)

        self.assertEquals('bar',
            ring.locate_key('foo').element_head.value())
        self.assertEquals('bing',
            ring.locate_key('baz').element_head.value())
        del ring

        os.remove(path)

    def _alloc(self, ring, key, value = ''):

        pkt = ring.allocate_packets(len(key) + len(value))
        if not pkt:
            return None

        pkt.set_key(key)
        pkt.set_value(value)
        pkt.set_combined_checksum(
            pkt.compute_combined_checksum(MurmurHash()))

        locator = ring.locate_key(key)
        ring.update_hash_chain(locator, ring.packet_offset(pkt))
        return pkt

