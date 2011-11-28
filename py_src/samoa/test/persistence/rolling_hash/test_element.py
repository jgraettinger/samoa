
import unittest

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.element import Element

class TestElement(unittest.TestCase):

    # Note: testing of protobuf message value de/serialization is
    #  "up" a level, excercised in rolling hash unit tests

    def test_single_packet_element(self):

        ring = HeapHashRing.open(1 << 14, 1)

        element = Element(ring, ring.allocate_packets(4096),
            'test key', 'test value')

        # reload element
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 8)
        self.assertEquals(element.key(), 'test key')
        self.assertEquals(element.value_length(), 10)
        self.assertEquals(element.value(), 'test value')
        self.assertEquals(element.capacity(), 4099)

        element.set_value('test value 2')

        # reload element
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.value_length(), 12)
        self.assertEquals(element.value(), 'test value 2')

        element.set_dead()

        self.assertEquals(element.key_length(), 8)
        self.assertEquals(element.key(), 'test key')
        self.assertEquals(element.value_length(), 0)

        packet = element.head()
        self.assertTrue(packet.completes_sequence())
        self.assertTrue(packet.is_dead())
        self.assertFalse(element.step(packet))

    def test_multi_packet_element(self):

        ring = HeapHashRing.open(1 << 17, 1)
        pattern = ''.join(chr(i % 255) for i in xrange(1<<15))

        element = Element(ring, ring.allocate_packets(len(pattern) * 2),
            pattern, '')

        # reload element
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.capacity(), len(pattern) * 2 + 3)
        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value_length(), 0)

        element.set_value(pattern)

        # reload element
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.value_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value(), pattern)

        element.set_dead()

        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value_length(), 0)

        packet = element.head()
        while not packet.completes_sequence():
            self.assertTrue(packet.is_dead())
            packet = element.step(packet)

    def test_single_packet_integrity_violation_detection(self):

        ring = HeapHashRing.open(1 << 14, 1)

        element = Element(ring, ring.allocate_packets(4096),
            'test key', 'test value')

        element.head().set_value('test-value')

        with self.assertRaisesRegexp(RuntimeError, 'check_integrity'):
            Element(ring, ring.head())

    def test_multi_packet_integrity_violation_detection(self):

        ring = HeapHashRing.open(1 << 17, 1)
        pattern = ''.join(chr(i % 255) for i in xrange(1<<15))

        element = Element(ring, ring.allocate_packets(len(pattern) * 2),
            pattern, '')

        # iterate to last sequence packet, and flip a byte in it's value
        tmp_pkt = element.head()
        while not tmp_pkt.completes_sequence():
            tmp_pkt = element.step(tmp_pkt)

        tmp_pkt.set_value('a' + tmp_pkt.value()[1:])

        # reload element; doesn't throw yet
        del element
        element = Element(ring, ring.head())

        # key_length also doesn't throw (hasn't reached sequence end)
        element.key_length()

        # value_length throws upon reaching end & discovering corruption
        with self.assertRaisesRegexp(RuntimeError, 'check_integrity'):
            element.value_length()

