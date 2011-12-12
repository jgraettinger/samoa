
import unittest

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.element import Element
from samoa.core.protobuf import PersistedRecord

class TestElement(unittest.TestCase):

    def test_single_packet_element(self):

        ring = HeapHashRing.open(1 << 14, 1)

        # write initial value
        element = Element(ring, ring.allocate_packets(4096),
            'test key', 'test value')

        # reload element, & validate
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 8)
        self.assertEquals(element.key(), 'test key')
        self.assertEquals(element.value_length(), 10)
        self.assertEquals(element.value(), 'test value')
        self.assertEquals(element.capacity(), 4099)

        # write updated value
        element.set_value('test value 2')

        # reload element, & validate
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 8)
        self.assertEquals(element.key(), 'test key')
        self.assertEquals(element.value_length(), 12)
        self.assertEquals(element.value(), 'test value 2')
        self.assertEquals(element.capacity(), 4099)

        # write a protobuf record
        record = PersistedRecord()
        record.add_blob_value('test value 3')
        record.set_expire_timestamp(1234)
        element.write_persisted_record(record)

        expected_bytes = record.ByteSize()

        # reload element, & validate
        del element, record
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 8)
        self.assertEquals(element.key(), 'test key')
        self.assertEquals(element.value_length(), expected_bytes)
        self.assertEquals(element.capacity(), 4099)

        record = PersistedRecord()
        element.parse_persisted_record(record)
        self.assertEquals(record.blob_value[0], 'test value 3')
        self.assertEquals(record.expire_timestamp, 1234)

        # mark element as dead
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

        # write key and initial empty value
        element = Element(ring, ring.allocate_packets(len(pattern) * 2),
            pattern, '')

        # reload element, & validate
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value_length(), 0)
        self.assertEquals(element.value(), '')
        self.assertEquals(element.capacity(), len(pattern) * 2 + 3)

        # write pattern as value 
        element.set_value(pattern)

        # reload element, & validate
        del element
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value_length(), 1 << 15)
        self.assertEquals(element.value(), pattern)
        self.assertEquals(element.capacity(), len(pattern) * 2 + 3)

        # write a protobuf record, which spans multiple packets
        #  but leaves some packets unwritten
        record = PersistedRecord()
        record.add_blob_value(pattern[:1<<14])
        record.set_expire_timestamp(1234)
        element.write_persisted_record(record)

        expected_bytes = record.ByteSize()

        # reload element, & validate
        del element, record
        element = Element(ring, ring.head())

        self.assertEquals(element.key_length(), 1 << 15)
        self.assertEquals(element.key(), pattern)
        self.assertEquals(element.value_length(), expected_bytes)
        self.assertEquals(element.capacity(), len(pattern) * 2 + 3)

        record = PersistedRecord()
        element.parse_persisted_record(record)
        self.assertEquals(record.blob_value[0], pattern[:1<<14])
        self.assertEquals(record.expire_timestamp, 1234)

        # mark element as dead
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

