
import unittest

from samoa.persistence.rolling_hash.heap_hash_ring import HeapHashRing
from samoa.persistence.rolling_hash.packet import Packet
from samoa.core.murmur_hash import MurmurHash

class TestPacket(unittest.TestCase):

    def test_packet_alignment_and_content_handling(self):

        ring = HeapHashRing.open(16572, 1)
        packet = ring.allocate_packets(1023)

        # packets are aligned to word-boundary
        self.assertFalse(packet.packet_length() % 4)
        self.assertEquals(packet.capacity(),
            packet.packet_length() - packet.header_length())

        packet.set_key('test-key')
        self.assertEquals(packet.key_length(), 8)
        self.assertEquals(packet.key(), 'test-key')

        packet.set_value('test-value')
        self.assertEquals(packet.value_length(), 10)
        self.assertEquals(packet.value(), 'test-value')

        self.assertEquals(packet.capacity() - 18,
            packet.available_capacity())

    def test_packet_integrity_checking(self):

        ring = HeapHashRing.open(16572, 1)
        packet_1 = ring.allocate_packets(1024)
        packet_2 = ring.allocate_packets(1024)

        packet_1.set_key('test-key')
        packet_1.set_value('test-value')

        packet_2.set_continues_sequence()
        packet_2.set_completes_sequence()
        packet_2.set_value('test-value-part-2')

        # start with correct packet checksums
        content_cs = MurmurHash()

        packet_1.set_combined_checksum(
            packet_1.compute_combined_checksum(content_cs))
        packet_2.set_combined_checksum(
            packet_2.compute_combined_checksum(content_cs))

        # packet checks pass
        content_cs = MurmurHash()
        self.assertTrue(packet_1.check_integrity(content_cs))
        self.assertTrue(packet_2.check_integrity(content_cs))

        # a check of packet_2 w/o packet_1 fails
        #  (depends on packet_1 content)
        content_cs = MurmurHash()
        self.assertFalse(packet_2.check_integrity(content_cs))

        # update packet_1 metadata
        old_meta_cs = packet_1.compute_meta_checksum()
        packet_1.set_hash_chain_next(1234)

        self.assertNotEquals(old_meta_cs, packet_1.compute_meta_checksum())
        packet_1.update_meta_of_combined_checksum(old_meta_cs)

        # both packets still check out
        content_cs = MurmurHash()
        self.assertTrue(packet_1.check_integrity(content_cs))
        self.assertTrue(packet_2.check_integrity(content_cs))

        # modifying packet 1 content w/o updating checksum
        packet_1.set_value('test-valu3')

        # both packets now fail
        content_cs = MurmurHash()
        self.assertFalse(packet_1.check_integrity(content_cs))
        self.assertFalse(packet_2.check_integrity(content_cs))

    def test_running_content_cs(self):

        ring = HeapHashRing.open(16572, 1)

        packet_1 = ring.allocate_packets(18)
        packet_2a = ring.allocate_packets(18)
        packet_2b = ring.allocate_packets(18)
        packet_3a = ring.allocate_packets(18)
        packet_3b = ring.allocate_packets(18)
        packet_3c = ring.allocate_packets(18)

        packet_1.set_key('test-key')
        packet_1.set_value('test-value')

        packet_2a.set_key('test-key')
        packet_2b.set_value('test-value')

        packet_3a.set_key('test-k')
        packet_3b.set_key('ey')
        packet_3b.set_value('te')
        packet_3c.set_value('st-value')

        # all packets produce equal content checksums
        cs1 = MurmurHash()
        packet_1.compute_content_checksum(cs1)

        cs2 = MurmurHash()
        packet_2a.compute_content_checksum(cs2)
        packet_2b.compute_content_checksum(cs2)

        cs3 = MurmurHash()
        packet_3a.compute_content_checksum(cs3)
        packet_3b.compute_content_checksum(cs3)
        packet_3c.compute_content_checksum(cs3)

        self.assertEquals(cs1.checksum(), cs2.checksum())
        self.assertEquals(cs1.checksum(), cs3.checksum())
