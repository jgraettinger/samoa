#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_RECORD_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_RECORD_HPP

#include <cstddef>
#include <stdexcept>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class hash_ring_packet
{
public:

    /// Byte-length of packet header
    const uint32_t header_length = sizeof(rolling_hash_packet);

    /// Byte-alignment of the (packed) packet header structure
    const uint32_t header_alignment = sizeof(_meta) % sizeof(uint32_t);

    /// Correcting byte-alignment of capacity required
    ///  to ensure that packet is uint32_t aligned
    const uint32_t capacity_alignment_adjustment = header_alignment ? \
        (sizeof(uint32_t) - header_alignment) : 0;

    /// Maximum representable capacity of a packet 
    const uint32_t max_capacity = (1<<11) + capacity_alignment_adjustment - 1;

    /// Minimum byte-length of an (uint32_t-aligned) packet
    const uint32_t min_packet_byte_length = header_length() + \
        capacity_alignment_adjustment;


    // 
    hash_ring_packet(uint32_t capacity);

    /*!
     * \brief Examines packet for issues in storage or retrieval.
     *  - Key and value lengths are bounds-checked against capacity
     *  - Current and stored CRC checksums are compared
     * @return: false if any checks fail
     */
    bool check_integrity() const;

    /*!
     * \brief Computes a 32-bit CRC of this packet
     *
     * Checksum includes:
     *  - hash_chain_next()
     *  - is_dead()
     *  - continues_sequence()
     *  - completes_sequence()
     *  - byte-range of key_begin() : key_end()
     *  - byte-range of value_begin() : value_end()
     */
    uint32_t compute_crc_32();

    /*!
     * \brief Update the 32-bit CRC of the packet
     * @param checksum Must be returned by compute_crc_32()
     */
    void set_crc_32(uint32_t checksum)
    { _meta.crc_32 = checksum; }

    /// Location of next element in linear hash table
    uint32_t get_hash_chain_next() const
    { return _meta.hash_chain_next; }

    /// Update location of next record in linear hash table
    void set_hash_chain_next(uint32_t next)
    { _meta.hash_chain_next = next; }

    /// Whether this packet contains no data, and may be reclaimed
    bool is_dead() const
    { return _meta.is_dead; }

    /// Marks the packet as reclaimable
    void set_dead();

    /*!
     * This packet contains additional key/value content,
     *  continuing the packet which immediately preceeded it.
     */
    bool continues_sequence() const
    { return _meta.continues_sequence; }

    void set_continues_sequence()
    { _meta.continues_sequence = true; }

    /*!
     * This packet completes the sequence of packets which
     *  store a single logical key/value
     */
    bool completes_sequence() const
    { return _meta.completes_sequence; }

    void set_completes_sequence()
    { _meta.completes_sequence = true; }

    /// Total key / value storage capacity of the packet
    uint32_t get_capacity() const
    { return (_meta.capacity << 2) + capacity_alignment_adjustment; }

    /// Remaining storage capacity of the packet
    uint32_t get_available_capacity() const
    { return capacity() - key_length() - value_length(); }

    uint32_t get_key_length() const
    { return _meta.key_length; }

    const char * key_begin() const
    { return reinterpret_cast<char*>(this) + header_length(); }

    const char * key_end() const
    { return key_begin() + key_length(); }

    /*!
     * \brief Allocates capacity for storing the element key,
     *  and returns a mutable underlying buffer.
     *
     * Preconditions:
     *   * Neither set_key() nor set_value have been called
     *     (ie, set_key() may be called only once in packet lifetime)
     *   * key_length is <= available_capacity()
     *
     * @return: buffer of length key_length into which
     *   key content is to be written
     */
    char * set_key(uint32_t key_length);


    uint32_t get_value_length() const
    { return _meta.value_length; }

    const char * value_begin() const
    { return key_end(); }

    const char * value_end() const
    { return key_end() + value_length(); }

    /*!
     * Allocates capacity for storing the packet value.
     *
     * Preconditions:
     *   * value_length is <= available_capacity() + current value_length()
     *
     * @return: buffer of length value_length into which
     *   value content is to be written
     */
    char * set_value(uint32_t value_length);

    uint32_t get_packet_length() const
    { return header_length() + key_length() + value_length(); }

private:

    struct {

        uint32_t crc_32;

        // offset of next element in hash chain, or 0
        uint32_t hash_chain_next;

        // whether this packet may be reclaimed
        bool is_dead : 1;

        // a sequence of rolling_hash_packet provides storage for
        //   longer keys / values which would otherwise overflow
        bool continues_sequence : 1;
        bool completes_sequence : 1;

        // capacity is stored left-shifted 2 bits
        unsigned capacity : 11;

        unsigned key_length : 13;
        unsigned value_length : 13;

        // total size of structure is 13 bytes

    // tell gcc to not word-align (pad) struct bounds
    } __attribute__((__packed__)) _meta;
};

}
}
}

#endif
