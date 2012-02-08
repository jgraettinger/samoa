#ifndef SAMOA_PERSISTENCE_ROLLING_HASH_PACKET_HPP
#define SAMOA_PERSISTENCE_ROLLING_HASH_PACKET_HPP

#include <cstddef>
#include <stdexcept>
#include <boost/crc.hpp>

namespace samoa {
namespace persistence {
namespace rolling_hash {

class packet
{
public:

    // 
    packet(uint32_t capacity);

    /*!
     * \brief Examines packet for issues in storage or retrieval.
     *  - Key and value lengths are bounds-checked against capacity
     *  - Computed and stored meta & content checksums are compared
     * @return: false if any checks fail
     */
    bool check_integrity(boost::crc_32_type & content_crc) const;

    /*!
     * \brief Computes the running checksum of this packet's content
     *
     * Added to content_crc:
     *  - byte-range of key_begin() : key_end()
     *  - byte-range of value_begin() : value_end()
     *
     * Checksum reflects this packet's content, and the content of
     *  all antecedant packets. This works because packets arrange all
     *  key content first, follwed by all value content; as meta-data
     *  checksuming is isolated from content_crc, any distribution of
     *  content amount different packets will result in the same
     *  content_crc state.
     */ 
    uint32_t compute_content_checksum(boost::crc_32_type & content_crc) const;

    /*!
     * \brief Computes a checksum over (only) this packet's meta-data.
     *
     * Added to the CRC:
     *  - hash_chain_next()
     *  - is_dead()
     *  - continues_sequence()
     *  - completes_sequence()
     */
    uint32_t compute_meta_checksum() const;

    /*!
     * \brief Computes a combined (xor) checksum from content & meta-data
     */
    uint32_t compute_combined_checksum(boost::crc_32_type & content_crc) const;

    uint32_t combined_checksum() const
    { return _meta.combined_checksum; }

    /*!
     * \brief Update the checksum of the packet
     * @param checksum Must be returned by compute_checksum()
     */
    void set_combined_checksum(uint32_t checksum)
    { _meta.combined_checksum = checksum; }

    /*!
     * \brief Folds an update to metadata into the combined checksum
     *
     * Avoids re-computing content portions of the checksum. Previous
     *  meta checksum must be provided. 
     */
    void update_meta_of_combined_checksum(uint32_t old_meta_checksum);

    /// Location of next element in linear hash table
    uint32_t hash_chain_next() const
    { return _meta.hash_chain_next; }

    /// Update location of next record in linear hash table
    void set_hash_chain_next(uint32_t next)
    { _meta.hash_chain_next = next; }

    /// Whether this packet contains no data, and may be reclaimed
    bool is_dead() const
    { return _meta.is_dead; }

    /// Marks the packet as reclaimable
    void set_dead()
    { _meta.is_dead = true; }

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
    uint32_t capacity() const
    { return (_meta.capacity << 2) + capacity_alignment_adjustment(); }

    /// Remaining storage capacity of the packet
    uint32_t available_capacity() const
    { return capacity() - key_length() - value_length(); }

    uint32_t key_length() const
    { return _meta.key_length; }

    const char * key_begin() const
    { return reinterpret_cast<const char*>(this) + header_length(); }

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


    uint32_t value_length() const
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

    uint32_t packet_length() const
    { return header_length() + capacity(); }

    /// Byte-length of packet header
    static uint32_t header_length()
    { return sizeof(packet); }

    /// Byte-alignment of the (packed) packet header structure
    static uint32_t header_alignment()
    { return sizeof(_meta) % sizeof(uint32_t); }

    /// Correcting byte-alignment of capacity required
    ///  to ensure that packet is uint32_t aligned
    static uint32_t capacity_alignment_adjustment()
    { return header_alignment ? (sizeof(uint32_t) - header_alignment()) : 0; }

    /// Maximum representable capacity of a packet 
    static uint32_t max_capacity()
    { return (1<<13) - header_alignment(); }

    /// Minimum byte-length of an (uint32_t-aligned) packet
    static uint32_t min_packet_byte_length()
    { return header_length() + capacity_alignment_adjustment(); }

    /// Maximum byte-length of a packet
    static uint32_t max_packet_byte_length()
    { return header_length() + max_capacity(); }

private:

    struct {

        uint32_t combined_checksum;

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
