
#include "samoa/persistence/rolling_hash/packet.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace persistence {
namespace rolling_hash {

packet::packet(unsigned capacity)
 :  _meta({0, 0, false, false, false, capacity >> 2, 0, 0})
{
    SAMOA_ASSERT(capacity % sizeof(uint32_t) == \
        capacity_alignment_adjustment());
}

bool packet::check_integrity(boost::crc_32_type & content_crc) const
{
    if(key_length() + value_length() > capacity())
        return false;

    if(compute_combined_checksum(content_crc) != combined_checksum())
        return false;

    return true;
}

uint32_t packet::compute_content_checksum(
    boost::crc_32_type & content_crc) const
{
    content_crc.process_block(key_begin(), key_end());
    content_crc.process_block(value_begin(), value_end());

    return content_crc.checksum();
}

uint32_t packet::compute_meta_checksum() const
{
    boost::crc_32_type meta_crc;

    meta_crc.process_bytes(&_meta.hash_chain_next,
        sizeof(_meta.hash_chain_next));
    meta_crc.process_byte(is_dead());
    meta_crc.process_byte(continues_sequence());
    meta_crc.process_byte(completes_sequence());

    return meta_crc.checksum();
}

uint32_t packet::compute_combined_checksum(
    boost::crc_32_type & content_crc) const
{
    return compute_meta_checksum() ^ compute_content_checksum(content_crc);
}

void packet::update_meta_of_combined_checksum(uint32_t old_meta_checksum)
{
    // subtract old meta checksum component, and re-add current checksum
    _meta.combined_checksum ^= old_meta_checksum;
    _meta.combined_checksum ^= compute_meta_checksum();
}

char * packet::set_key(uint32_t new_key_length)
{
    SAMOA_ASSERT(!(key_length() || value_length()));
    SAMOA_ASSERT(new_key_length <= available_capacity());

    _meta.key_length = new_key_length;
    return reinterpret_cast<char*>(this) + header_length();
}

char * packet::set_value(uint32_t new_value_length)
{
    SAMOA_ASSERT(new_value_length <= (available_capacity() + value_length()));

    _meta.value_length = new_value_length;
    return reinterpret_cast<char*>(this) + header_length() + key_length();
}

}
}
}

