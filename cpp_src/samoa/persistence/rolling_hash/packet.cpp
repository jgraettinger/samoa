
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

bool packet::check_integrity(core::murmur_checksummer & content_cs) const
{
    if(key_length() + value_length() > capacity())
        return false;

    if(compute_combined_checksum(content_cs) != combined_checksum())
        return false;

    return true;
}

uint32_t packet::compute_content_checksum(
    core::murmur_checksummer & content_cs) const
{
    content_cs.process_bytes(key_begin(), key_length());
    content_cs.process_bytes(value_begin(), value_length());

    return (uint32_t)(content_cs.checksum()[0] >> 32);
}

uint32_t packet::compute_meta_checksum() const
{
    uint32_t meta_bytes[] = {
        _meta.hash_chain_next,
        is_dead(),
        continues_sequence(),
        completes_sequence()};

    core::murmur_checksummer cs;
    cs.process_bytes(&meta_bytes, sizeof(meta_bytes));
    return (uint32_t)(cs.checksum()[0] >> 32);
}

uint32_t packet::compute_combined_checksum(
    core::murmur_checksummer & content_cs) const
{
    return compute_meta_checksum() ^ compute_content_checksum(content_cs);
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

