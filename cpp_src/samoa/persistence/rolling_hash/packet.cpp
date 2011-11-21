
#include "samoa/persistence/rolling_hash/packet.hpp"
#include "samoa/error.hpp"
#include <boost/crc.hpp>


namespace samoa {
namespace persistence {
namespace rolling_hash {

packet::packet(unsigned capacity)
 :  _meta({0, 0, false, false, false, capacity >> 2, 0, 0})
{
    SAMOA_ASSERT(capacity % sizeof(uint32_t) == capacity_alignment_adjustment());
}

bool packet::check_integrity() const
{
    if(key_length() + value_length() > capacity())
        return false;

    if(compute_crc_32() != _meta.crc_32)
        return false;

    return true;
}

uint32_t packet::compute_crc_32() const
{
    boost::crc_32_type crc;

    crc.process_bytes(&_meta.hash_chain_next, sizeof(_meta.hash_chain_next));
    crc.process_byte(is_dead());
    crc.process_byte(continues_sequence());
    crc.process_byte(completes_sequence());
    crc.process_block(key_begin(), key_end());
    crc.process_block(value_begin(), value_end());

    return crc.checksum();
}

void packet::set_dead()
{
    _meta.is_dead = true;
    _meta.key_length = 0;
    _meta.value_length = 0;

    set_crc_32(compute_crc_32());
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

