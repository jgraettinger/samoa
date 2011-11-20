
#include "samoa/persistence/rolling_hash_record.hpp"
#include "samoa/error.hpp"
#include <boost/crc.hpp>


namespace samoa {
namespace persistence {

rolling_hash_record::rolling_hash_record(unsigned capacity)
{
    // capacity must be 4-byte aligned
    SAMOA_ASSERT(!(capacity & 0x00000003));

    _meta.next = 0;
    _meta.crc_32 = 0;
    _meta.is_dead = false;
    _meta.continues_chain = false;
    _meta.completes_chain = false;
    _meta.capacity = (capacity >> 2);
    _meta.key_length = 0;
    _meta.value_length = 0;
}

bool rolling_hash_record::check_integrity() const
{
    if(key_length() + value_length() > capacity())
        return false;

    if(compute_crc() != _meta.crc_32)
        return false;

    return true;
}

void rolling_hash_record::compute_crc_32()
{
    boost::crc_32_type crc;

    crc.process_bytes(&_meta.next, sizeof(_meta.next));
    crc.process_bit(is_dead());
    crc.process_bit(continues_sequence());
    crc.process_bit(completes_sequence());
    crc.process_block(key_begin(), key_end());
    crc.process_block(value_begin(), value_end());

    return crc.checksum();
}

void rolling_hash_record::set_dead() const
{
    _meta.next = 0;
    _meta.is_dead = true;
    _meta.continues_chain = false;
    _meta.completes_chain = false;
    _meta.key_length = 0;
    _meta.value_length = 0;

    set_crc_32(compute_crc_32());
}

void rolling_hash_record::set_key(offset_t key_length)
{
    SAMOA_ASSERT(!(key_length() || value_length()));
    SAMOA_ASSERT(key_length <= available_capacity());

    _meta.key_length = key_length;
    return reinterpret_cast<char*>(this) + header_size();
}

void rolling_hash_record::set_value(offset_t value_length)
{
    SAMOA_ASSERT(value_length <= (available_capacity() + value_length()));

    _meta.value_length = value_length;
    return reinterpret_cast<char*>(this) + header_size() + key_length();
}

}
}

