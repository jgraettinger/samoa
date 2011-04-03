
#include <algorithm>

namespace samoa {
namespace persistence {

template<typename KeyIterator>
record::record(
    const KeyIterator & k_begin, const KeyIterator & k_end,
    unsigned value_length)
{
    _meta.next = 0;
    _meta.is_dead = false;
    _meta.key_length = std::distance(k_begin, k_end);
    _meta.value_length = value_length;

    std::copy(k_begin, k_end, (char*)key_begin());
}

inline record::offset_t record::allocated_size(
    size_t key_length, size_t value_length)
{
    offset_t rec_len = header_size() + key_length + value_length;

    // records are aligned to sizeof(offset_t)
    if(rec_len % sizeof(offset_t))
        rec_len += sizeof(offset_t) - (rec_len % sizeof(offset_t));

    return rec_len;
}

}
}

