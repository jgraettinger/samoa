#ifndef SAMOA_CORE_STREAM_PROTOCOL_IMPL_HPP
#define SAMOA_CORE_STREAM_PROTOCOL_IMPL_HPP

#include "samoa/core/stream_protocol.hpp"

namespace samoa {
namespace core {

template<typename Iterator>
void stream_protocol_write_interface::queue_write(
    const Iterator & begin, const Iterator & end)
{
    SAMOA_ASSERT(_ring.available_read() == 0);
    _ring.produce_range(begin, end);
    _ring.get_read_regions(_regions);
    _ring.consumed(_ring.available_read());
}

}
}

#endif

