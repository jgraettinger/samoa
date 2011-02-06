#ifndef SAMOA_CORE_STREAM_PROTOCOL_IMPL_HPP
#define SAMOA_CORE_STREAM_PROTOCOL_IMPL_HPP

#include "samoa/core/stream_protocol.hpp"

namespace samoa {
namespace core {

template<typename Iterator>
void stream_protocol_write_interface::queue_write(
    const Iterator & begin, const Iterator & end)
{
    assert(_w_ring.available_read() == 0);
    _w_ring.produce_range(begin, end);
    _w_ring.get_read_regions(_w_regions);
    _w_ring.consumed(_w_ring.available_read());
}

}
}

#endif

