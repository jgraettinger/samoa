#ifndef SAMOA_REQUEST_CLIENT_STATE_IMPL_HPP
#define SAMOA_REQUEST_CLIENT_STATE_IMPL_HPP

#include "samoa/request/client_state.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace request {

template<typename Iterator>
void client_state::add_response_data_block(
    const Iterator & begin, const Iterator & end)
{
    SAMOA_ASSERT(!_flush_response_called);
    SAMOA_ASSERT(_w_ring.available_read() == 0);

    _w_ring.produce_range(begin, end);

    unsigned length = _w_ring.available_read();

    _samoa_response.add_data_block_length(length);
    _w_ring.get_read_regions(_response_data);
    _w_ring.consumed(length);
}

}
}

#endif
