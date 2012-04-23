#ifndef SAMOA_CLIENT_SERVER_IMPL_HPP
#define SAMOA_CLIENT_SERVER_IMPL_HPP

namespace samoa {
namespace client {

template<typename Iterator>
void server_request_interface::add_data_block(
    const Iterator & begin, const Iterator & end) const
{
    core::buffer_ring ring;
    ring.produce_range(begin, end);

    _srv->_samoa_request.add_data_block_length(ring.available_read());
    ring.get_read_regions(_srv->_request_data);
    ring.consumed(ring.available_read());
}

}
}

#endif

