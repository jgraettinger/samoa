
#include "samoa/core/stream_protocol.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <functional>

namespace samoa {
namespace core {

////////////////////////////////////////////////////////////////////////
//  stream_protocol_read_interface

void stream_protocol_read_interface::read(
    read_callback_t callback, weak_ptr_t w_self, size_t length)
{
    SAMOA_ASSERT(!_in_read);
    _in_read = true;

    on_read(std::move(w_self), std::move(callback),
        boost::system::error_code(), length, 0);
}

/* static */
void stream_protocol_read_interface::on_read(
    weak_ptr_t w_self,
    read_callback_t callback,
    boost::system::error_code ec,
    size_t target_length,
    size_t bytes_transferred)
{
    // was stream_protocol destroyed during the operation?
    ptr_t self = w_self.lock();
    if(!self)
    {
        LOG_INFO("post-dtor callback");
        return;
    }
    if(ec)
    {
        callback(std::move(self), ec, buffer_regions_t());
        return;
    }

    self->_ring.produced(bytes_transferred);

    if(self->_ring.available_read() >= target_length)
    {
        // read finished; extract containing buffers & return to client
        buffer_regions_t regions;
        self->_ring.get_read_regions(regions, target_length);
        self->_ring.consumed(target_length);
        self->_in_read = false;

        callback(std::move(self), ec, std::move(regions));
        return;
    }

    // not enough buffered data to satisfy the request; begin an async read

    // ensure the larger of the read remainder, or a half buffer is reserved
    size_t reserve_length = std::max<size_t>(
        target_length - self->_ring.available_read(),
        ref_buffer::allocation_size / 2);

    self->_ring.reserve(reserve_length);

    buffer_regions_t regions;
    self->_ring.get_write_regions(regions);

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol&>(*self).get_socket());

    // schedule read of available data, bound by _w_regions capacity
    sock.async_read_some(std::move(regions), [
            w_self = std::move(w_self),
            callback = std::move(callback),
            target_length
        ](boost::system::error_code ec, size_t read_length)
        {
            on_read(std::move(w_self),
                std::move(callback), ec,
                target_length, read_length);
        });
}

////////////////////////////////////////////////////////////////////////
//  stream_protocol_write_interface

bool stream_protocol_write_interface::has_queued_writes() const
{ return !_regions.empty(); }

void stream_protocol_write_interface::queue_write(
    const buffer_regions_t & bs)
{ _regions.insert(_regions.end(), bs.begin(), bs.end()); }

void stream_protocol_write_interface::write(
    write_callback_t callback, weak_ptr_t w_self)
{
    SAMOA_ASSERT(!_in_write);
    _in_write = true;

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol&>(*this).get_socket());

    // Schedule write-till-completion
    boost::asio::async_write(sock, std::move(_regions),
        [w_self = std::move(w_self),
            callback = std::move(callback)](
                boost::system::error_code ec, size_t)
        {
            ptr_t self = w_self.lock();
            if(!self)
            {
                // destroyed during operation
                LOG_INFO("post-dtor callback");
                return;
            }

            self->_in_write = false;
            callback(std::move(self), ec);
        });
}

////////////////////////////////////////////////////////////////////////
//  stream_protocol

stream_protocol::stream_protocol(
    std::unique_ptr<boost::asio::ip::tcp::socket> sock)
 :  _sock(std::move(sock))
{
    SAMOA_ASSERT(_sock);
}

/* virtual */
stream_protocol::~stream_protocol()
{
    // destroy socket from it's own io_service
    _sock->get_io_service().dispatch(
        [sock = _sock.release()]{ delete sock; });
}

std::string stream_protocol::get_local_address() const
{ return get_socket().local_endpoint().address().to_string(); }

std::string stream_protocol::get_remote_address() const
{ return get_socket().remote_endpoint().address().to_string(); }

unsigned stream_protocol::get_local_port() const
{ return get_socket().local_endpoint().port(); }

unsigned stream_protocol::get_remote_port() const
{ return get_socket().remote_endpoint().port(); }

bool stream_protocol::is_open() const
{
    return _sock->is_open();
}

void stream_protocol::close()
{
    _sock->close();
}

};
};

