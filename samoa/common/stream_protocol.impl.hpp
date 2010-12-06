#ifndef COMMON_STREAM_PROTOCOL_IMPL_HPP
#define COMMON_STREAM_PROTOCOL_IMPL_HPP

#include "samoa/common/stream_protocol.hpp"

namespace common {

inline stream_protocol::stream_protocol(
    std::auto_ptr<boost::asio::ip::tcp::socket> & sock
) :
    _sock_write_ind(0),
    _in_read(false),
    _in_write(false),
    _r_ring(),
    _w_ring(),
    _sock(sock)
{ }


template<typename Callback>
void stream_protocol::read_regex(
    const boost::regex & regex,
    size_t max_read_length,
    const  Callback & callback)
{
    on_regex_read(
        boost::system::error_code(),
        0, regex, max_read_length,
        // wrap to avoid eval as boost::bind
        //  func composition
        boost::protect(callback));

    return;
}

template<typename Callback>
void stream_protocol::read_until(
    char delim_char,
    size_t max_read_length,
    const Callback & callback)
{
    on_delim_read(
        boost::system::error_code(),
        0, delim_char, max_read_length,
        // wrap to avoid eval as boost::bind
        //  func composition
        boost::protect(callback));

    return;
}

template<typename Callback>
void stream_protocol::read_data(
    size_t read_length,
    buffer_regions_t & data_regions,
    const Callback & callback)
{
    on_data_read(
        boost::system::error_code(),
        0, read_length, data_regions,
        // wrap to avoid eval as boost::bind
        //  func composition
        boost::protect(callback));

    return;
}

inline void stream_protocol::queue_write( const const_buffer_region & b)
{ _sock_write_regions.push_back(b); }

inline void stream_protocol::queue_write( const const_buffer_regions_t & bs)
{
    _sock_write_regions.insert(_sock_write_regions.end(),
        bs.begin(), bs.end());
}

template<typename Iterator>
void stream_protocol::queue_write(const Iterator & begin, const Iterator & end)
{
    assert(_w_ring.available_read() == 0);
    _w_ring.produce_range(begin, end);
    _w_ring.get_read_regions(_sock_write_regions);
    _w_ring.consumed( _w_ring.available_read());
    return;
}

template<typename Callback>
void stream_protocol::write_queued(const Callback & callback)
{
    assert(!_in_write);
    _in_write = true;

    on_write(boost::system::error_code(), 0,
        // wrap to avoid eval as boost::bind
        //   func composition
        boost::protect(callback)
    );
    return;
}

template<typename Callback>
void stream_protocol::on_regex_read(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    const  boost::regex & regex,
    size_t max_read_length,
    const  Callback & callback)
{
    if(ec)
    {
        callback(ec, match_results_t());
        return;
    }

    post_socket_read(bytes_transferred);

    // Identify regions to match against
    _match_regions.clear();
    _r_ring.get_read_regions(_match_regions, max_read_length);

    // contiguous iteration facade
    //   over non-contiguous buffers
    buffers_iterator_t begin(boost::asio::buffers_begin(_match_regions));
    buffers_iterator_t end(boost::asio::buffers_end(_match_regions));

    match_results_t match;

    if(boost::regex_search(begin, end, match, regex,
        boost::regex_constants::match_continuous))
    {
        // mark match as delivered
        _r_ring.consumed(std::distance(begin, match[0].second));
        // call back to client w/ matches
        callback(ec, match);
        return;
    }

    // No match
    if(_r_ring.available_read() >= max_read_length)
    {
        // No remaining bytes to read; signal an error
        callback(boost::system::errc::make_error_code(
            boost::system::errc::value_too_large), match_results_t());
        return;
    }

    // pre-allocate for read
    pre_socket_read(max_read_length);

    // Schedule a read
    socket().async_read_some(_sock_read_regions, boost::bind(
        &stream_protocol::template on_regex_read<Callback>, this,
        boost::asio::placeholders::error,
        boost::asio::placeholders::bytes_transferred,
        regex, max_read_length, callback));

    return;
}

template<typename Callback>
void stream_protocol::on_delim_read(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    char   delim,
    size_t max_read_length,
    const  Callback & callback)
{
    if(ec)
    {
        callback(ec, buffers_iterator_t(), buffers_iterator_t());
 
        return;
    }

    post_socket_read(bytes_transferred);

    // Identify regions to match against
    _match_regions.clear();
    _r_ring.get_read_regions(_match_regions, max_read_length);

    // contiguous iteration facade
    //   over non-contiguous buffers
    buffers_iterator_t begin(boost::asio::buffers_begin(_match_regions));
    buffers_iterator_t end(boost::asio::buffers_end(_match_regions));

    buffers_iterator_t match = std::find(begin, end, delim);

    if(match != end)
    {
        // mark match as delivered
        _r_ring.consumed(std::distance(begin, match));
        // call back to client w/ matches
        callback(ec, begin, match);
        return;
    }

    // No match
    if(_r_ring.available_read() >= max_read_length)
    {
        // No remaining bytes to read; signal an error
        callback(boost::system::errc::make_error_code(
            boost::system::errc::value_too_large),
            buffers_iterator_t(), buffers_iterator_t());
        return;
    }

    // pre-allocate for read
    pre_socket_read(max_read_length);

    // Schedule a read
    socket().async_read_some(_sock_read_regions, boost::bind(
        &stream_protocol::template on_delim_read<Callback>, this,
        boost::asio::placeholders::error,
        boost::asio::placeholders::bytes_transferred,
        delim, max_read_length, callback));

    return;
}

template<typename Callback>
void stream_protocol::on_data_read(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    size_t read_length,
    buffer_regions_t & data_regions,
    const  Callback & callback
)
{
    if(ec)
    {
        callback(ec, 0);
        return;
    }

    post_socket_read(bytes_transferred);

    if(_r_ring.available_read() >= read_length)
    {
        // read finished

        // extract data regions
        _r_ring.get_read_regions(data_regions, read_length);
        // mark as delivered
        _r_ring.consumed(read_length);
        // call back to client
        callback(ec, read_length);
        return;
    }

    // Not done yet

    // Schedule read-to-completion
    pre_socket_read(read_length);

    boost::asio::async_read(socket(), _sock_read_regions,
        boost::bind(&stream_protocol::template on_data_read<Callback>, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred,
            read_length, boost::ref(data_regions), callback));

    return;
}


template<typename Callback>
void stream_protocol::on_write(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    const  Callback & callback
)
{
    if(ec)
    {
        callback(ec);
    }

    // temp
    if(bytes_transferred)
    {
        size_t pending = 0;
        for(size_t i = 0; i != _sock_write_ind; ++i)
            pending += _sock_write_regions[i].size();
        assert(pending == bytes_transferred);
    }
    // end temp

    _sock_write_regions.erase(
        _sock_write_regions.begin(),
        _sock_write_regions.begin() + _sock_write_ind
    );
    _sock_write_ind = 0;
    _in_write = false;

    if(_sock_write_regions.empty())
    {
        // No more to write
        callback(ec);
        return;
    }

    _in_write = true;
    _sock_write_ind = _sock_write_regions.size();
    boost::asio::async_write(socket(), _sock_write_regions,
        boost::bind(&stream_protocol::template on_write<Callback>,
            this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred,
            callback));

    return;
}

inline void stream_protocol::pre_socket_read(size_t read_target)
{
    assert(read_target > _r_ring.available_read());

    _r_ring.reserve(read_target);

    _sock_read_regions.clear();
    _r_ring.get_write_regions(_sock_read_regions,
        read_target - _r_ring.available_read());

    _in_read = true;
    return;
}

inline void stream_protocol::post_socket_read(size_t bytes_read)
{
    _r_ring.produced(bytes_read);
    _in_read = false;
    return;
}

}

#endif // guard
