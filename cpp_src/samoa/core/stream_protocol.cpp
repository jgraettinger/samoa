
#include "samoa/core/stream_protocol.hpp"

namespace samoa {
namespace core {

////////////////////////////////////////////////////////////////////////
//  stream_protocol_read_interface

stream_protocol_read_interface::stream_protocol_read_interface()
 : _in_read(false)
{ }

void stream_protocol_read_interface::read_regex(
    const  stream_protocol_read_interface::read_regex_callback_t & callback,
    const  boost::regex & regex,
    size_t max_read_length)
{
    assert(!_in_read);
    _in_read = true;

    on_regex_read(
        boost::system::error_code(),
        0, regex, max_read_length, callback);
}

void stream_protocol_read_interface::read_line(
    const stream_protocol_read_interface::read_line_callback_t & callback,
    char delim_char /* = '\n' */,
    size_t max_read_length /* = 1024 */)
{
    assert(!_in_read);
    _in_read = true;

    on_read_line(
        boost::system::error_code(),
        0, delim_char, max_read_length, callback);
}

void stream_protocol_read_interface::read_data(
    const stream_protocol_read_interface::read_data_callback_t & callback,
    size_t read_length)
{
    assert(!_in_read);
    _in_read = true;

    on_read_data(
        boost::system::error_code(),
        0, read_length, callback);
}

void stream_protocol_read_interface::pre_socket_read(size_t read_target)
{
    assert(read_target > _r_ring.available_read());

    _r_ring.reserve(read_target);

    _r_regions.clear();
    _r_ring.get_write_regions(_r_regions,
        read_target - _r_ring.available_read());
}

void stream_protocol_read_interface::post_socket_read(size_t bytes_read)
{ _r_ring.produced(bytes_read); }

void stream_protocol_read_interface::on_regex_read(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    const  boost::regex & regex,
    size_t max_read_length,
    const  stream_protocol_read_interface::read_regex_callback_t & callback)
{
    if(ec)
    {
        _in_read = false;
        callback(ec, match_results_t());
        return;
    }

    post_socket_read(bytes_transferred);

    // Identify regions to match against
    _r_regions.clear();
    _r_ring.get_read_regions(_r_regions, max_read_length);

    // contiguous iteration facade
    //   over non-contiguous buffers
    buffers_iterator_t begin(boost::asio::buffers_begin(_r_regions));
    buffers_iterator_t end(boost::asio::buffers_end(_r_regions));

    match_results_t match;

    if(boost::regex_search(begin, end, match, regex,
        boost::regex_constants::match_continuous))
    {
        // mark match as delivered
        _r_ring.consumed(std::distance(begin, match[0].second));
        _in_read = false;
        // call back to client w/ matches
        callback(ec, match);
        return;
    }

    // No match
    if(_r_ring.available_read() >= max_read_length)
    {
        // No remaining bytes to read; signal an error
        _in_read = false;
        callback(boost::system::errc::make_error_code(
            boost::system::errc::value_too_large), match_results_t());
        return;
    }

    // pre-allocate for read
    pre_socket_read(max_read_length);

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol*>(this)->get_socket());

    // Schedule a partial read
    sock.async_read_some(_r_regions, boost::bind(
        &stream_protocol_read_interface::on_regex_read, this,
        boost::asio::placeholders::error,
        boost::asio::placeholders::bytes_transferred,
        regex, max_read_length, callback));
}

void stream_protocol_read_interface::on_read_line(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    char   delim,
    size_t max_read_length,
    const stream_protocol_read_interface::read_line_callback_t & callback)
{
    if(ec)
    {
        _in_read = false;
        callback(ec, buffers_iterator_t(), buffers_iterator_t());
        return;
    }

    post_socket_read(bytes_transferred);

    // Identify regions to match against
    _r_regions.clear();
    _r_ring.get_read_regions(_r_regions, max_read_length);

    // contiguous iteration facade
    //   over non-contiguous buffers
    buffers_iterator_t begin(boost::asio::buffers_begin(_r_regions));
    buffers_iterator_t end(boost::asio::buffers_end(_r_regions));

    buffers_iterator_t match = std::find(begin, end, delim);

    if(match != end)
    {
        // 'match' points at delim char; move one beyond
        ++match;
        // mark match as delivered
        _r_ring.consumed(std::distance(begin, match));
        // call back to client w/ matches
        _in_read = false;
        callback(ec, begin, match);
        return;
    }

    // No match
    if(_r_ring.available_read() >= max_read_length)
    {
        // No remaining bytes to read; signal an error
        _in_read = false;
        callback(boost::system::errc::make_error_code(
            boost::system::errc::value_too_large),
            buffers_iterator_t(), buffers_iterator_t());
        return;
    }

    // pre-allocate for read
    pre_socket_read(max_read_length);

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol*>(this)->get_socket());

    // Schedule a partial read
    sock.async_read_some(_r_regions, boost::bind(
        &stream_protocol::on_read_line, this,
        boost::asio::placeholders::error,
        boost::asio::placeholders::bytes_transferred,
        delim, max_read_length, callback));
}

void stream_protocol_read_interface::on_read_data(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    size_t read_length,
    const  stream_protocol_read_interface::read_data_callback_t & callback)
{
    if(ec)
    {
        _in_read = false;
        callback(ec, 0, buffer_regions_t());
        return;
    }

    post_socket_read(bytes_transferred);

    if(_r_ring.available_read() >= read_length)
    {
        // read finished

        // extract data regions
        _r_regions.clear();
        _r_ring.get_read_regions(_r_regions, read_length);
        // mark as delivered
        _r_ring.consumed(read_length);
        // call back to client
        _in_read = false;
        callback(ec, read_length, _r_regions);
        return;
    }

    // Not done yet

    // pre-allocate for read
    pre_socket_read(read_length);

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol*>(this)->get_socket());

    // Schedule read-till-completion
    boost::asio::async_read(sock, _r_regions,
        boost::bind(&stream_protocol::on_read_data, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred,
            read_length, callback));
}

////////////////////////////////////////////////////////////////////////
//  stream_protocol_write_interface

stream_protocol_write_interface::stream_protocol_write_interface()
 : _in_write(false)
{ }

bool stream_protocol_write_interface::has_queued_writes() const
{ return !_w_regions.empty(); }

void stream_protocol_write_interface::queue_write(
    const const_buffer_region & b)
{ _w_regions.push_back(b); }

void stream_protocol_write_interface::queue_write(
    const const_buffer_regions_t & bs)
{ _w_regions.insert(_w_regions.end(), bs.begin(), bs.end()); }

void stream_protocol_write_interface::write_queued(
    const stream_protocol_write_interface::write_queued_callback_t & callback)
{
    assert(!_in_write);
    _in_write = true;

    boost::asio::ip::tcp::socket & sock(
        static_cast<stream_protocol*>(this)->get_socket());

    // Schedule write-till-completion
    boost::asio::async_write(sock, _w_regions,
        boost::bind(&stream_protocol_write_interface::on_write_queued, this,
        boost::asio::placeholders::error,
        boost::asio::placeholders::bytes_transferred,
        callback));
}

void stream_protocol_write_interface::on_write_queued(
    const  boost::system::error_code & ec,
    size_t bytes_transferred,
    const  stream_protocol_write_interface::write_queued_callback_t & callback)
{
    // temp
    if(!ec)
    {
        size_t pending = 0;
        for(size_t i = 0; i != _w_regions.size(); ++i)
            pending += _w_regions[i].size();
        assert(pending == bytes_transferred);
    }
    // end temp

    _w_regions.clear();
    _in_write = false;

    callback(ec, bytes_transferred);
}

////////////////////////////////////////////////////////////////////////
//  stream_protocol

stream_protocol::stream_protocol(
    io_service_ptr_t io_srv,
    std::unique_ptr<boost::asio::ip::tcp::socket> & sock
) :
    stream_protocol_read_interface(),
    stream_protocol_write_interface(),
    _sock(std::move(sock)),
    _io_srv(io_srv)
{ }

stream_protocol::~stream_protocol()
{ }

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

