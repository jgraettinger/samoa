#ifndef SAMOA_CORE_STREAM_PROTOCOL_HPP
#define SAMOA_CORE_STREAM_PROTOCOL_HPP

#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include <boost/regex.hpp>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>
#include <memory>

namespace samoa {
namespace core {

class stream_protocol : private boost::noncopyable
{
public:

    typedef boost::match_results<buffers_iterator_t
        > match_results_t;

    stream_protocol(std::auto_ptr<boost::asio::ip::tcp::socket> & sock);

    // Underlying socket
    boost::asio::ip::tcp::socket & socket()
    { return *_sock; }

    // Is currently in a read operation?
    bool in_read() const
    { return _in_read; }

    // Is currently in a write operation?
    bool in_write() const
    { return _in_write; }

    // Initiates an asynchronous read of the regex
    //
    // Callback signature:
    //
    //  callback(boost::system::error_code,
    //      stream_protocol::match_results_t);
    //
    // Note that results are invalidated 
    //  at the start of the next read operation.
    template<typename Callback>
    void read_regex(
        const  boost::regex &,
        size_t max_read_length,
        const  Callback & callback
    );

    // Initiates an asynchronous read to the
    //   delimiating character
    //
    // Callback signature:
    //
    //  callback(boost::system::error_code,
    //      core::buffers_iterator_t begin,
    //      core::buffers_iterator_t end);
    //
    // Note that results are invalidated
    //  at the start of the next read operation.
    template<typename Callback>
    void read_until(
        char   delim_char,
        size_t max_read_length,
        const  Callback & callback);

    // Initiates an asynchronous read of read_length
    //  bytes from the socket.
    //
    // Callback signature:
    //
    //   callback(boost::system::error_code,
    //       size_t read_length,
    //       const buffer_regions_t &);
    //
    // Note: read_length is returned for convienence only.
    //   The operation will either read the entire amount of
    //   requested data, or will return with an error.
    //
    //  Result buffer_regions_t is invalidated at the start
    //   of the next read operation.
    template<typename Callback>
    void read_data(
        size_t read_length,
        const Callback & callback);

    // queue_write(*) - schedule buffer for writing to the client,
    //  using gather-IO.
    //
    // It is an error to call queue_write() while a
    //  write operation is in progress.
    void queue_write(const const_buffer_region &);
    void queue_write(const const_buffer_regions_t &);

    template<typename Iterator>
    void queue_write(const Iterator & begin, const Iterator & end);

    void queue_write(const std::string & str)
    { queue_write(str.begin(), str.end()); }

    // Initiates an asynchronous write of queued buffer
    //  regions to the socket.
    //
    // Callback signature:
    //
    //   callback(boost::system::error_code,
    //       size_t write_length)
    //
    // Note: write_length is returned for convienence only.
    //   The operation will either write the entire amount of
    //   requested data, or will return with an error.
    //
    template<typename Callback>
    void write_queued(const Callback & callback);

private:

    template<typename Callback>
    void on_regex_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  boost::regex & regex,
        size_t max_read_length,
        const  Callback & callback);

    template<typename Callback>
    void on_delim_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        char   delim,
        size_t max_read_length,
        const  Callback & callback);

    template<typename Callback>
    void on_data_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        size_t read_length,
        const  Callback & callback);

    template<typename Callback>
    void on_write(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  Callback & callback
    );

    void pre_socket_read(size_t read_target);

    void post_socket_read(size_t bytes_read);

    // Reusable containers of regions for pending socket ops
    buffer_regions_t       _sock_read_regions;
    const_buffer_regions_t _sock_write_regions;

    //  Reusable array of regions to match over
    buffer_regions_t _result_regions;

    bool _in_read, _in_write;

    buffer_ring _r_ring, _w_ring;
    std::auto_ptr<boost::asio::ip::tcp::socket> _sock;
};

}
}

#include "samoa/core/stream_protocol.impl.hpp"

#endif

