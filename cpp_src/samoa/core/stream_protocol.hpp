#ifndef SAMOA_CORE_STREAM_PROTOCOL_HPP
#define SAMOA_CORE_STREAM_PROTOCOL_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/core/buffer_ring.hpp"
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/regex.hpp>
#include <boost/system/error_code.hpp>

namespace samoa {
namespace core {

class stream_protocol_read_interface
    : private boost::noncopyable
{
public:

    stream_protocol_read_interface();

    typedef boost::match_results<buffers_iterator_t
        > match_results_t;

    // match_results_t argument is invalidated by
    //  the next call to read_*
    typedef boost::function<
        void(const boost::system::error_code &,
            const match_results_t &)
    > read_regex_callback_t;

    void read_regex(
        const  read_regex_callback_t &,
        const  boost::regex &,
        size_t max_read_length);

    // iteration range is invalidated by
    //  the next call to read_*
    typedef boost::function<
        void(const boost::system::error_code &,
            const buffers_iterator_t & begin,
            const buffers_iterator_t & end)
    > read_line_callback_t;

    void read_line(
        const  read_line_callback_t &,
        char   delim_char = '\n',
        size_t max_read_length = 1024);

    // buffer_regions_t argument is invalidated by
    //  the next call to read_*
    typedef boost::function<
        void(const boost::system::error_code &,
            size_t, const buffer_regions_t &)
    > read_data_callback_t;

    void read_data(
        const read_data_callback_t &,
        size_t read_length);

private:

    void pre_socket_read(size_t read_target);
    void post_socket_read(size_t bytes_read);

    void on_regex_read(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  boost::regex & regex,
        size_t max_read_length,
        const  read_regex_callback_t &);

    void on_read_line(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        char   delim,
        size_t max_read_length,
        const  read_line_callback_t &);

    void on_read_data(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        size_t read_length,
        const  read_data_callback_t &);

    bool _in_read;
    buffer_ring _r_ring;
    buffer_regions_t _r_regions;
};

class stream_protocol_write_interface
    : private boost::noncopyable
{
public:

    stream_protocol_write_interface();

    bool has_queued_writes() const;

    // queue_write(*) - schedule buffer for writing to the client,
    //  using gather-IO.
    //
    // It is an error to call queue_write() while a
    //  write operation is in progress.
    void queue_write(const const_buffer_region &);
    void queue_write(const const_buffer_regions_t &);
    void queue_write(const buffer_regions_t &);

    template<typename Iterator>
    void queue_write(const Iterator & begin, const Iterator & end);

    void queue_write(const std::string & str)
    { queue_write(str.begin(), str.end()); }

    typedef boost::function<
        void(const boost::system::error_code &, size_t)
    > write_queued_callback_t;

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
    void write_queued(const write_queued_callback_t &);

private:

    void on_write_queued(
        const  boost::system::error_code & ec,
        size_t bytes_transferred,
        const  write_queued_callback_t &);

    bool _in_write;
    buffer_ring _w_ring;
    const_buffer_regions_t _w_regions;
};

class stream_protocol :
    protected stream_protocol_read_interface,
    protected stream_protocol_write_interface
{
public:

    stream_protocol_read_interface::match_results_t;

    typedef stream_protocol_read_interface read_interface_t;
    typedef stream_protocol_write_interface write_interface_t;

    stream_protocol(const io_service_ptr_t &,
        std::unique_ptr<boost::asio::ip::tcp::socket> & sock);

    virtual ~stream_protocol();

    std::string get_local_address() const;
    std::string get_remote_address() const;

    unsigned get_local_port() const;
    unsigned get_remote_port() const;

    const io_service_ptr_t & get_io_service()
    { return _io_srv; }

    bool is_open() const;

    void close();

protected:

    read_interface_t & read_interface()
    { return *this; }

    write_interface_t & write_interface()
    { return *this; }

    // Underlying socket
    boost::asio::ip::tcp::socket & get_socket()
    { return *_sock; }

    const boost::asio::ip::tcp::socket & get_socket() const
    { return *_sock; }

private:

    friend class stream_protocol_read_interface;
    friend class stream_protocol_write_interface;

    std::unique_ptr<boost::asio::ip::tcp::socket> _sock;
    io_service_ptr_t _io_srv;
};

}
}

#include "samoa/core/stream_protocol.impl.hpp"

#endif

