#ifndef SAMOA_CORE_STREAM_PROTOCOL_HPP
#define SAMOA_CORE_STREAM_PROTOCOL_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/core/ref_buffer.hpp"
#include "samoa/core/buffer_region.hpp"
#include "samoa/core/buffer_ring.hpp"
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <functional>

namespace samoa {
namespace core {

class stream_protocol_read_interface
    : private boost::noncopyable
{
public:

    typedef boost::shared_ptr<stream_protocol_read_interface> ptr_t;
    typedef boost::weak_ptr<stream_protocol_read_interface> weak_ptr_t;

    stream_protocol_read_interface()
     :  _in_read(false)
    { }

    virtual ~stream_protocol_read_interface()
    { }

    typedef std::function<
        void(ptr_t, boost::system::error_code, buffer_regions_t)
    > read_callback_t;

    void read(read_callback_t, weak_ptr_t self, size_t length);

private:

    static void on_read(
        weak_ptr_t self,
        read_callback_t,
        boost::system::error_code,
        size_t read_length,
        size_t bytes_transferred);

    bool _in_read;
    buffer_ring _ring;
};

class stream_protocol_write_interface
    : private boost::noncopyable
{
public:

    typedef boost::shared_ptr<stream_protocol_write_interface> ptr_t;
    typedef boost::weak_ptr<stream_protocol_write_interface> weak_ptr_t;

    stream_protocol_write_interface()
     :  _in_write(false)
    { }

    virtual ~stream_protocol_write_interface()
    { }

    bool has_queued_writes() const;

    // queue_write(*) - schedule buffer for later write using gather-IO
    //
    // It is an error to call queue_write() while a
    //  write operation is in progress.
    void queue_write(const buffer_regions_t &);

    template<typename Iterator>
    void queue_write(const Iterator & begin, const Iterator & end);

    void queue_write(const std::string & str)
    { queue_write(std::begin(str), std::end(str)); }

    typedef std::function<
        void(ptr_t, boost::system::error_code)
    > write_callback_t;

    // Initiates an asynchronous write of queued buffer
    //  regions to the socket. Callback is invoked when
    //  the entire write has completed, or an error occurs
    void write(write_callback_t, weak_ptr_t self);

private:

    bool _in_write;
    buffer_ring _ring;
    const_buffer_regions_t _regions;
};

class stream_protocol :
    public stream_protocol_read_interface,
    public stream_protocol_write_interface
{
public:

    typedef stream_protocol_read_interface read_interface_t;
    typedef stream_protocol_write_interface write_interface_t;

    stream_protocol(std::unique_ptr<boost::asio::ip::tcp::socket> sock,
        io_service_ptr_t io_srv);

    virtual ~stream_protocol();

    std::string get_local_address() const;
    std::string get_remote_address() const;

    unsigned get_local_port() const;
    unsigned get_remote_port() const;

    bool is_open() const;

    const io_service_ptr_t & get_io_service()
    { return _io_srv; }

protected:

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

