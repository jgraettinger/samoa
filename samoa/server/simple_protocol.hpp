#ifndef SERVER_SIMPLE_PROTOCOL_HPP
#define SERVER_SIMPLE_PROTOCOL_HPP

#include "samoa/server/protocol.hpp"
#include "samoa/common/stream_protocol.hpp"

namespace server {

class simple_protocol :
    public protocol,
    public boost::enable_shared_from_this<simple_protocol>
{
public:

    typedef boost::shared_ptr<simple_protocol> ptr_t;

    simple_protocol() {}

    ~simple_protocol() {}

    virtual void start(const client_ptr_t &);

    virtual void next_request(const client_ptr_t &);

protected:

    // called after completion of startup (writing protocol version to client)
    void on_start(const boost::system::error_code &, const client_ptr_t &);

    // called after a command-line has been received (or error)
    void on_command(const client_ptr_t &,
        const boost::system::error_code &,
        const common::buffers_iterator_t & begin,
        const common::buffers_iterator_t & end);

    // called after completion of writing to client
    void on_write(const client_ptr_t &,
        const boost::system::error_code &,
        bool close);
};

}

#endif

