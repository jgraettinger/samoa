#ifndef SAMOA_SERVER_CLIENT_HPP
#define SAMOA_SERVER_CLIENT_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <memory>

namespace samoa {
namespace server {

class client :
    public boost::enable_shared_from_this<client>,
    public core::stream_protocol
{
public:

    typedef boost::shared_ptr<client> ptr_t;

    client(context_ptr_t, protocol_ptr_t,
        std::auto_ptr<boost::asio::ip::tcp::socket>);

    const context_ptr_t & get_context() const
    { return _context; }

    const protocol_ptr_t & get_protocol() const
    { return _protocol; }

    void start_next_request();

private:

    context_ptr_t _context;
    protocol_ptr_t _protocol;
};

}
}

#endif

