#ifndef SAMOA_SERVER_LISTENER_HPP
#define SAMOA_SERVER_LISTENER_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/core/tasklet.hpp"
#include "samoa/server/fwd.hpp"
#include <boost/asio.hpp>
#include <string>

namespace samoa {
namespace server {

class listener : public core::tasklet<listener>
{
public:

    using core::tasklet<listener>::ptr_t;

    listener(const context_ptr_t &, const protocol_ptr_t &);

    ~listener();

    std::string get_address();
    unsigned short get_port();

    const context_ptr_t & get_context() const
    { return _context; }

    const protocol_ptr_t & get_protocol() const
    { return _protocol; }

    void run_tasklet();
    void halt_tasklet();

private:

    void on_accept(const boost::system::error_code & ec);

    const context_ptr_t  _context;
    const protocol_ptr_t _protocol;

    boost::asio::ip::tcp::acceptor _accept_sock;

    // Next connection to accept, and it's io_service
    std::unique_ptr<boost::asio::ip::tcp::socket> _next_sock;
    core::io_service_ptr_t _next_io_srv;
};

}
}

#endif

