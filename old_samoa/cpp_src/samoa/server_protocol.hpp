#ifndef SAMOA_SERVER_PROTOCOL_HPP
#define SAMOA_SERVER_PROTOCOL_HPP

#include "samoa/fwd.hpp"
#include "common/reactor.hpp"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <string>

namespace samoa {

class server_protocol :
    public boost::enable_shared_from_this<server_protocol>,
    public stream_protocol
{
public:
    
    typedef boost::shared_ptr<server_protocol> ptr_t;
    
    static ptr_t new_server_protocol(
        const std::string & host,
        const std::string & port,
        common::reactor::ptr_t & reactor
    );
    
    std::string _host;
    std::string _port;
    tcp::socket _socket;
};

#endif // guard
