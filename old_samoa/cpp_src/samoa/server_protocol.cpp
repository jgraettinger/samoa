
#include "samoa/server_protocol.hpp"
#include <boost/bind.hpp>

server_protocol::ptr_t server_protocol::new_server_protocol(
    const std::string & host, const std::string & port,
    common::reactor::ptr_t & reactor)
{
    tcp::resolver resolver(reactor->io_service());
    
    // Blocks, & throws on resolution failure
    tcp::endpoint ep = *resolver.resolve(
        tcp::resolver::query(host, port));
    
    

}

