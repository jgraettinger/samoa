#ifndef SAMOA_PARTITION_ROUTER_HPP
#define SAMOA_PARTITION_ROUTER_HPP

#include "samoa/fwd.hpp"
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {

class partition_router {
public:
    
    typedef boost::shared_ptr<partition_router> ptr_t;

    partition_router(boost::asio::io_service &);
    
    void route_request(const client_protocol_ptr_t &)
    { }
};

};

#endif

