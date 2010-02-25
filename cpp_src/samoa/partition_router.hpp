#ifndef SAMOA_PARTITION_ROUTER_HPP
#define SAMOA_PARTITION_ROUTER_HPP

#include "samoa/fwd.hpp"
#include <boost/asio.hpp>

namespace samoa {

class partition_router {
public:
    
    partition_router(io_service &);
    
    void route_request(const client_ptr_t &);
    
};

};

#endif

