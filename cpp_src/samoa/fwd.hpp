#ifndef SAMOA_FWD_HPP
#define SAMOA_FWD_HPP

#include <boost/shared_ptr.hpp>
#include "common/rolling_hash.hpp"

#include <iostream>

namespace samoa {

class server;
typedef boost::shared_ptr<server> server_ptr_t;
class client_protocol;
typedef boost::shared_ptr<client_protocol> client_protocol_ptr_t;
struct request;
typedef boost::shared_ptr<request> request_ptr_t;

class partition;
typedef boost::shared_ptr<partition> partition_ptr_t;
class partition_router;
typedef boost::shared_ptr<partition_router> partition_router_ptr_t;

// standard samoa record-table base
typedef common::rolling_hash<
    4, // offset bytes
    1, // key-length bytes
    3, // val-length bytes
    4  // expiry bytes
> rolling_hash_t;
typedef boost::shared_ptr<rolling_hash_t> rolling_hash_ptr_t;

};

#endif // guard
