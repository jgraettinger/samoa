#ifndef SAMOA_CLIENT_FWD_HPP
#define SAMOA_CLIENT_FWD_HPP

#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace client {

class server;
typedef boost::shared_ptr<server> server_ptr_t;

// when possible use server::request_interface & server::response_interface
class server_request_interface;
class server_response_interface;

class server_pool;
typedef boost::shared_ptr<server_pool> server_pool_ptr_t;

};
};

#endif

