#ifndef SAMOA_CLIENT_FWD_HPP
#define SAMOA_CLIENT_FWD_HPP

#include <memory>

namespace samoa {
namespace client {

class server;
typedef std::shared_ptr<server> server_ptr_t;

// When possible use server::request_interface & server::response_interface.
class server_request_interface;
class server_response_interface;

class server_pool;
typedef std::shared_ptr<server_pool> server_pool_ptr_t;

};
};

#endif

