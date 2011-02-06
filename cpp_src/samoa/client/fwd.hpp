#ifndef SAMOA_CLIENT_FWD_HPP
#define SAMOA_CLIENT_FWD_HPP

#include <boost/shared_ptr.hpp>

namespace samoa {
namespace client {

class server;
typedef boost::shared_ptr<server> server_ptr_t;

// when possible use server::request_interface & server::response_interface
class server_request_interface;
class server_response_interface;

};
};

#endif

