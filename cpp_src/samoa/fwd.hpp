#ifndef SAMOA_FWD_HPP
#define SAMOA_FWD_HPP

#include <boost/shared_ptr.hpp>

namespace samoa {

class server;
class client;

// Workaround for inability to forward declare a class-nested typdef.
//  If definition is available, perfer class_t::ptr_t
typedef boost::shared_ptr<server> server_ptr_t;
typedef boost::shared_ptr<client> client_ptr_t;

};

#endif // guard
