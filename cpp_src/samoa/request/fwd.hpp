#ifndef SAMOA_REQUEST_FWD_HPP
#define SAMOA_REQUEST_FWD_HPP

#include <boost/shared_ptr.hpp>

namespace samoa {
namespace request {

class io_service_state;
class context_state;
class client_state;
class table_state;
class route_state;
class record_state;
class replication_state;

class state;
typedef boost::shared_ptr<state> state_ptr_t;

}
}

#endif

