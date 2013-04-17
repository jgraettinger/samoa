#ifndef SAMOA_REQUEST_FWD_HPP
#define SAMOA_REQUEST_FWD_HPP

#include <memory>

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
typedef std::shared_ptr<state> state_ptr_t;

}
}

#endif

