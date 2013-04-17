#ifndef SAMOA_SERVER_COMMAND_COUNTER_VALUE_HPP
#define SAMOA_SERVER_COMMAND_COUNTER_VALUE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include <memory>

namespace samoa {
namespace server {
namespace command {

class counter_value_handler :
    public command_handler,
    public std::enable_shared_from_this<counter_value_handler>
{
public:

    typedef std::shared_ptr<counter_value_handler> ptr_t;

    counter_value_handler()
    { }

    void handle(const request::state_ptr_t &);
};

}
}
}

#endif

