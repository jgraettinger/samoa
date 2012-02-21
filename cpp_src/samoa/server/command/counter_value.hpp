#ifndef SAMOA_SERVER_COMMAND_COUNTER_VALUE_HPP
#define SAMOA_SERVER_COMMAND_COUNTER_VALUE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

class counter_value_handler :
    public command_handler,
    public boost::enable_shared_from_this<counter_value_handler>
{
public:

    typedef boost::shared_ptr<counter_value_handler> ptr_t;

    counter_value_handler()
    { }

    void handle(const request::state_ptr_t &);
};

}
}
}

#endif

