#ifndef SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP
#define SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include <memory>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class replicate_handler :
    public command_handler,
    public std::enable_shared_from_this<replicate_handler>
{
public:

    typedef std::shared_ptr<replicate_handler> ptr_t;

    replicate_handler()
    { }

    void handle(const request::state_ptr_t &);
};

}
}
}

#endif

