#ifndef SAMOA_SERVER_COMMAND_SET_BLOB_HPP
#define SAMOA_SERVER_COMMAND_SET_BLOB_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/server/table.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <memory>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class set_blob_handler :
    public command_handler,
    public std::enable_shared_from_this<set_blob_handler>
{
public:

    typedef std::shared_ptr<set_blob_handler> ptr_t;

    set_blob_handler()
    { }

    void handle(const request::state_ptr_t &);
};

}
}
}

#endif

