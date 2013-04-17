#ifndef SAMOA_SERVER_COMMAND_CLUSTER_STATE_HPP
#define SAMOA_SERVER_COMMAND_CLUSTER_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include <memory>

namespace samoa {
namespace server {
namespace command {

class cluster_state_handler :
    public command_handler,
    public std::enable_shared_from_this<cluster_state_handler>
{
public:

    typedef std::shared_ptr<cluster_state_handler> ptr_t;

    cluster_state_handler()
    { }

    void handle(const request::state_ptr_t &);

private:

    bool on_state_transaction(core::protobuf::ClusterState &,
        const request::state_ptr_t &);
    
    void on_complete(const request::state_ptr_t &);
};

}
}
}

#endif

