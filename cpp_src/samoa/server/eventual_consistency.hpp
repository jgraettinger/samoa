#ifndef SAMOA_SERVER_EVENTUAL_CONSISTENCY_HPP
#define SAMOA_SERVER_EVENTUAL_CONSISTENCY_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

class eventual_consistency
{
public:

    eventual_consistency(
        const context_ptr_t & context,
        const core::uuid & table_uuid,
        const core::uuid & partition_uuid,
        const datamodel::prune_func_t & prune_func);

    bool operator()(const request::state_ptr_t & rstate);

private:

    void on_move_request(
        const boost::system::error_code & ec,
        samoa::client::server_request_interface iface,
        const request::state::ptr_t & rstate);

    context_weak_ptr_t & _weak_context;
    core::uuid & _table_uuid;
    core::uuid & _partition_uuid;
    datamodel::prune_func_t _prune_func;
};

}
}

#endif

