#ifndef SAMOA_SERVER_EVENTUAL_CONSISTENCY_HPP
#define SAMOA_SERVER_EVENTUAL_CONSISTENCY_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/system/error_code.hpp>

namespace samoa {
namespace server {

class eventual_consistency :
    public boost::enable_shared_from_this<eventual_consistency>
{
public:

    typedef boost::shared_ptr<eventual_consistency> ptr_t;

    eventual_consistency(
        const context_ptr_t &,
        const core::uuid & table_uuid,
        const core::uuid & partition_uuid,
        const datamodel::prune_func_t &);

    bool operator()(const request::state_ptr_t & rstate);

private:

    void on_replication();

    void on_move_request(
        const boost::system::error_code &,
        samoa::client::server_request_interface,
        const request::state_ptr_t &);

    void on_move_response(
        const boost::system::error_code &,
        samoa::client::server_response_interface,
        const request::state_ptr_t &);

    bool on_move_drop(bool, const request::state_ptr_t &);

    context_weak_ptr_t _weak_context;
    const core::uuid _table_uuid;
    const core::uuid _partition_uuid;
    const datamodel::prune_func_t _prune_func;
};

}
}

#endif

