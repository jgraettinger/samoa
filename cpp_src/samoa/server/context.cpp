
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/core/tasklet_group.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {

context::context(const spb::ClusterState & state)
 : _uuid(core::uuid_from_hex(state.local_uuid())),
   _hostname(state.local_hostname()),
   _port(state.local_port()),
   _proactor(core::proactor::get_proactor()),
   _io_srv(_proactor->serial_io_service()),
   _tasklet_group(boost::make_shared<core::tasklet_group>())
{
    SAMOA_ASSERT(state.IsInitialized());

    std::unique_ptr<spb::ClusterState> state_copy(
        new spb::ClusterState(state));

    _cluster_state = boost::make_shared<cluster_state>(
        std::move(state_copy), cluster_state::ptr_t());

    LOG_DBG("context created");
}

context::~context()
{
    LOG_DBG("context destroyed");
}

cluster_state_ptr_t context::get_cluster_state() const
{
    spinlock::guard guard(_cluster_state_lock);

    return _cluster_state;
}

void context::spawn_tasklets()
{
    _cluster_state->spawn_tasklets(shared_from_this());
}

void context::cluster_state_transaction(
    const cluster_state_callback_t & callback)
{
    _io_srv->post(boost::bind(&context::on_cluster_state_transaction,
        shared_from_this(), callback));
}

void context::on_cluster_state_transaction(
    const cluster_state_callback_t & callback)
{
    // copy current protobuf description
    std::unique_ptr<spb::ClusterState> next_pb_state(
        new spb::ClusterState(_cluster_state->get_protobuf_description()));

    bool commit = callback(*next_pb_state);

    if(!commit)
        return;

    SAMOA_ASSERT(next_pb_state->IsInitialized());

    // build runtime instance; cluster_state (and descendant) ctors are
    //   responsible for checking invariants of the new description
    cluster_state::ptr_t next_state = boost::make_shared<cluster_state>(
        std::move(next_pb_state), _cluster_state);

    next_state->spawn_tasklets(shared_from_this());

    // TODO(johng) commit next_cluster_state to disk

    // 'commit' transaction by atomically updating held instance
    {
        spinlock::guard guard(_cluster_state_lock);
        _cluster_state = next_state;
    }
}

}
}

