
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/listener.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/bind.hpp>

namespace samoa {
namespace server {

context::context(const spb::ClusterState & state)
 : _uuid(core::parse_uuid(state.local_uuid())),
   _hostname(state.local_hostname()),
   _port(state.local_port()),
   _proactor(core::proactor::get_proactor()),
   _io_srv(_proactor->serial_io_service())
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

void context::initialize()
{
    _cluster_state->initialize(shared_from_this());
}

void context::shutdown()
{
    for(const listeners_t::value_type & entry : _listeners)
    {
    	entry.second->shutdown();
    }
    _listeners.clear();

    for(const clients_t::value_type & entry : _clients)
    {
        client::ptr_t client(entry.second.lock());

        if(client)
        {
        	client->shutdown();
        }
    }
}

void context::cluster_state_transaction(
    const cluster_state_callback_t & callback)
{
    _io_srv->post(boost::bind(&context::on_cluster_state_transaction,
        shared_from_this(), callback));
}

void context::add_client(size_t client_id, const client::ptr_t & client)
{
    SAMOA_ASSERT(_clients.insert(
        std::make_pair(client_id, client)).second);
}

void context::drop_client(size_t client_id)
{
    clients_t::iterator it = _clients.find(client_id);
    SAMOA_ASSERT(it != _clients.end());
    _clients.erase(it);
}

void context::add_listener(size_t listener_id,
    const listener::ptr_t & listener)
{
    SAMOA_ASSERT(_listeners.insert(
        std::make_pair(listener_id, listener)).second);
}

void context::drop_listener(size_t listener_id)
{
    listeners_t::iterator it = _listeners.find(listener_id);
    SAMOA_ASSERT(it != _listeners.end());
    _listeners.erase(it);
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

    next_state->initialize(shared_from_this());

    // TODO(johng) commit next_cluster_state to disk

    // 'commit' transaction by atomically updating held instance
    {
        spinlock::guard guard(_cluster_state_lock);
        _cluster_state = next_state;
    }
}

}
}

