
#include "samoa/server/context.hpp"
#include "samoa/server/cluster_state.hpp"
#include "samoa/server/listener.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/uuid.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

context::context(const spb::ClusterState & state)
 : _uuid(core::parse_uuid(state.local_uuid())),
   _hostname(state.local_hostname()),
   _port(state.local_port()),
   _proactor(core::proactor::get_proactor()),
   _cluster_transaction_srv(_proactor->serial_io_service())
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
    {
        spinlock::guard guard(_listener_lock);
        for(const listeners_t::value_type & entry : _listeners)
        {
            listener::ptr_t listener(entry.second.lock());

            if(listener)
            {
                listener->shutdown();
            }
        }
    }
    {
        spinlock::guard guard(_client_lock);
        _clients.clear();
    }
}

void context::cluster_state_transaction(
    const cluster_state_callback_t & callback)
{
    context::ptr_t self = shared_from_this();
    auto transaction = [self, callback]()
    {
        // copy current protobuf description
        std::unique_ptr<spb::ClusterState> next_pb_state(
            new spb::ClusterState(
                self->get_cluster_state()->get_protobuf_description()));

        bool commit = callback(*next_pb_state);

        if(!commit)
            return;

        SAMOA_ASSERT(next_pb_state->IsInitialized());

        // build runtime instance; cluster_state (and descendant) ctors are
        //   responsible for checking invariants of the new description
        cluster_state::ptr_t next_state = boost::make_shared<cluster_state>(
            std::move(next_pb_state), self->get_cluster_state());

        // TODO(johng) commit next_cluster_state to disk

        // 'commit' transaction by atomically updating held instance
        {
            spinlock::guard guard(self->_cluster_state_lock);
            self->_cluster_state = next_state;
        }

        self->initialize();
    };
    _cluster_transaction_srv->dispatch(transaction);
}

void context::add_client(size_t client_id, const client::ptr_t & client)
{
    spinlock::guard guard(_client_lock);

    LOG_INFO("add client " << client_id);

    SAMOA_ASSERT(_clients.insert(
        std::make_pair(client_id, client)).second);
}

void context::drop_client(size_t client_id)
{
    spinlock::guard guard(_client_lock);

    LOG_INFO("drop client " << client_id);

    clients_t::iterator it = _clients.find(client_id);
    SAMOA_ASSERT(it != _clients.end());
    _clients.erase(it);
}

void context::add_listener(size_t listener_id,
    const listener::ptr_t & listener)
{
    spinlock::guard guard(_listener_lock);

    SAMOA_ASSERT(_listeners.insert(
        std::make_pair(listener_id, listener)).second);
}

void context::drop_listener(size_t listener_id)
{
    spinlock::guard guard(_listener_lock);

    listeners_t::iterator it = _listeners.find(listener_id);
    SAMOA_ASSERT(it != _listeners.end());
    _listeners.erase(it);
}

}
}

