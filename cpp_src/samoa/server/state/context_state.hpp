#ifndef SAMOA_SERVER_STATE_CONTEXT_STATE_HPP
#define SAMOA_SERVER_STATE_CONTEXT_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/cluster_state.hpp"

namespace samoa {
namespace server {
namespace state {

/*!
 * Provides the consistent server context under which an operation
 *  is processed. More specifically, provides the immutable peer_set,
 *  table_set, and cluster_state instances which should be referenced
 *  throughout the operation's lifetime.
 */
class context_state
{
public:

    const context_ptr_t & get_context() const
    { return _context; }

    const cluster_state_ptr_t & get_cluster_state() const
    { return _cluster_state; }

    const peer_set_ptr_t & get_peer_set() const
    { return _cluster_state->get_peer_set(); }

    const table_set_ptr_t & get_table_set() const
    { return _cluster_state->get_table_set(); }

    void load_context_state(const context_ptr_t & context);

    void reset_context_state();

private:

    context_ptr_t _context;
    cluster_state_ptr_t _cluster_state;
};

}
}
}

#endif

