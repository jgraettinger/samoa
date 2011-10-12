#ifndef SAMOA_REQUEST_CONTEXT_STATE_HPP
#define SAMOA_REQUEST_CONTEXT_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/cluster_state.hpp"

namespace samoa {
namespace request {

/*!
 * Provides the consistent server context under which a request
 *  is processed. More specifically, provides the immutable peer_set,
 *  table_set, and cluster_state instances which should be referenced
 *  throughout the request's lifetime.
 */
class context_state
{
public:

    virtual ~context_state();

    const server::context_ptr_t & get_context() const
    { return _context; }

    const server::cluster_state_ptr_t & get_cluster_state() const
    { return _cluster_state; }

    const server::peer_set_ptr_t & get_peer_set() const
    { return _cluster_state->get_peer_set(); }

    const server::table_set_ptr_t & get_table_set() const
    { return _cluster_state->get_table_set(); }

    void load_context_state(const server::context_ptr_t & context);

    void reset_context_state();

private:

    server::context_ptr_t _context;
    server::cluster_state_ptr_t _cluster_state;
};

}
}

#endif

