#include "samoa/request/context_state.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace request {

context_state::~context_state()
{ }

void context_state::load_context_state(const server::context::ptr_t & context)
{
    SAMOA_ASSERT(!_context);
    _context = context;
    _cluster_state = _context->get_cluster_state();
}

void context_state::reset_context_state()
{
    _context.reset();
    _cluster_state.reset();
}

}
}

