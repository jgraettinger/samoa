#ifndef SAMOA_SERVER_CONTEXT_HPP
#define SAMOA_SERVER_CONTEXT_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/server/fwd.hpp"

namespace samoa {
namespace server {

class context :
    public boost::enable_shared_from_this<context>
{
public:

    typedef context_ptr_t ptr_t;

    context(core::proactor_ptr_t);

    const core::proactor_ptr_t & get_proactor() const
    { return _proactor; }

private:

    core::proactor_ptr_t _proactor;
};

}
}

#endif

