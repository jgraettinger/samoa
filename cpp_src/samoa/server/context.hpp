#ifndef SAMOA_SERVER_CONTEXT_HPP
#define SAMOA_SERVER_CONTEXT_HPP

#include "samoa/core/proactor.hpp"
#include "samoa/client/fwd.hpp"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {

class context :
    private boost::noncopyable,
    public boost::enable_shared_from_this<context>
{
public:

    typedef boost::shared_ptr<context> ptr_t;

    context(core::proactor::ptr_t);

    const core::proactor::ptr_t & get_proactor() const
    { return _proactor; }

    const samoa::client::server_pool_ptr_t & get_peer_pool() const
    { return _peer_pool; }

private:

    core::proactor::ptr_t _proactor;
    samoa::client::server_pool_ptr_t _peer_pool;
};

}
}

#endif

