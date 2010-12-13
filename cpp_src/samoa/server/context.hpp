#ifndef SAMOA_SERVER_CONTEXT_HPP
#define SAMOA_SERVER_CONTEXT_HPP

#include "samoa/core/proactor.hpp"
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

    context(core::proactor::ptr_t p)
     : _proactor(p)
    { }

    const core::proactor::ptr_t & get_proactor() const
    { return _proactor; }

private:

    core::proactor::ptr_t _proactor;
};

}
}

#endif

