
#include <boost/python.hpp>
#include "samoa/server/peer_discovery.hpp"
#include "samoa/server/context.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;
using namespace pysamoa;

void py_on_callback(
    const future::ptr_t & future,
    const boost::system::error_code & ec)
{
    python_scoped_lock block;

    if(ec)
    {
    	future->on_error(ec);
    	return;
    }

    future->on_result(bpl::object());
}

future::ptr_t py_call(peer_discovery & p)
{
    future::ptr_t f(boost::make_shared<future>());
    f->set_reenter_via_post();

    p(boost::bind(&py_on_callback, f, _1));
    return f;
}

void make_peer_discovery_bindings()
{
    bpl::class_<peer_discovery, peer_discovery::ptr_t,
        boost::noncopyable>("PeerDiscovery",
            bpl::init<const context::ptr_t, const core::uuid &>())
        .def("__call__", &py_call)
        ;
}

}
}

