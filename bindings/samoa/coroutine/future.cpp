#include "pysamoa/boost_python.hpp"
#include "pysamoa/future.hpp"

namespace samoa {
namespace coroutine {

namespace bpl = boost::python;

void make_future_bindings()
{
    bpl::class_<pysamoa::future, pysamoa::future::ptr_t,
        boost::noncopyable>("Future", bpl::init<>())
        .def(bpl::init<const bpl::object &>())
        .def("is_called", &pysamoa::future::is_called)
        ;
}

}
}

