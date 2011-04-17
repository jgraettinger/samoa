#include <boost/python.hpp>
#include "pysamoa/future.hpp"

namespace samoa {
namespace coroutine {

namespace bpl = boost::python;

void make_future_bindings()
{
    void (pysamoa::future::*on_error_ptr)(
        const bpl::object &, const bpl::object &) = &pysamoa::future::on_error;

    bpl::class_<pysamoa::future, pysamoa::future::ptr_t,
        boost::noncopyable>("Future", bpl::init<>())
        .def(bpl::init<const bpl::object &>())
        .def("on_error", on_error_ptr)
        .def("on_result", &pysamoa::future::on_result);
}

}
}

