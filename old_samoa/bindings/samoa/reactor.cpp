
#include "common/reactor.hpp"
#include <boost/python.hpp>

using namespace common;
using namespace boost::python;

void python_call_later_handler(const object & c)
{ c(); }

void python_call_later(reactor & r, object & c, size_t milli_sec)
{
    r.call_later(
        boost::bind(
            &python_call_later_handler,
            c
        ), milli_sec
    );
}

void make_reactor_bindings()
{
    class_<reactor, reactor::ptr_t, boost::noncopyable>("reactor")
        .def("run", &reactor::run)
        .def("call_later", &python_call_later);
}

