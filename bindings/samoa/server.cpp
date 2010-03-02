
#include "samoa/server.hpp"
#include "samoa/partition.hpp"
#include "common/reactor.hpp"
#include <boost/python.hpp>
#include <string>

using namespace samoa;
using namespace boost::python;

void make_server_bindings()
{
    class_<server, server::ptr_t, boost::noncopyable>("server", init<
        const std::string &,
        const std::string &,
        size_t,
        const common::reactor::ptr_t &>())
    .def("shutdown", &server::shutdown)
    .def_readwrite("partition", &server::_partition);
}

