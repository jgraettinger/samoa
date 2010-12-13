
#include <boost/python.hpp>
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/runthread.hpp"
#include "samoa/core/coros.hpp"
#include <boost/system/error_code.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>
#include <iostream>

namespace samoa {
namespace core {

namespace bpl = boost::python;

void py_on_read_until(const bpl::object & gen,
    const boost::system::error_code & ec,
    const buffers_iterator_t & begin,
    const buffers_iterator_t & end)
{
    scoped_python block;

    generator_scope gen_scope(gen);

    // check ec
    if(ec) {
        gen_scope.send_exception(ec.message());
    }
    else {

        // allocate a python copy of the buffer
        bpl::str buf(0, std::distance(begin, end));
        std::copy(begin, end, PyString_AS_STRING(buf.ptr()));

        gen_scope.send_value(buf);
    }
}

void py_read_until(stream_protocol & p,
    char delim_char, size_t max_read_length)
{
    if(_current_generator.ptr() == Py_None)
        throw std::runtime_error("read_until() must be called from a generator context");

    p.read_until(delim_char, max_read_length,
        boost::bind(&py_on_read_until, _current_generator, _1, _2, _3));
}


void make_stream_protocol_bindings()
{
    bpl::class_<stream_protocol, boost::noncopyable>("StreamProtocol", bpl::no_init)
        .def("read_until", &py_read_until);
//        .def("queue_write", &py_queue_write)
//        .def("write_queued", &py_write_queued);
}

}
}

