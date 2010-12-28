
#include <boost/python.hpp>
#include "samoa/core/stream_protocol.hpp"
#include "samoa/core/scoped_python.hpp"
#include "samoa/core/future.hpp"
#include <boost/system/error_code.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>
#include <iostream>

namespace samoa {
namespace core {

namespace bpl = boost::python;

boost::regex py_compile_regex(const bpl::str & re)
{
    char * buf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(re.ptr(), &buf, &length) == -1)
        bpl::throw_error_already_set();

    return boost::regex(buf, buf + length);
}

future::ptr_t py_read_regex(stream_protocol & p,
    const boost::regex & re, size_t max_read_length)
{
    future::ptr_t f(new future());

    p.read_regex(re, max_read_length,
        boost::bind(&future::on_regex_match_result, f, _1, _2));
    return f;
}
    
future::ptr_t py_read_until(stream_protocol & p,
    char delim_char, size_t max_read_length)
{
    future::ptr_t f(new future());

    p.read_until(delim_char, max_read_length,
        boost::bind(&future::on_buffer_result, f, _1, _2, _3));
    return f;
}

future::ptr_t py_read_data(stream_protocol & p, size_t read_length)
{
    future::ptr_t f(new future());

    p.read_data(read_length,
        boost::bind(&future::on_data_result, f, _1, _2, _3));
    return f;
}

void py_queue_write(stream_protocol & p, const bpl::str & buf)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(buf.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    p.queue_write(cbuf, cbuf + length);
}

future::ptr_t py_write_queued(stream_protocol & p)
{
    future::ptr_t f(new future());

    p.write_queued(boost::bind(&future::on_length_result, f, _1, _2));
    return f;
}


void make_stream_protocol_bindings()
{
    bpl::class_<boost::regex>("_boost_regex", bpl::no_init);

    bpl::class_<stream_protocol, boost::noncopyable>(
		"StreamProtocol", bpl::no_init)
        .def("compile_regex", &py_compile_regex)
        .staticmethod("compile_regex")
        .def("read_regex", &py_read_regex)
        .def("read_data", &py_read_data)
        .def("read_until", &py_read_until)
        .def("queue_write", &py_queue_write)
        .def("write_queued", &py_write_queued);
}

}
}

