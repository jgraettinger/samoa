
#include <boost/python.hpp>
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/future.hpp"
#include <boost/system/error_code.hpp>
#include <boost/bind/protect.hpp>
#include <boost/bind.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace core {

namespace bpl = boost::python;
using namespace pysamoa;

boost::regex py_compile_regex(const bpl::str & re)
{
    char * buf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(re.ptr(), &buf, &length) == -1)
        bpl::throw_error_already_set();

    return boost::regex(buf, buf + length);
}

future::ptr_t py_read_regex(stream_protocol::read_interface_t & p,
    const boost::regex & re, size_t max_read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_regex(re, max_read_length,
        boost::bind(&future::on_regex_match_result, f, _1, _2));
    return f;
}
    
future::ptr_t py_read_line(stream_protocol::read_interface_t & p,
    char delim_char, size_t max_read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_line(boost::bind(&future::on_buffer_result, f, _1, _2, _3),
        delim_char, max_read_length);
    return f;
}

future::ptr_t py_read_data(stream_protocol::read_interface_t & p,
    size_t read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_data(read_length,
        boost::bind(&future::on_data_result, f, _1, _2, _3));
    return f;
}

void py_queue_write(stream_protocol::write_interface_t & p,
    const bpl::str & buf)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(buf.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    p.queue_write(cbuf, cbuf + length);
}

future::ptr_t py_write_queued(stream_protocol::write_interface_t & p)
{
    future::ptr_t f(boost::make_shared<future>());

    p.write_queued(boost::bind(&future::on_length_result, f, _1, _2));
    return f;
}

void make_stream_protocol_bindings()
{
    bpl::class_<boost::regex>("_boost_regex", bpl::no_init);

    bpl::class_<stream_protocol::read_interface_t, boost::noncopyable>(
        "_StreamProtocol_ReadInterface", bpl::no_init)
        .def("read_regex", &py_read_regex)
        .def("read_data", &py_read_data)
        .def("read_line", &py_read_line, (
            bpl::arg("delim_char") = '\n',
            bpl::arg("max_read_length") = 1024));

    bpl::class_<stream_protocol::write_interface_t, boost::noncopyable>(
        "_StreamProtocol_WriteInterface", bpl::no_init)
        .def("has_queued_writes",
            &stream_protocol::write_interface_t::has_queued_writes)
        .def("queue_write", &py_queue_write)
        .def("write_queued", &py_write_queued);

    bpl::class_<stream_protocol, boost::noncopyable>(
		"StreamProtocol", bpl::no_init)
        .def("compile_regex", &py_compile_regex)
        .def("is_open", &stream_protocol::is_open)
        .def("close", &stream_protocol::close)
        .add_property("local_address", &stream_protocol::get_local_address)
        .add_property("remote_address", &stream_protocol::get_remote_address)
        .add_property("local_port", &stream_protocol::get_local_port)
        .add_property("remote_port", &stream_protocol::get_remote_port);
}

}
}

