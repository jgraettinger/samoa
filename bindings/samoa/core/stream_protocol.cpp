
#include "samoa/core/stream_protocol.hpp"
#include "pysamoa/future.hpp"
#include "pysamoa/scoped_python.hpp"
#include <boost/python.hpp>
#include <boost/system/error_code.hpp>
#include <boost/bind.hpp>

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

/////////// read_regex support

void py_on_read_regex(
    const pysamoa::future::ptr_t & future,
    const boost::system::error_code & ec,
    const stream_protocol::match_results_t & match)
{
    python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    PyObject * tuple = PyTuple_New(match.size());

    for(unsigned i = 0; i != match.size(); ++i)
    {
        // allocate uninitialized string of appropriate size
        //   (adds a reference to str)
        PyObject * str;
        if(!(str = PyString_FromStringAndSize(0, match[i].length())))
            bpl::throw_error_already_set();

        // copy in regex match body
        std::copy(match[i].first, match[i].second,
            PyString_AS_STRING(str));

        // add str to tuple (steals reference)
        PyTuple_SET_ITEM(tuple, i, str);
    }

    future->on_result(bpl::object(bpl::handle<>(tuple)));
}

future::ptr_t py_read_regex(
    stream_protocol::read_interface_t & p,
    const boost::regex & re,
    size_t max_read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_regex(boost::bind(py_on_read_regex, f, _1, _2),
        re, max_read_length);
    return f;
}

/////////// read_line support

void py_on_read_line(
    const pysamoa::future::ptr_t & future,
    const boost::system::error_code & ec,
    const buffers_iterator_t & begin,
    const buffers_iterator_t & end)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    // allocate a python copy of the buffer
    bpl::object result = bpl::str(0, std::distance(begin, end));
    std::copy(begin, end, PyString_AS_STRING(result.ptr()));

    future->on_result(result);
}

future::ptr_t py_read_line(
    stream_protocol::read_interface_t & p,
    char delim_char,
    size_t max_read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_line(boost::bind(py_on_read_line, f, _1, _2, _3),
        delim_char, max_read_length);
    return f;
}

/////////// read_data support

void py_on_read_data(
    const pysamoa::future::ptr_t & future,
    const boost::system::error_code & ec,
    size_t length,
    const buffer_regions_t & regions)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    // allocate uninitialized buffer of appropriate size
    //   (adds a reference to str)
    PyObject * buf;
    if(!(buf = PyString_FromStringAndSize(0, length)))
        bpl::throw_error_already_set();

    char * buf_ptr = PyString_AS_STRING(buf);

    for(unsigned i = 0; i != regions.size(); ++i)
    {
        std::copy(regions[i].begin(), regions[i].end(), buf_ptr);
        buf_ptr += regions[i].size();
    }

    future->on_result(bpl::object(bpl::handle<>(buf)));
}

future::ptr_t py_read_data(
    stream_protocol::read_interface_t & p,
    size_t read_length)
{
    future::ptr_t f(boost::make_shared<future>());

    p.read_data(boost::bind(py_on_read_data, f, _1, _2, _3),
        read_length);
    return f;
}

/////////// write_queued support

void py_on_write_queued(
    const pysamoa::future::ptr_t & future,
    const boost::system::error_code & ec,
    size_t length)
{
    pysamoa::python_scoped_lock block;

    if(ec)
    {
        future->on_error(ec);
        return;
    }

    future->on_result(bpl::object(bpl::handle<>(
        PyInt_FromSize_t(length))));
}

future::ptr_t py_write_queued(stream_protocol::write_interface_t & p)
{
    future::ptr_t f(boost::make_shared<future>());

    p.write_queued(boost::bind(py_on_write_queued, f, _1, _2));
    return f;
}

/////////// queue_write support

void py_queue_write(stream_protocol::write_interface_t & p,
    const bpl::str & buf)
{
    char * cbuf;
    Py_ssize_t length;

    if(PyString_AsStringAndSize(buf.ptr(), &cbuf, &length) == -1)
        bpl::throw_error_already_set();

    p.queue_write(cbuf, cbuf + length);
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

