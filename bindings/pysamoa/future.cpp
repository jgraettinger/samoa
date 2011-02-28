
#include "future.hpp"
#include "coroutine.hpp"
#include "scoped_python.hpp"
#include "samoa/client/server.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace pysamoa {

namespace bpl = boost::python;
using namespace samoa::core;

future::future()
 : _called(false), _error(false)
{
    std::cerr << "future " << (size_t)this << " created" << std::endl;
}

future::future(const bpl::object & result)
 : _called(true), _error(false), _result(result)
{
    std::cerr << "*precalled* future " << (size_t)this << " created" << std::endl;
}

future::~future()
{
    // obtain GIL to safely destroy python state
    python_scoped_lock block;

    _result = _exc_type = _exc_msg = bpl::object();
    _coroutine.reset();

    std::cerr << "future " << (size_t)this << " destroyed" << std::endl;
}

// precondition: Python GIL is held
void future::set_yielding_coroutine(const coroutine::ptr_t & coro)
{
    _coroutine = coro;
    send_result();
}

// precondition: Python GIL is held
void future::send_result()
{
    // The operation must have finished, and an owning, blocked coroutine
    //   must have been set before a result or error is returned
    if(!_called || !_coroutine)
        return;

    if(_error)
        _coroutine->error(_exc_type, _exc_msg);
    else
        _coroutine->send(_result);

    // clear held references to break cycles
    _result = bpl::object();
    _exc_type = bpl::object();
    _exc_msg = bpl::object();
}

void future::on_buffer_result(
    const boost::system::error_code & ec,
    const buffers_iterator_t & begin,
    const buffers_iterator_t & end)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        // allocate a python copy of the buffer
        _result = bpl::str(0, std::distance(begin, end));
        std::copy(begin, end, PyString_AS_STRING(_result.ptr()));
    }

    _called = true;
    send_result();
}

void future::on_regex_match_result(
    const boost::system::error_code & ec,
    const stream_protocol::match_results_t & match)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
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

        _result = bpl::object(bpl::handle<>(tuple));
    }

    _called = true;
    send_result();
}

void future::on_data_result(
    const boost::system::error_code & ec,
    size_t length, const buffer_regions_t & regions)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
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

        _result = bpl::object(bpl::handle<>(buf));
    }

    _called = true;
    send_result();
}

void future::on_length_result(
    const boost::system::error_code & ec, size_t length)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        PyObject * len = PyInt_FromSize_t(length);
        _result = bpl::object(bpl::handle<>(len));
    }

    _called = true;
    send_result();
}

void future::on_server_connect(
    const boost::system::error_code & ec, samoa::client::server::ptr_t srv)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        _result = bpl::object(srv);
    }

    _called = true;
    send_result();
}

void future::on_server_request(
    const boost::system::error_code & ec,
    samoa::client::server::request_interface req_int)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        _result = bpl::object(req_int);
    }

    _called = true;
    send_result();
}

void future::on_server_response(
    const boost::system::error_code & ec,
    samoa::client::server::response_interface resp_int)
{
    python_scoped_lock block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            bpl::handle<>(bpl::borrowed(PyExc_RuntimeError)));
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        _result = bpl::object(resp_int);
    }

    _called = true;
    send_result();
}

void future::on_error(
    const bpl::object & exc_type, const bpl::object & exc)
{
    _exc_type = exc_type;
    _exc_msg = exc;
    _error = true;

    _called = true;
    send_result();
}

void future::on_result(const bpl::object & result)
{
    _result = result;

    _called = true;
    send_result();
}

}

