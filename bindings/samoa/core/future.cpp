
#include "samoa/core/future.hpp"
#include "samoa/core/coroutine.hpp"
#include "samoa/core/scoped_python.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace samoa {
namespace core {

future::future()
 : _called(false), _error(false)
{ }

future::~future()
{
    // obtain GIL to safely destroy python state
    scoped_python block;

    _result = _exc_type = _exc_msg = bpl::object();
    _coroutine.reset();
}

// Precondition: Python GIL is held
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
}

void future::on_buffer_result(
    const boost::system::error_code & ec,
    const buffers_iterator_t & begin,
    const buffers_iterator_t & end)
{
    scoped_python block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            (bpl::detail::borrowed_reference) PyExc_RuntimeError);
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
    scoped_python block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            (bpl::detail::borrowed_reference) PyExc_RuntimeError);
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

        _result = bpl::object((bpl::detail::new_reference) tuple);
    }

    _called = true;
    send_result();
}

void future::on_data_result(
    const boost::system::error_code & ec,
    size_t length, const buffer_regions_t & regions)
{
    scoped_python block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            (bpl::detail::borrowed_reference) PyExc_RuntimeError);
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

        _result = bpl::object((bpl::detail::new_reference) buf);
    }

    _called = true;
    send_result();
}

void future::on_length_result(
    const boost::system::error_code & ec, size_t length)
{
    scoped_python block;

    if(ec)
    {
        // save exception
        _exc_type = bpl::object(
            (bpl::detail::borrowed_reference) PyExc_RuntimeError);
        _exc_msg = bpl::str(ec.message());
        _error = true;
    }
    else {
        PyObject * len = PyInt_FromSize_t(length);
        _result = bpl::object((bpl::detail::new_reference) len);
    }

    _called = true;
    send_result();
}


void make_future_bindings()
{
    bpl::class_<future, future::ptr_t, boost::noncopyable>(
        "Future", bpl::no_init);
}

};
};

