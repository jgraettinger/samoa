#ifndef PYSAMOA_FUTURE_HPP
#define PYSAMOA_FUTURE_HPP

#include <boost/python.hpp>
#include "pysamoa/fwd.hpp"
#include <boost/system/error_code.hpp>

namespace pysamoa {

namespace bpl = boost::python;

class future
{
public:

    typedef future_ptr_t ptr_t;

    future();
    future(const bpl::object & result);
    ~future();

    // Precondition: Python GIL is held
    void set_yielding_coroutine(const coroutine_ptr_t & coro);

    void set_reenter_via_post();

    // precondition: Python GIL is held
    void on_error(
        const boost::system::error_code &);

    // precondition: Python GIL is held
    void on_error(
        const bpl::object & exception_type,
        const bpl::object & exception);

    // precondition: Python GIL is held
    void on_result(const bpl::object & result);

    /*
    // TODO: Perhaps the should be broken out into subclasses?
    //  (Not worth the complexity at the moment)
    void on_buffer_result(
        const boost::system::error_code & ec,
        const samoa::core::buffers_iterator_t & begin,
        const samoa::core::buffers_iterator_t & end);

    void on_regex_match_result(
        const boost::system::error_code & ec,
        const samoa::core::stream_protocol::match_results_t & match);

    void on_data_result(
        const boost::system::error_code & ec,
        size_t length, const samoa::core::buffer_regions_t & regions);

    void on_length_result(
        const boost::system::error_code & ec, size_t length);

    void on_server_connect(
        const boost::system::error_code & ec,
        samoa::client::server_ptr_t);

    void on_server_request(
        const boost::system::error_code & ec,
        samoa::client::server_request_interface);

    void on_server_response(
        const boost::system::error_code & ec,
        samoa::client::server_response_interface);
    */

private:

    // precondition: Python GIL is held
    void send_result();

    bool _called;
    bool _error;
    bool _reenter_via_post;

    bpl::object _result;
    bpl::object _exc_type;
    bpl::object _exc_msg;

    coroutine_ptr_t _coroutine;
};

}

#endif

