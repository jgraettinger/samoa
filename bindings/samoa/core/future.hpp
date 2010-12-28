
#ifndef SAMOA_CORE_FUTURE_HPP
#define SAMOA_CORE_FUTURE_HPP

#include "samoa/core/buffer_region.hpp"
#include "samoa/core/stream_protocol.hpp"
#include <boost/python.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace core {

namespace bpl = boost::python;

// fwd-declare coroutine
class coroutine;
typedef boost::shared_ptr<coroutine> coroutine_ptr_t;

class future
{
public:

    typedef boost::shared_ptr<future> ptr_t;

    future();
    ~future();

    // Precondition: Python GIL is held
    void set_yielding_coroutine(const coroutine_ptr_t & coro);

    // TODO: Perhaps the should be broken out into subclasses?
    //  (Not worth the complexity at the moment)
    void on_buffer_result(
        const boost::system::error_code & ec,
        const buffers_iterator_t & begin,
        const buffers_iterator_t & end);

    void on_regex_match_result(
        const boost::system::error_code & ec,
        const stream_protocol::match_results_t & match);

    void on_data_result(
        const boost::system::error_code & ec,
        size_t length, const buffer_regions_t & regions);

    void on_length_result(
        const boost::system::error_code & ec, size_t length);

private:

    // precondition: Python GIL is held
    void send_result();

    bool _called;
    bool _error;

    bpl::object _result;
    bpl::object _exc_type;
    bpl::object _exc_msg;

    coroutine_ptr_t _coroutine;
};

};
};

#endif

