#ifndef PYSAMOA_COROUTINE_HPP
#define PYSAMOA_COROUTINE_HPP

#include <boost/python.hpp>
#include "pysamoa/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include <boost/noncopyable.hpp>
#include <vector>
#include <memory>

namespace pysamoa {

namespace bpl = boost::python;

class coroutine :
    public boost::enable_shared_from_this<coroutine>,
    private boost::noncopyable
{
public:

    typedef coroutine_ptr_t ptr_t;

    // Precondition: Python GIL is held (Py_INCREF called under the hood)
    coroutine(const bpl::object & generator,
        const samoa::core::io_service_ptr_t &);

    // Python GIL need not be held (is aquired internally)
    ~coroutine();

    // Precondition: Python GIL is held
    void next(bool as_post = false);

    // Precondition: Python GIL is held
    void send(const bpl::object & arg, bool as_post = false);

    // Precondition: Python GIL is held
    void error(const bpl::object & exc_type,
        const bpl::object & exc_msg, bool as_post = false);

private:

    // Aquires GIL
    void on_reenter(const bpl::object & arg);

    std::vector<bpl::object> _stack;

    bool _exception_set;
    bpl::object _exc_type;
    bpl::object _exc_val;
    bpl::object _exc_traceback;

    samoa::core::io_service_ptr_t _io_srv;
};

}

#endif
