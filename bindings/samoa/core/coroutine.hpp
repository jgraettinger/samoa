#ifndef SAMOA_CORE_COROUTINE_HPP
#define SAMOA_CORE_COROUTINE_HPP

#include <boost/python.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace core {

namespace bpl = boost::python;

class coroutine :
    public boost::enable_shared_from_this<coroutine>,
    private boost::noncopyable
{
public:
    typedef boost::shared_ptr<coroutine> ptr_t;

    // Precondition: Python GIL is held (Py_INCREF called under the hood)
    coroutine(const bpl::object & generator);

    // Python GIL is aquired (need not be held)
    ~coroutine();

    // Precondition: Python GIL is held
    void start();

    // Precondition: Python GIL is held
    void send(const bpl::object & arg);

    // Precondition: Python GIL is held
    void error(const bpl::object & exc_type, const bpl::object & exc_msg);

private:

    bpl::object _generator;
};

}
}

#endif
