
#include <boost/python.hpp>
#include "samoa/server/command_handler.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"
#include <memory>

namespace samoa {
namespace server {

namespace bpl = boost::python;
using namespace std;

class py_command_handler :
    public command_handler, public bpl::wrapper<command_handler> 
{
public:

    typedef boost::shared_ptr<py_command_handler> ptr_t;

    void handle(const client::ptr_t & client)
    {
        pysamoa::python_scoped_lock block;

        if(!_handler)
        {
            _handler = this->get_override("handle");
            SAMOA_ASSERT(_handler);
        }

        bpl::object result = _handler(client);

        // is result a generator?
        if(PyGen_Check(result.ptr()))
        {
            // start a new coroutine
            pysamoa::coroutine::ptr_t coro(new pysamoa::coroutine(
                result, client->get_io_service()));
            coro->next();
        }
        else if(result.ptr() != Py_None)
        {
            string msg = bpl::extract<string>(result.attr("__repr__")());
            string h_msg = bpl::extract<string>(_handler.attr("__repr__")());
            throw runtime_error("command_handler.handle(): expected "
                "handler to return either a generator or None, "
                "but got:\n\t" + msg + "\n<handler was " + h_msg + ">");
        }
    }

private:

    bpl::object _handler;
};

void make_command_handler_bindings()
{
    bpl::class_<py_command_handler, py_command_handler::ptr_t,
        boost::noncopyable>("CommandHandler")
        .def("handle", &command_handler::handle);
}

}
}

