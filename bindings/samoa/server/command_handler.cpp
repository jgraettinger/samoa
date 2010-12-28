
#include "samoa/server/command_handler.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/scoped_python.hpp"
#include "samoa/core/coroutine.hpp"
#include <boost/python.hpp>
#include <iostream>

namespace samoa {
namespace server {

using namespace boost::python;

class py_command_handler :
    public command_handler, public wrapper<command_handler> 
{
public:
    typedef boost::shared_ptr<py_command_handler> ptr_t;

    void handle(const client::ptr_t & client)
    {
        core::scoped_python block;

        // call handler
        object res = this->get_override("handle")(client);

        // is res a generator?
        if(PyGen_Check(res.ptr()))
        {
            // start a new coroutine
            core::coroutine::ptr_t coro(new core::coroutine(res));
            coro->start();
        }
        else if(res.ptr() != Py_None)
        {
            std::string msg = extract<std::string>(res.attr("__repr__")());
            throw std::runtime_error("Expected either generator or "
                "Py_None, but got: " + msg);
        }
    }
};

void make_command_handler_bindings()
{
    class_<py_command_handler, py_command_handler::ptr_t,
        boost::noncopyable>("CommandHandler")
        .def("handle", &command_handler::handle);
}

}
}

