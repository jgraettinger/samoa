
#include <boost/python.hpp>
#include "samoa/server/command/basic_replicate.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/proactor.hpp"
#include "pysamoa/scoped_python.hpp"
#include "pysamoa/coroutine.hpp"

namespace samoa {
namespace server {
namespace command {

namespace bpl = boost::python;
using namespace std;

class py_basic_replicate_handler :
    public basic_replicate_handler,
    public bpl::wrapper<basic_replicate_handler>
{
public:

    typedef boost::shared_ptr<py_basic_replicate_handler> ptr_t;

    void replicate(
        const client_ptr_t & client,
        const table_ptr_t & target_table,
        const local_partition_ptr_t & target_partition,
        const std::string & key,
        const spb::PersistedRecord_ptr_t & record)
    {
        pysamoa::python_scoped_lock block;

        bpl::object method = this->get_override("replicate");
        SAMOA_ASSERT(method);

        bpl::object result = method(
            client,
            target_table,
            target_partition,
            key, record);

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
            string h_msg = bpl::extract<string>(method.attr("__repr__")());
            throw runtime_error("basic_replicate_handler.replicate(): "
                "expected method to return either a generator or None, "
                "but got:\n\t" + msg + "\n<method was " + h_msg + ">");
        }
    }
};

void make_basic_replicate_handler_bindings()
{
    bpl::class_<py_basic_replicate_handler, py_basic_replicate_handler::ptr_t,
            boost::noncopyable, bpl::bases<command_handler>
        >("BasicReplicateHandler")
        ;
}

}
}
}

