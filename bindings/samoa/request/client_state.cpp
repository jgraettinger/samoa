
#include <boost/python.hpp>
#include "samoa/request/client_state.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

void make_client_state_bindings()
{
    // just enough support for unit-tests
    bpl::class_<client_state, boost::noncopyable>("ClientState", bpl::init<>())
        .def("mutable_samoa_request", &client_state::mutable_samoa_request,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("validate_samoa_request_syntax",
            &client_state::validate_samoa_request_syntax)
        ;
}

}
}

