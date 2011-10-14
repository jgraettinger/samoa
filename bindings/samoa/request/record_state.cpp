
#include <boost/python.hpp>
#include "samoa/request/record_state.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

void make_record_state_bindings()
{
    bpl::class_<record_state, boost::noncopyable>("RecordState", bpl::init<>())
        .def("get_local_record", &record_state::get_local_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("get_remote_record", &record_state::get_remote_record,
            bpl::return_value_policy<bpl::reference_existing_object>())
        .def("reset_record_state", &record_state::reset_record_state)
        ;
}

}
}

