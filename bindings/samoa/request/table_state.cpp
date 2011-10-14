
#include <boost/python.hpp>
#include "samoa/request/table_state.hpp"
#include "samoa/server/table.hpp"

namespace samoa {
namespace request {

namespace bpl = boost::python;

void make_table_state_bindings()
{
    bpl::class_<table_state, boost::noncopyable>("TableState", bpl::init<>())
        .def("has_table_uuid", &table_state::has_table_uuid)
        .def("get_table_uuid", &table_state::get_table_uuid,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("has_table_name", &table_state::has_table_name)
        .def("get_table_name", &table_state::get_table_name,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("get_table", &table_state::get_table,
            bpl::return_value_policy<bpl::copy_const_reference>())
        .def("set_table_uuid", &table_state::set_table_uuid)
        .def("set_table_name", &table_state::set_table_name)
        .def("load_table_state", &table_state::load_table_state)
        .def("reset_table_state", &table_state::reset_table_state)
        ;
}

}
}

