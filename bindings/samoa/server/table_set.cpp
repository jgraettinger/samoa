
#include <boost/python.hpp>
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

void make_table_set_bindings()
{
    bpl::class_<table_set, table_set::ptr_t, boost::noncopyable>(
        "TableSet", bpl::init<
            const spb::ClusterState &,
            const table_set::ptr_t &>())
        .def("get_table_by_uuid", &table_set::get_table_by_uuid)
        .def("get_table_by_name", &table_set::get_table_by_name)
        .def("merge_table_set", &table_set::merge_table_set);
}

}
}

