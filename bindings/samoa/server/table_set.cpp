
#include <boost/python.hpp>
#include "samoa/server/table_set.hpp"
#include "samoa/server/table.hpp"

namespace samoa {
namespace server {

namespace bpl = boost::python;

bpl::dict py_get_uuid_index(const table_set & ts)
{
    bpl::dict d;
    for(const auto & entry : ts.get_uuid_index())
    {
        d[entry.first] = entry.second;
    }
    return d;
}

bpl::dict py_get_name_index(const table_set & ts)
{
    bpl::dict d;
    for(const auto & entry : ts.get_name_index())
    {
        d[entry.first] = entry.second;
    }
    return d;
}

void make_table_set_bindings()
{
    bpl::class_<table_set, table_set::ptr_t, boost::noncopyable>(
        "TableSet", bpl::init<
            const spb::ClusterState &,
            const table_set::ptr_t &>())
        .def("get_table", &table_set::get_table)
        .def("get_table_by_name", &table_set::get_table_by_name)
        .def("get_uuid_index", &py_get_uuid_index)
        .def("get_name_index", &py_get_name_index)
        .def("merge_table_set", &table_set::merge_table_set);
}

}
}

