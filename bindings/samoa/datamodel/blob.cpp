#include "pysamoa/boost_python.hpp"
#include "samoa/datamodel/blob.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

bpl::list py_value(const spb::PersistedRecord & record)
{
    bpl::list values;
    blob::value(record, [&values](const std::string & value)
        { values.append(value); });

    return values;
}

void make_blob_bindings()
{
    void(*update_ptr)(spb::PersistedRecord &, uint64_t,
        const std::string &) = &blob::update;

    bpl::class_<blob>("Blob", bpl::no_init)
        .def("update", update_ptr).staticmethod("update")
        .def("prune", &blob::prune).staticmethod("prune")
        .def("merge", &blob::merge).staticmethod("merge")
        .def("value", &py_value).staticmethod("value")
        ;
}

}
}

