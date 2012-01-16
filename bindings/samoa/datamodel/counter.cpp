
#include <boost/python.hpp>
#include "samoa/datamodel/counter.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void make_counter_bindings()
{
    bpl::class_<counter>("Counter", bpl::no_init)
        .def("update", &counter::update).staticmethod("update")
        .def("prune", &counter::prune).staticmethod("prune")
        .def("merge", &counter::merge).staticmethod("merge")
        .def("value", &counter::value).staticmethod("value")
        ;
}

}
}

