
#include <boost/python.hpp>
#include "samoa/datamodel/blob.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

void make_blob_bindings()
{
    bpl::class_<blob>("Blob", bpl::no_init)
        .def("consistent_merge", &blob::consistent_merge)
        .staticmethod("consistent_merge")
        ;
}

}
}

