#include <boost/python.hpp>
#include "samoa/persistence/data_type.hpp"

namespace samoa {
namespace persistence {

namespace bpl = boost::python;

void make_data_type_bindings()
{
    bpl::enum_<data_type>("DataType")
        .value("BLOB_TYPE", BLOB_TYPE)
        .value("COUNT_TYPE", COUNT_TYPE)
        .value("MAP_TYPE", MAP_TYPE);
}

}
}

