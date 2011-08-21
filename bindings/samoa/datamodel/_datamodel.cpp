
#include <boost/python.hpp>

namespace samoa {
namespace datamodel {
    void make_clock_util_bindings();
    void make_blob_bindings();
}
}

BOOST_PYTHON_MODULE(_datamodel)
{
    samoa::datamodel::make_clock_util_bindings();
    samoa::datamodel::make_blob_bindings();
}

