
#include <boost/python.hpp>

namespace samoa {
namespace datamodel {
    void make_cluster_clock_bindings();
    void make_blob_bindings();
}
}

BOOST_PYTHON_MODULE(_datamodel)
{
    samoa::datamodel::make_cluster_clock_bindings();
    samoa::datamodel::make_blob_bindings();
}

