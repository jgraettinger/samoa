
#include <boost/python.hpp>
#include "samoa/datamodel/cluster_clock.hpp"
#include <boost/uuid/uuid_io.hpp>
#include <sstream>

namespace samoa {
namespace datamodel {

namespace bpl = boost::python;

std::string py_repr(const cluster_clock & c)
{
    std::stringstream s;

    bool first = true;
    for(auto it = c.begin(); it != c.end(); ++it)
    {
        if(first)
        {
            s << "ClusterClock<";
            first = false;
        }
        else
            s << ",";

        s << "{";
        s << it->partition_uuid << ",";
        s << it->unix_timestamp << ",";
        s << it->lamport_tick;
        s << "}";
    }
    s << ">";

    return s.str();
}

std::string py_str(const cluster_clock & c)
{
    std::stringstream s;

    s << c;
    return s.str();
}

cluster_clock py_from_str(const std::string s_in)
{
    std::stringstream s(s_in);

    cluster_clock clock;
    s >> clock;

    return clock;
}

void make_cluster_clock_bindings()
{
    bpl::class_<cluster_clock>("ClusterClock", bpl::init<>())
        .def("__repr__", &py_repr)
        .def("__str__", &py_str)
        .def("from_string", &py_from_str)
        .staticmethod("from_string")
        .def("tick", &cluster_clock::tick)
        .def("serialized_length", &cluster_clock::serialized_length)
        .staticmethod("serialized_length")
        .def("compare", &cluster_clock::compare,
            (bpl::arg("lhs"),
             bpl::arg("rhs"),
             bpl::arg("merged_clock_out") = bpl::object()))
        .staticmethod("compare")
        ;

    bpl::enum_<cluster_clock::clock_ancestry>("ClockAncestry")
        .value("EQUAL", cluster_clock::EQUAL)
        .value("MORE_RECENT", cluster_clock::MORE_RECENT)
        .value("LESS_RECENT", cluster_clock::LESS_RECENT)
        .value("DIVERGE", cluster_clock::DIVERGE)
        ;


}

}
}

