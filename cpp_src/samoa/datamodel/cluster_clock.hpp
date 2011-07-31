#ifndef SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP
#define SAMOA_DATAMODEL_CLUSTER_CLOCK_HPP

#include "samoa/datamodel/partition_clock.hpp"
#include <string>

namespace samoa {
namespace datamodel {

/*! \brief A logical timepoint of the cluster

Partially orders operations in the system as a composition of
partition_clock instances (eg, a vector-clock).
*/
class cluster_clock : public std::vector<partition_clock>
{
public:

    void tick(const core::uuid & partition_uuid); 

    enum clock_ancestry
    {
        EQUAL = 0,
        MORE_RECENT = 1,
        LESS_RECENT = 2,
        DIVERGE = 3
    };

    static clock_ancestry compare(
        const cluster_clock & lhs, const cluster_clock & rhs,
        cluster_clock * merged_clock_out);

    static unsigned serialized_length()
    {
        return packed_unsigned::serialized_length(1) +
               partition_clock::serialized_length();
    }
};

std::ostream & operator << (std::ostream &, const cluster_clock &);
std::istream & operator >> (std::istream &, cluster_clock &);

}
}

#endif

