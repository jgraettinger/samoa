#ifndef SAMOA_DATAMODEL_BLOB_HPP
#define SAMOA_DATAMODEL_BLOB_HPP

#include "samoa/persistence/record.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/datamodel/cluster_clock.hpp"
#include "samoa/datamodel/partition_clock.hpp"
#include "samoa/datamodel/packed_unsigned.hpp"
#include "samoa/core/buffer_region.hpp"
#include <string>

namespace samoa {
namespace datamodel {

/*! 

Blob is serialized as:

    - cluster_clock
    - repeated:
      - unsigned (blob length)
      - repeated byte (blob value)

*/
class blob
{
public:

    /*! \brief Size of a serialized blob, assuming no current value

    Because a blob-write will replace any existing values, including their
    clocks, the sum of expected_write_size and the current record size must
    be an upper bound on the required record size to store the blob
    */
    static unsigned serialized_length(unsigned blob_length)
    {
        return  packed_unsigned::serialized_length(1) +
                partition_clock::serialized_length() +
                packed_unsigned::serialized_length(blob_length) +
                blob_length;
    };

    static void send_blob_value(
        const server::client_ptr_t &,
        const persistence::record &,
        std::string & version_tag);

    static void write_blob_value(
        const cluster_clock & clock,
        const core::buffer_regions_t & value,
        persistence::record & record_out);
};

}
}

#endif

