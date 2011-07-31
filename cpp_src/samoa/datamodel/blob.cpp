
#include "samoa/datamodel/blob.hpp"
#include "samoa/server/client.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/error.hpp"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

namespace samoa {
namespace datamodel {

namespace bio = boost::iostreams;
namespace spb = samoa::core::protobuf;

void blob::send_blob_value(
    const server::client::ptr_t & client,
    const persistence::record & record,
    std::string & version_tag)
{
    spb::SamoaResponse & samoa_response = client->get_response();

    bio::stream<bio::array_source> istr(
        record.value_begin(), record.value_end());

    cluster_clock clock;
    istr >> clock;

    // as placed within the record, the cluster_clock runs from
    //  the first byte through istr.tellg(); directly copy
    version_tag.assign(record.value_begin(),
        record.value_begin() + istr.tellg());

    // parse out the length of each blob value
    //  stop when we reach the length of the record value
    std::streampos total_length = 0;

    while(true)
    {
        unsigned end_offset = istr.tellg() + total_length;

        if(end_offset == record.value_length())
        {
            break;
        }

        SAMOA_ASSERT(end_offset < record.value_length());

        packed_unsigned blob_length;
        istr >> blob_length;

        samoa_response.add_data_block_length(blob_length);
        total_length += blob_length;
    }

    // start sending the response
    client->start_response();

    // blob values are packed in the remainder of the
    //   record value; just directly copy to the client
    client->write_interface().queue_write(
        record.value_begin() + istr.tellg(), record.value_end());

    client->finish_response();
}

void blob::write_blob_value(
    const cluster_clock & clock,
    const core::buffer_regions_t & value,
    persistence::record & record_out)
{
    bio::stream<bio::array_sink> ostr(
        record_out.value_begin(), record_out.value_end());

    unsigned value_len = 0;
    for(auto it = value.begin(); it != value.end(); ++it)
    {
        value_len += it->size();
    }

    ostr << clock;
    ostr << packed_unsigned(value_len);
    ostr << value;

    ostr.flush();
    record_out.trim_value_length(ostr.tellp());
}

}
}

