
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
    const spb::PersistedRecord & record)
{
    spb::SamoaResponse & samoa_response = client->get_response();

    samoa_response.mutable_cluster_clock()->CopyFrom(
        record.cluster_clock());

    for(auto val_it = record.blob_value().begin();
        val_it != record.blob_value().end(); ++val_it)
    {
        samoa_response.add_data_block_length(val_it->size());
    }

    client->start_response();

    for(auto val_it = record.blob_value().begin();
        val_it != record.blob_value().end(); ++val_it)
    {
        client->write_interface().queue_write(
            val_it->begin(), val_it->end());
    }

    client->finish_response();
}

}
}

