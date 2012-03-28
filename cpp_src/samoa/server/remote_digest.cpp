#include "samoa/server/remote_digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <fstream>

namespace samoa {
namespace server {

namespace bfs = boost::filesystem;
using std::begin;
using std::end;

remote_digest::remote_digest(const core::uuid & uuid)
 :  _properties_path(properties_path(uuid)),
    _marked_for_deletion(false)
{
    {
        if(bfs::exists(_properties_path))
        {
        	// read existing properties
            std::ifstream fs(_properties_path.string());
            SAMOA_ASSERT(_properties.ParseFromIstream(&fs));

            _memory_map.reset(new core::memory_map(_properties.filter_path()));
        }
        else
        {
            _properties.set_filter_path(generate_filter_path(uuid).string());

        	// write new, generated properties
            std::ofstream fs(_properties_path.string());
            SAMOA_ASSERT(_properties.SerializeToOstream(&fs));

            _memory_map.reset(new core::memory_map(
                _properties.filter_path(), get_default_byte_length()));

            // zero bloom filter
            memset(_memory_map->get_region_address(), 0,
                _memory_map->get_region_size());
        }
    }
}

remote_digest::remote_digest(
    const core::uuid & uuid,
    const spb::DigestProperties & properties,
    const core::buffer_regions_t & buffers)
 :  _properties_path(properties_path(uuid)),
    _marked_for_deletion(false)
{
    _properties.CopyFrom(properties);
    _properties.set_filter_path(generate_filter_path(uuid).string());

    // copy buffers into digest filter
    uint64_t total_size = 0;
    for(const core::buffer_region & region : buffers)
    {
    	total_size += region.size();
    }

    _memory_map.reset(new core::memory_map(
        _properties.filter_path(), total_size));

    char * it_out = reinterpret_cast<char*>(
        _memory_map->get_region_address());

    for(const core::buffer_region & region : buffers)
    {
    	it_out = std::copy(region.begin(), region.end(), it_out);
    }

    // write new properties to disk
    std::ofstream fs(_properties_path.string());
    SAMOA_ASSERT(_properties.SerializeToOstream(&fs));
}

remote_digest::~remote_digest()
{
	if(_marked_for_deletion)
    {
    	// delete filter backing this digest
        _memory_map->close();
        SAMOA_ASSERT(boost::filesystem::remove(_properties.filter_path()));
    }
    else
    {
    	// sync current properties to disk
        std::ofstream fs(_properties_path.string());
        SAMOA_ABORT_UNLESS(_properties.SerializeToOstream(&fs));
    }
}

bfs::path remote_digest::properties_path(const core::uuid & uuid)
{
    bfs::path path = digest::get_directory();
    path /= "digest_" + core::to_hex(uuid) + ".properties";
    return path;
}

}
}

