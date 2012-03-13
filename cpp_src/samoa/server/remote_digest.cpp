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
 :  _properties_path(properties_path(uuid))
{
    {
        if(bfs::exists(_properties_path))
        {
        	// read existing properties
            std::ifstream fs(_properties_path.string());
            SAMOA_ASSERT(_properties.ParseFromIstream(&fs));
        }
        else
        {
        	// write generated properties
            std::ofstream fs(_properties_path.string());
            SAMOA_ASSERT(_properties.SerializeToOstream(&fs));
        }
    }
    open_filter(filter_path(uuid));

	if(_memory_map->was_resized())
    {
        memset(_memory_map->get_region_address(), 0,
            _memory_map->get_region_size());
    }
}

remote_digest::remote_digest(
    const core::uuid & uuid,
    const spb::DigestProperties & properties,
    const core::buffer_regions_t & buffers)
 :  _properties_path(properties_path(uuid))
{
    _properties.CopyFrom(properties);

    // write out properties
    std::ofstream fs(_properties_path.string());
    SAMOA_ASSERT(_properties.SerializeToOstream(&fs));

    open_filter(filter_path(uuid));

    // copy buffers into digest filter
    uint64_t total_size = 0;
    for(const core::buffer_region & region : buffers)
    {
    	total_size += region.size();
    }

    SAMOA_ASSERT(total_size == _properties.byte_length());
    SAMOA_ASSERT(total_size == _memory_map->get_region_size());

    char * it_out = reinterpret_cast<char*>(
        _memory_map->get_region_address());

    for(const core::buffer_region & region : buffers)
    {
    	it_out = std::copy(region.begin(), region.end(), it_out);
    }
}

remote_digest::~remote_digest()
{
    std::ofstream fs(_properties_path.string());
    SAMOA_ABORT_IF(!_properties.SerializeToOstream(&fs));
}

bfs::path remote_digest::properties_path(const core::uuid & uuid)
{
    bfs::path path = digest::get_directory();
    path /= "digest_" + core::to_hex(uuid) + ".properties";
    return path;
}

bfs::path remote_digest::filter_path(const core::uuid & uuid)
{
    bfs::path path = digest::get_directory();
    path /= "digest_" + core::to_hex(uuid) + ".filter";
    return path;
}

}
}

