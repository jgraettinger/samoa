#include "samoa/server/digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/log.hpp"
#include "samoa/error.hpp"
#include <sstream>
#include <fstream>

namespace samoa {
namespace server {

std::string digest::_path_base;
uint32_t digest::_default_byte_length;

digest::digest(const core::uuid & partition_uuid)
 :  _partition_uuid(partition_uuid)
{
    // open or create the digest properties
    {
        std::stringstream spath;
        spath << digest::_path_base << _partition_uuid << "_digest.properties";
        std::string path = spath.str();

        LOG_DBG("Digest " << _partition_uuid << " properties: " << path);

        std::fstream fs(path, std::ios_base::in);

        if(fs.fail())
        {
            _properties.set_seed(core::random::generate_uint64());
            _properties.set_byte_length(digest::_default_byte_length);

            fs.open(path, std::ios_base::out);
            SAMOA_ABORT_IF(!_properties.SerializeToOstream(&fs));
        }
        else
        {
            SAMOA_ABORT_IF(!_properties.ParseFromIstream(&fs));
        }
    }

    // memory-map the digest bloom filter
    {
        std::stringstream spath;
        spath << digest::_path_base << _partition_uuid << "_digest.filter";
        std::string path = spath.str();

        LOG_DBG("Digest " << _partition_uuid << " filter: " << path);

        _memory_map.reset(new core::memory_map(
            path, _properties.byte_length()));

        if(_memory_map->was_resized())
        {
            LOG_DBG("Filter was resized; zeroing");
            memset(_memory_map->get_region_address(), 0,
                _memory_map->get_region_size());
        }
    }
}

void digest::clear()
{
    std::stringstream spath;
    spath << digest::_path_base << _partition_uuid << "_digest.properties";
    std::string path = spath.str();

    _properties.set_seed(core::random::generate_uint64());

    std::fstream fs(path, std::ios_base::out);
    SAMOA_ABORT_IF(!_properties.SerializeToOstream(&fs));

    memset(_memory_map->get_region_address(), 0,
        _memory_map->get_region_size());
}

void digest::add(const core::murmur_checksum_t & checksum)
{
    uint8_t * filter = reinterpret_cast<uint8_t*>(
        _memory_map->get_region_address());

    uint64_t filter_size = _properties.byte_length() << 3;

    uint64_t bit1 = (_properties.seed() ^ checksum[0]) % filter_size;
    uint64_t bit2 = (_properties.seed() ^ checksum[1]) % filter_size;

    filter[bit1 >> 3] |= 1 << (bit1 % 8);
    filter[bit2 >> 3] |= 1 << (bit2 % 8);
}

bool digest::test(const core::murmur_checksum_t & checksum) const
{
    const uint8_t * filter = reinterpret_cast<uint8_t*>(
        _memory_map->get_region_address());

    uint64_t filter_size = _properties.byte_length() << 3;

    uint64_t bit1 = (_properties.seed() ^ checksum[0]) % filter_size;
    uint64_t bit2 = (_properties.seed() ^ checksum[1]) % filter_size;

    return (filter[bit1 >> 3] >> (bit1 % 8)) & \
           (filter[bit2 >> 3] >> (bit2 % 8)) & 1;
}

void digest::set_path_base(const std::string & path_str)
{
    digest::_path_base = path_str;
}

void digest::set_default_byte_length(uint32_t length)
{
    digest::_default_byte_length = length;
}

}
}

