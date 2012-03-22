#include "samoa/server/digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/log.hpp"
#include "samoa/error.hpp"
#include <sstream>
#include <fstream>

namespace samoa {
namespace server {

namespace bfs = boost::filesystem;

bfs::path digest::_directory = "/tmp";
uint32_t digest::_default_byte_length;

digest::digest()
{
	// assign reasonable defaults, to be over-ridden by subclasses
    _properties.set_seed(core::random::generate_uint64());
    _properties.set_byte_length(digest::get_default_byte_length());
    _properties.set_element_count(0);
}

digest::~digest()
{ }

void digest::open_filter(const bfs::path & path)
{
    _memory_map.reset(new core::memory_map(path, _properties.byte_length()));
}

void digest::add(const core::murmur_checksum_t & checksum)
{
    uint8_t * filter = reinterpret_cast<uint8_t*>(
        _memory_map->get_region_address());

    uint64_t filter_size = _properties.byte_length() << 3;

    uint64_t bit1 = (_properties.seed() ^ checksum[0]) % filter_size;
    uint64_t bit2 = (_properties.seed() ^ checksum[1]) % filter_size;

    if( (filter[bit1 >> 3] >> (bit1 % 8)) & \
    	(filter[bit2 >> 3] >> (bit2 % 8)) & 1)
    {
    	// already set
    	return;
    }

    _properties.set_element_count(_properties.element_count() + 1);

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

void digest::set_directory(const bfs::path & directory)
{
	SAMOA_ASSERT(bfs::is_directory(directory));
    digest::_directory = directory;
}

void digest::set_default_byte_length(uint32_t length)
{
    digest::_default_byte_length = length;
}

bfs::path digest::generate_filter_path(const core::uuid & uuid)
{
    while(true)
    {
        std::stringstream s;
        s << "digest_" << uuid << "_";
        s << (core::random::generate_uint64() >> 48) << ".filter";

        // TODO: make transactional; there's a race condition here
        bfs::path path = digest::get_directory() / s.str();
        if(!bfs::exists(path))
        {
        	return path;
        }
    }
}

}
}

