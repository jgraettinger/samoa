#include "samoa/server/local_digest.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/error.hpp"
#include <boost/filesystem.hpp>
#include <sstream>

namespace samoa {
namespace server {

namespace bfs = boost::filesystem;
using std::begin;
using std::end;

local_digest::local_digest(const core::uuid & uuid)
{
    // TODO: compute length from target element count & false positive rate?

    _memory_map.reset(new core::memory_map(
        generate_filter_path(uuid), get_default_byte_length()));

    // zero bloom filter
    memset(_memory_map->get_region_address(), 0,
        _memory_map->get_region_size());
}

local_digest::~local_digest()
{
    _memory_map->close();

    // delete temporary file backing the mapping
    SAMOA_ASSERT(boost::filesystem::remove(_memory_map->get_path()));
}

}
}
