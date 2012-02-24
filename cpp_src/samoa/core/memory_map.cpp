
#include "samoa/core/memory_map.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/filesystem.hpp>
#include <fstream>

namespace samoa {
namespace core {

namespace bfs = boost::filesystem;

memory_map::memory_map(
    const bfs::path & path,
    uint64_t region_size)
 :  _path(path), 
    _region_size(region_size),
    _was_resized(false)
{
    LOG_DBG("Mapped path " << path << ", region_size " << region_size);

    if(!bfs::exists(path))
    {
        LOG_INFO(path << " doesn't exist (creating)");

        // touch to create the file
        std::ofstream fs(path.string());
        SAMOA_ASSERT(!fs.fail() && "failed to open for writing");
    }

    if(bfs::file_size(path) <= region_size)
    {
        LOG_DBG("Resizing " << path << " to " << region_size);

        std::ofstream fs(path.string());
        fs.seekp(region_size);
        fs.put(0);

        SAMOA_ASSERT(!fs.fail() && "failed to resize file");
        _was_resized = true;
    }

    // obtain a (co-operative) lock on the file
    _lock.reset(new bip::file_lock(path.c_str()));
    if(!_lock->try_lock())
        throw std::runtime_error(path.string() + " is locked");

    // open the file in read/write mode, for mapping
    _mapping.reset(new bip::file_mapping(path.c_str(), bip::read_write));

    // map complete file to a chunk of address space
    _region.reset(new bip::mapped_region(*_mapping,
        bip::read_write, 0, _region_size));
}

memory_map::~memory_map()
{
    if(_region)
    {
        _region->flush();
    }
}

void memory_map::close()
{
    SAMOA_ASSERT(_region);

    _region->flush();
    _region.reset();
    _mapping.reset();
}

}
}

