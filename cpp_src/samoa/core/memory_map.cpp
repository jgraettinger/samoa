
#include "samoa/core/memory_map.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/filesystem.hpp>
#include <fstream>

namespace samoa {
namespace core {

namespace bfs = boost::filesystem;

memory_map::memory_map(const bfs::path & path, uint64_t region_size)
 :  _path(path), 
    _region_size(region_size)
{
    SAMOA_ASSERT(region_size);
    LOG_DBG("Mapping path " << path << ", region_size " << region_size);

    if(!bfs::exists(path))
    {
        LOG_INFO(path << " doesn't exist (creating)");

        // create the backing file, and seek to proper region size
        std::ofstream fs(path.string());
        SAMOA_ASSERT(!fs.fail() && "failed to open for writing");

        fs.seekp(region_size - 1);
        fs.put(0);
    }

    SAMOA_ASSERT(bfs::file_size(path) == region_size);
    init();
}

memory_map::memory_map(const bfs::path & path)
 :  _path(path),
    _region_size(bfs::file_size(path))
{
    LOG_DBG("Remapping path " << path << ", region_size " << _region_size);

    SAMOA_ASSERT(bfs::exists(path) && _region_size);
    init();
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

void memory_map::init()
{
    // obtain a (co-operative) lock on the file
    _lock.reset(new bip::file_lock(_path.c_str()));
    if(!_lock->try_lock())
        throw std::runtime_error(_path.string() + " is locked");

    // open the file in read/write mode, for mapping
    _mapping.reset(new bip::file_mapping(_path.c_str(), bip::read_write));

    // map complete file to a chunk of address space
    _region.reset(new bip::mapped_region(*_mapping,
        bip::read_write, 0, _region_size));
}

}
}

