
#include "samoa/core/memory_map.hpp"
#include "samoa/log.hpp"
#include <fstream>

namespace samoa {
namespace core {

memory_map::memory_map(const std::string & file, uint64_t region_size)
  : _region_size(region_size),
    _was_resized(false)
{
    LOG_DBG("Mapped file " << file << ", region_size " << region_size);

    std::fstream fs;

    // check existance & size of file
    fs.open(file, std::ios_base::in);
    fs.seekg(0, std::ios_base::end);

    if(fs.fail() || (uint64_t)fs.tellg() <= region_size)
    {
        if(fs.fail())
        {
            LOG_DBG("File " << file << " doesn't exist");
        }
        else
        {
            LOG_DBG("Resizing " << file << " from " << \
                fs.tellg() << " to " << region_size);
        }

        fs.open(file, std::ios_base::out);
        fs.seekp(region_size);
        fs.put(0);

        if(fs.fail())
            throw std::runtime_error("Failed to size " + file);

        _was_resized = true;
    }
    fs.close();

    // obtain a (co-operative) lock on the file
    _lock.reset(new bip::file_lock(file.c_str()));
    if(!_lock->try_lock())
        throw std::runtime_error(file + " is locked");

    // open the file in read/write mode, for mapping
    _mapping.reset(new bip::file_mapping(file.c_str(), bip::read_write));

    // map complete file to a chunk of address space
    _region.reset(new bip::mapped_region(*_mapping,
        bip::read_write, 0, _region_size));
}

memory_map::~memory_map()
{
    _region->flush();
}

}
}

