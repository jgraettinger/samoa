
#include "samoa/persistence/mapped_rolling_hash.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>

namespace samoa {
namespace persistence {

namespace bip = boost::interprocess;

typedef std::unique_ptr<bip::file_mapping> file_mapping_ptr_t;
typedef std::unique_ptr<bip::file_lock> file_lock_ptr_t;
typedef std::unique_ptr<bip::mapped_region> mapped_region_ptr_t;

// private implementation pattern for boost::interprocess state
struct mapped_rolling_hash::pimpl_t
{
    size_t region_size;
    size_t index_size;
    file_lock_ptr_t     flock;
    file_mapping_ptr_t  fmapping;
    mapped_region_ptr_t mregion;
};

mapped_rolling_hash::mapped_rolling_hash(pimpl_ptr_t pimpl)
 : rolling_hash::rolling_hash(
    pimpl->mregion->get_address(), pimpl->region_size, pimpl->index_size),
   _pimpl(std::move(pimpl))
{ }

mapped_rolling_hash::~mapped_rolling_hash()
{
    // persist table by 'freezing' it & flushing
    _tbl.state = FROZEN;

    _pimpl->mregion->flush();
    _pimpl->mregion.reset();
    _pimpl->fmapping.reset();
    return;
}

std::unique_ptr<mapped_rolling_hash> mapped_rolling_hash::open(
    const std::string & file, size_t region_size, size_t index_size)
{
    if(std::ifstream(file.c_str()).fail())
    {
        std::ofstream tmp(file.c_str());
        if(tmp.fail())
            throw std::runtime_error("Failed to open " + file);

        tmp.seekp(region_size);
        tmp.put(0);

        if(tmp.fail())
            throw std::runtime_error("Failed to size " + file);
    }

    pimpl_ptr_t p(new pimpl_t());
    p->region_size = region_size;
    p->index_size = index_size;

    // obtain a lock on the file
    p->flock.reset(new bip::file_lock(file.c_str()));
    if(!p->flock->try_lock())
        throw std::runtime_error(file + " is locked");

    // open the file in read/write mode for mapping
    p->fmapping.reset(new bip::file_mapping(file.c_str(), bip::read_write));

    // map complete file to a chunk of address space
    p->mregion.reset(new bip::mapped_region(
        *p->fmapping, bip::read_write, 0, region_size));

    return std::unique_ptr<mapped_rolling_hash>(new mapped_rolling_hash(std::move(p)));
}

}
}

