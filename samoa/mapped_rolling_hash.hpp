#ifndef SAMOA_MAPPED_ROLLING_HASH_HPP
#define SAMOA_MAPPED_ROLLING_HASH_HPP

#include "samoa/rolling_hash.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>
#include <memory>

namespace samoa {

namespace bip = boost::interprocess;

typedef std::auto_ptr<bip::file_mapping> file_mapping_ptr_t;
typedef std::auto_ptr<bip::file_lock> file_lock_ptr_t;
typedef std::auto_ptr<bip::mapped_region> mapped_region_ptr_t;

class mapped_rolling_hash : public rolling_hash
{
public:

    static std::auto_ptr<mapped_rolling_hash> open(
        const std::string & file, size_t region_size, size_t table_size)
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

        // obtain a lock on the file
        file_lock_ptr_t flock( new bip::file_lock(file.c_str()));
        if(!flock->try_lock())
            throw std::runtime_error(file + " is locked");

        // open the file in read/write mode for mapping
        file_mapping_ptr_t fmapping( new bip::file_mapping(
            file.c_str(), bip::read_write));

        // map complete file to a chunk of address space
        mapped_region_ptr_t mregion( new bip::mapped_region(
            *fmapping, bip::read_write, 0, region_size));

        return std::auto_ptr<mapped_rolling_hash>(new mapped_rolling_hash(
            flock, fmapping, mregion, region_size, table_size));
    }

    virtual ~mapped_rolling_hash()
    {
        _tbl.state = FROZEN;

        _mregion->flush();
        _mregion.reset();
        _fmapping.reset();
        return;
    }

private:

    mapped_rolling_hash(
        file_lock_ptr_t  flock,
        file_mapping_ptr_t  fmapping,
        mapped_region_ptr_t mregion,
        size_t region_size,
        size_t table_size
    ) :
        rolling_hash::rolling_hash(
            mregion->get_address(), region_size, table_size),
        _flock(flock),
        _fmapping(fmapping),
        _mregion(mregion)
    { }

    file_lock_ptr_t _flock;
    file_mapping_ptr_t _fmapping;
    mapped_region_ptr_t _mregion;
};

};

#endif

