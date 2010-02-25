#ifndef COMMON_MAPPED_ROLLING_HASH_HPP
#define COMMON_MAPPED_ROLLING_HASH_HPP

#include "common/rolling_hash.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>
#include <memory>

namespace common {

typedef std::auto_ptr<boost::interprocess::file_mapping> file_mapping_ptr_t;
typedef std::auto_ptr<boost::interprocess::mapped_region> mapped_region_ptr_t;

template<
    int OffsetBytes = 4,
    int KeyLenBytes = 1,
    int ValLenBytes = 3,
    int ExpireBytes = 4
>
class mapped_rolling_hash :
    public rolling_hash<OffsetBytes, KeyLenBytes, ValLenBytes, ExpireBytes>
{
public:
    
    static boost::shared_ptr<mapped_rolling_hash> open_mapped_rolling_hash(
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
        
        file_mapping_ptr_t fmapping( new boost::interprocess::file_mapping(
            file.c_str(), boost::interprocess::read_write));
        mapped_region_ptr_t mregion( new boost::interprocess::mapped_region(
            *fmapping, boost::interprocess::read_write, 0, region_size));
        
        return boost::shared_ptr<mapped_rolling_hash>( new mapped_rolling_hash(
            fmapping, mregion, region_size, table_size));
    }
    
    ~mapped_rolling_hash()
    {
        this->freeze();
        _mregion->flush();
    }
    
private:
    
    mapped_rolling_hash(
        file_mapping_ptr_t  fmapping,
        mapped_region_ptr_t mregion,
        size_t region_size,
        size_t table_size
    ) :
        rolling_hash<OffsetBytes, KeyLenBytes, ValLenBytes, ExpireBytes
            >::rolling_hash(mregion->get_address(), region_size, table_size),
        _fmapping(fmapping),
        _mregion(mregion)
    { }
    
    file_mapping_ptr_t _fmapping;
    mapped_region_ptr_t _mregion;
};

}; // end namespace common

#endif // guard

