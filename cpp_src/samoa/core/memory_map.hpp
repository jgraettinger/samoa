

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem/path.hpp>
#include <memory>
#include <string>

namespace samoa {
namespace core {

namespace bip = boost::interprocess;

class memory_map
{
public:

    typedef std::unique_ptr<bip::file_mapping> file_mapping_ptr_t;
    typedef std::unique_ptr<bip::file_lock> file_lock_ptr_t;
    typedef std::unique_ptr<bip::mapped_region> mapped_region_ptr_t;

    memory_map(const boost::filesystem::path & path, uint64_t region_size);

    memory_map(const boost::filesystem::path & path);

    ~memory_map();

    const file_mapping_ptr_t & get_mapping() const
    { return _mapping; }

    const mapped_region_ptr_t & get_region() const
    { return _region; }

    uint64_t get_region_size() const
    { return _region_size; }

    void * get_region_address() const
    { return _region->get_address(); }

    const boost::filesystem::path & get_path() const
    { return _path; }

    void close();

private:

    void init();

    boost::filesystem::path _path;
    uint64_t _region_size;

    file_lock_ptr_t _lock;
    file_mapping_ptr_t _mapping;
    mapped_region_ptr_t _region;
};

}
}

