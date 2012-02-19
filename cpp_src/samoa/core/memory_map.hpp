

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/mapped_region.hpp>
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

    memory_map(const std::string & file, uint64_t region_size);

    ~memory_map();

    bool was_resized() const
    { return _was_resized; }

    const file_mapping_ptr_t & get_mapping() const
    { return _mapping; }

    const mapped_region_ptr_t & get_region() const
    { return _region; }

    uint64_t get_region_size() const
    { return _region_size; }

    void * get_region_address() const
    { return _region->get_address(); }

private:

    uint64_t _region_size;
    bool _was_resized;

    file_lock_ptr_t _lock;
    file_mapping_ptr_t _mapping;
    mapped_region_ptr_t _region;
};

}
}

