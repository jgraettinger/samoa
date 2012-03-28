#ifndef SAMOA_SERVER_DIGEST_HPP
#define SAMOA_SERVER_DIGEST_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <boost/filesystem.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class digest
{
public:

    typedef digest_ptr_t ptr_t;

    virtual ~digest();

    void add(const core::murmur_checksum_t &);

    bool test(const core::murmur_checksum_t &) const;

    const spb::DigestProperties & get_properties() const
    { return _properties; }

    const core::memory_map_ptr_t & get_memory_map() const
    { return _memory_map; }

    // Static methods

    static const boost::filesystem::path & get_directory()
    { return _directory; }

    static void set_directory(const boost::filesystem::path &);

    static uint32_t get_default_byte_length()
    { return _default_byte_length; }

    static void set_default_byte_length(uint32_t);

protected:

    digest();

    boost::filesystem::path generate_filter_path(const core::uuid &);

    static uint32_t _default_byte_length;
    static boost::filesystem::path _directory;

    spb::DigestProperties _properties;
    core::memory_map_ptr_t _memory_map;
};

}
}

#endif
