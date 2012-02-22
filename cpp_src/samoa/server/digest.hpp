#ifndef SAMOA_SERVER_DIGEST_HPP
#define SAMOA_SERVER_DIGEST_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/fwd.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class digest
{
public:

    typedef digest_ptr_t ptr_t;

    digest(const core::uuid & partition_uuid);

    void add(const core::murmur_checksum_t &);

    bool test(const core::murmur_checksum_t &) const;

    void clear();

    // Static methods

    static const std::string & get_path_base()
    { return _path_base; }

    static void set_path_base(const std::string &);

    static uint32_t get_default_byte_length()
    { return _default_byte_length; }

    static void set_default_byte_length(uint32_t);

private:

    static std::string _path_base;
    static uint32_t _default_byte_length;

    core::uuid _partition_uuid;
    spb::DigestProperties _properties;
    core::memory_map_ptr_t _memory_map;
};

}
}

#endif
