#ifndef SAMOA_SERVER_REMOTE_DIGEST_HPP
#define SAMOA_SERVER_REMOTE_DIGEST_HPP

#include "samoa/server/digest.hpp"
#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace server {

class remote_digest : public digest
{
public:

    remote_digest(const core::uuid & partition_uuid);

    remote_digest(const core::uuid & partition_uuid,
        const spb::DigestProperties &,
        const core::buffer_regions_t &);

    virtual ~remote_digest();

private:

    static boost::filesystem::path properties_path(const core::uuid &);
    static boost::filesystem::path filter_path(const core::uuid &);

    boost::filesystem::path _properties_path;
};

}
}

#endif

