#ifndef SAMOA_SERVER_REMOTE_DIGEST_HPP
#define SAMOA_SERVER_REMOTE_DIGEST_HPP

#include "samoa/server/digest.hpp"
#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace server {

class remote_digest : public digest
{
public:

    remote_digest(
        const core::uuid & server_uuid,
        const core::uuid & partition_uuid);

    remote_digest(
        const core::uuid & server_uuid,
        const core::uuid & partition_uuid,
        const spb::DigestProperties &,
        const core::buffer_regions_t &);

    virtual ~remote_digest();

    void mark_filter_for_deletion()
    { _marked_for_deletion = true; }

private:

    static boost::filesystem::path properties_path(
        const core::uuid &, const core::uuid &);

    boost::filesystem::path _properties_path;
    bool _marked_for_deletion;
};

}
}

#endif

