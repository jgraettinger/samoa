#ifndef SAMOA_SERVER_LOCAL_DIGEST_HPP
#define SAMOA_SERVER_LOCAL_DIGEST_HPP

#include "samoa/server/digest.hpp"

namespace samoa {
namespace server {

class local_digest : public digest
{
public:

    local_digest(const core::uuid & partition_uuid);

    virtual ~local_digest();

private:

    const boost::filesystem::path & properties_path(const core::uuid &);
    const boost::filesystem::path & filter_path(const core::uuid &);
};

}
}

#endif

