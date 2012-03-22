#ifndef SAMOA_SERVER_COMMAND_DIGEST_SYNC_HPP
#define SAMOA_SERVER_COMMAND_DIGEST_SYNC_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/request/fwd.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

class digest_sync_handler :
    public command_handler,
    public boost::enable_shared_from_this<digest_sync_handler>
{
public:

    typedef boost::shared_ptr<digest_sync_handler> ptr_t;

    void handle(const request::state_ptr_t &);
};

}
}
}

#endif

