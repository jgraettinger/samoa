#ifndef SAMOA_SERVER_COMMAND_GET_BLOB_HPP
#define SAMOA_SERVER_COMMAND_GET_BLOB_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class get_blob_handler :
    public command_handler,
    public boost::enable_shared_from_this<get_blob_handler>
{
public:

    typedef boost::shared_ptr<get_blob_handler> ptr_t;

    get_blob_handler()
    { }

    void handle(const client_ptr_t &);

private:

    void on_get_record(
        const boost::system::error_code &,
        const client_ptr_t &,
        const spb::PersistedRecord_ptr_t &);
};

}
}
}

#endif

