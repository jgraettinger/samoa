#ifndef SAMOA_SERVER_COMMAND_SET_BLOB_HPP
#define SAMOA_SERVER_COMMAND_SET_BLOB_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <vector>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class set_blob_handler :
    public command_handler,
    public boost::enable_shared_from_this<set_blob_handler>
{
public:

    typedef boost::shared_ptr<set_blob_handler> ptr_t;

    set_blob_handler()
    { }

    void handle(const client_ptr_t &);

private:

    spb::PersistedRecord_ptr_t on_merge_record(
        const client_ptr_t &,
        const partition_ptr_t &,
        const spb::PersistedRecord_ptr_t &,
        const spb::PersistedRecord_ptr_t &);

    void on_put_record(
        const boost::system::error_code &,
        const client_ptr_t &,
        const partition_ptr_t &,
        const std::vector<partition_ptr_t> &,
        const spb::PersistedRecord_ptr_t &);
};

}
}
}

#endif

