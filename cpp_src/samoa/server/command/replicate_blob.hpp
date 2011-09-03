#ifndef SAMOA_SERVER_COMMAND_REPLICATE_BLOB_HPP
#define SAMOA_SERVER_COMMAND_REPLICATE_BLOB_HPP

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

class replicate_blob_handler :
    public command_handler,
    public boost::enable_shared_from_this<replicate_blob_handler>
{
public:

    typedef boost::shared_ptr<replicate_blob_handler> ptr_t;

    replicate_blob_handler()
    { }

    void handle(const client_ptr_t &);

private:

    spb::PersistedRecord_ptr_t on_merge_record(
        const client_ptr_t &, unsigned,
        const spb::PersistedRecord_ptr_t &,
        const spb::PersistedRecord_ptr_t &);

    void on_put_record(
        const boost::system::error_code &,
        const client_ptr_t &,
        const spb::PersistedRecord_ptr_t &,
        const spb::PersistedRecord_ptr_t &);

    void send_record_response(
        const client_ptr_t &,
        const spb::PersistedRecord_ptr_t &);
};

}
}
}

#endif

