#ifndef SAMOA_SERVER_COMMAND_REPLICATE_BLOB_HPP
#define SAMOA_SERVER_COMMAND_REPLICATE_BLOB_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/server/command/basic_replicate.hpp"
#include <boost/system/error_code.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class replicate_blob_handler :
    public basic_replicate_handler,
    public boost::enable_shared_from_this<replicate_blob_handler>
{
public:

    typedef boost::shared_ptr<replicate_blob_handler> ptr_t;

    replicate_blob_handler()
    { }

protected:

    void replicate(
        const client_ptr_t &,
        const table_ptr_t & target_table,
        const local_partition_ptr_t & target_partition,
        const std::string & key,
        const spb::PersistedRecord_ptr_t &);
    
    spb::PersistedRecord_ptr_t on_merge_record(
        const client_ptr_t &, unsigned,
        const spb::PersistedRecord_ptr_t &,
        const spb::PersistedRecord_ptr_t &);
};

}
}
}

#endif

