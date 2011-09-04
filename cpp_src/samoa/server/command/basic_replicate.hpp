#ifndef SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP
#define SAMOA_SERVER_COMMAND_BASIC_REPLICATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include <boost/system/error_code.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace server {
namespace command {

namespace spb = samoa::core::protobuf;

class basic_replicate_handler : public command_handler
{
public:

    typedef boost::shared_ptr<basic_replicate_handler> ptr_t;

    basic_replicate_handler()
    { }

    virtual ~basic_replicate_handler();

    void handle(const client_ptr_t &);

protected:

    /*!
    Performs datatype-specific consistent replication
    */
    virtual void replicate(
        const client_ptr_t &,
        const table_ptr_t & target_table,
        const local_partition_ptr_t & target_partition,
        const std::string & key,
        const spb::PersistedRecord_ptr_t &) = 0;

    /*!
    Helper for derived classes which sends a record back to the client,
    and finishes the response.
    */
    void send_record_response(
        const client_ptr_t &,
        const spb::PersistedRecord_ptr_t &);

    /*!
    Final callback handler for use of derived classes.

    If repl_record != put_record, a merge is assumed to have occurred
    and the result is send back to the client. Otherwise, an empty
    response is returned.
    */
    void on_put_record(
        const boost::system::error_code & ec,
        const client_ptr_t & client,
        const spb::PersistedRecord_ptr_t & repl_record,
        const spb::PersistedRecord_ptr_t & put_record);
};

}
}
}

#endif

