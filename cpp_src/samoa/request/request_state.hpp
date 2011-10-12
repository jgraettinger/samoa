#ifndef SAMOA_REQUEST_STATE_HPP
#define SAMOA_REQUEST_STATE_HPP

#include "samoa/request/fwd.hpp"
#include "samoa/request/io_service_state.hpp"
#include "samoa/request/context_state.hpp"
#include "samoa/request/client_state.hpp"
#include "samoa/request/table_state.hpp"
#include "samoa/request/route_state.hpp"
#include "samoa/request/record_state.hpp"
#include "samoa/request/replication_state.hpp"
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

namespace samoa {
namespace request {

class state :
    private io_service_state,
    private context_state,
    private client_state,
    private table_state,
    private route_state,
    private record_state,
    private replication_state,
    public boost::enable_shared_from_this<state>
{
public:

    typedef boost::shared_ptr<state> ptr_t;

    state();

    io_service_state & get_io_service_state()
    { return *this; }

    context_state & get_context_state()
    { return *this; }

    client_state & get_client_state()
    { return *this; }

    table_state & get_table_state()
    { return *this; }

    route_state & get_route_state()
    { return *this; }

    record_state & get_record_state()
    { return *this; }

    replication_state & get_replication_state()
    { return *this; }

    // expose a subset of methods, which pop up again & again
    using io_service_state::get_io_service;
    using context_state::get_peer_set;

    using table_state::get_table_uuid;
    using table_state::get_table;

    /*!
     * Parses the protobuf SamoaRequest captured by
     *  client_state::get_samoa_request(), and sets attributes of
     *  other composed member states as directed by the request.
     */
    void parse_from_protobuf_request();

    /*!
     * Delegates a reset to all member states.
     */
    void reset_state();
};

}
}

#endif

