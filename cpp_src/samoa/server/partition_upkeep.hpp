#ifndef SAMOA_SERVER_PARTITION_UPKEEP_HPP
#define SAMOA_SERVER_PARTITION_UPKEEP_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/client/fwd.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/core/periodic_task.hpp"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

/*
class partition_upkeep :
    public core::periodic_task<partition_upkeep>
{
public:

    using core::periodic_task<partition_upkeep>::ptr_t;
    using core::periodic_task<partition_upkeep>::weak_ptr_t;

    static boost::posix_time::time_duration period;

    partition_upkeep(const context_ptr_t &, const table_ptr_t &,
        const local_partition_ptr_t &);

    void begin_cycle();

    static bool is_enabled()
    { return _enabled; }

    static void set_enabled(bool);

protected:

    static bool _enabled;

    void on_iterate(
        const persistence::persister_ptr_t &,
        const persistence::record *);

    bool on_record_expire(bool, const request::state_ptr_t &);

    void on_record_upkeep(
        const persistence::persister_ptr_t &,
        const request::state_ptr_t &);

    void on_move_request(
        const boost::system::error_code &,
        samoa::client::server_request_interface,
        const persistence::persister_ptr_t &,
        const request::state_ptr_t &);

    void on_move_response(
        const boost::system::error_code &,
        samoa::client::server_response_interface,
        const persistence::persister_ptr_t &,
        const request::state_ptr_t &);

    bool on_move_drop(bool);

    void on_compact(
        const boost::system::error_code &,
        const datamodel::merge_result &,
        const request::state_ptr_t &);

    void on_replicate_sync();

    void load_request_state(const request::state_ptr_t &);

    const context_weak_ptr_t _weak_context;
    const persistence::persister_weak_ptr_t _weak_persister;

    const core::uuid _table_uuid;
    const core::uuid _partition_uuid;

    unsigned _ticket;
};
*/

}
}

#endif

