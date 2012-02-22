
#ifndef SAMOA_SERVER_LOCAL_PARTITION_HPP
#define SAMOA_SERVER_LOCAL_PARTITION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/server/partition.hpp"
#include "samoa/persistence/fwd.hpp"
#include "samoa/request/fwd.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/fwd.hpp"
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class local_partition :
    public partition,
    public boost::enable_shared_from_this<local_partition>
{
public:

    typedef local_partition_ptr_t ptr_t;

    //! Constructs a runtime local_partition from protobuf description
    /*!
        \param current The local_partition which this instance will
            be replacing. May be nullptr if there is none.
    */
    local_partition(
        const spb::ClusterState::Table::Partition &,
        uint64_t range_begin, uint64_t range_end,
        const ptr_t & current);

    uint64_t get_author_id() const
    { return _author_id; }

    const persistence::persister_ptr_t & get_persister()
    { return _persister; }

    bool merge_partition(
        const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local) const;

    void initialize(const context_ptr_t &, const table_ptr_t &);

    typedef boost::function<void(
        const boost::system::error_code &,
        const datamodel::merge_result &)
    > write_callback_t;

    void write(
        const write_callback_t &,
        const datamodel::merge_func_t &,
        const request::state_ptr_t &,
        bool is_novel);

    typedef boost::function<void(
        const boost::system::error_code &,
        bool /* found */)
    > read_callback_t;

    void read(
        const read_callback_t &,
        const request::state_ptr_t &);

    void poll_digest_gossip(const context_ptr_t &, const table_ptr_t &);

private:

    void on_local_write(
        const boost::system::error_code &,
        const datamodel::merge_result &,
        const core::murmur_checksum_t &,
        const write_callback_t &,
        const request::state_ptr_t &,
        bool);

    persistence::persister_ptr_t _persister;

    uint64_t _author_id;
    uint64_t _digest_gossip_threshold;
};

}
}

#endif

