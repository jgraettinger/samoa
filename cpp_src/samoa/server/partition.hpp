
#ifndef SAMOA_SERVER_PARTITION_HPP
#define SAMOA_SERVER_PARTITION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class partition
{
public:

    typedef partition_ptr_t ptr_t;

    virtual ~partition();

    const core::uuid & get_uuid()
    { return _uuid; }

    const core::uuid & get_server_uuid()
    { return _server_uuid; }

    unsigned get_ring_position()
    { return _ring_position; }

    unsigned get_consistent_range_begin()
    { return _consistent_range_begin; }

    unsigned get_consistent_range_end()
    { return _consistent_range_end; }

    unsigned get_lamport_ts()
    { return _lamport_ts; }

    //! Merges a peer partition description into the local description
    /*!
        \return true iff the local description was modified
    */
    virtual bool merge_partition(
        const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local) = 0;

protected:

    partition(const spb::ClusterState::Table::Partition &);

    core::uuid _uuid;
    core::uuid _server_uuid;
    unsigned   _ring_position;
    unsigned   _consistent_range_begin;
    unsigned   _consistent_range_end;
    unsigned   _lamport_ts;
};

}
}

#endif

