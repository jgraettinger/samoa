#ifndef SAMOA_SERVER_PARTITION_HPP
#define SAMOA_SERVER_PARTITION_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/protobuf/samoa.pb.h"
#include "samoa/core/uuid.hpp"
#include "samoa/spinlock.hpp"

namespace samoa {
namespace server {

namespace spb = samoa::core::protobuf;

class partition
{
public:

    typedef partition_ptr_t ptr_t;

    virtual ~partition();

    const core::uuid & get_uuid() const
    { return _uuid; }

    const core::uuid & get_server_uuid() const
    { return _server_uuid; }

    uint64_t get_ring_position() const
    { return _ring_position; }

    //! Inclusive
    uint64_t get_range_begin() const
    { return _range_begin; }

    //! Inclusive
    uint64_t get_range_end() const
    { return _range_end; }

    //! Inclusive
    uint64_t get_consistent_range_begin() const
    { return _consistent_range_begin; }

    //! Inclusive
    uint64_t get_consistent_range_end() const
    { return _consistent_range_end; }

    uint64_t get_lamport_ts() const
    { return _lamport_ts; }

    bool is_tracked() const
    { return _is_tracked; }

    digest_ptr_t get_digest() const;

    bool position_in_responsible_range(uint64_t ring_position) const;

    //! Merges a peer partition description into the local description
    /*!
        \return true iff the local description was modified
    */
    virtual bool merge_partition(
        const spb::ClusterState::Table::Partition & peer,
        spb::ClusterState::Table::Partition & local) const = 0;

    virtual void initialize(const context_ptr_t &, const table_ptr_t &) = 0;

protected:

    partition(const spb::ClusterState::Table::Partition &,
        uint64_t range_begin, uint64_t range_end, bool is_tracked);

    void set_digest(const digest_ptr_t &);

    const core::uuid _uuid;
    const core::uuid _server_uuid;
    const uint64_t   _ring_position;
    const uint64_t   _range_begin;
    const uint64_t   _range_end;
    const uint64_t   _consistent_range_begin;
    const uint64_t   _consistent_range_end;
    const uint64_t   _lamport_ts;
    const bool       _is_tracked;

private:

    digest_ptr_t _digest;
    mutable spinlock _digest_lock;
};

}
}

#endif

