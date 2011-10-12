#ifndef SAMOA_REQUEST_ROUTE_STATE_HPP
#define SAMOA_REQUEST_ROUTE_STATE_HPP

#include "samoa/server/fwd.hpp"
#include "samoa/core/uuid.hpp"
#include <vector>
#include <string>

namespace samoa {
namespace request {

class route_state
{
public:

    route_state();

    virtual ~route_state();

    /*!
     * Retrieves explicitly-configured route key
     */
    const std::string & get_key() const
    { return _key; }

    /*!
     * Retrieves the key's position in the table's ring continuum.
     *
     * Note: Only available after load_route_state()
     */
    uint64_t get_ring_position() const
    { return _ring_position; }

    /*! 
     * Retrives whether a primary-partition uuid is set on this route.
     *
     * A primary-partition uuid is either explicitly set, or dynamically
     *  set by load_route_state()
     */
    bool has_primary_partition_uuid() const
    { return _primary_partition_uuid.is_nil(); }

    /*!
     * Retrieves the primary-partition uuid set on this route, or
     *  the nil uuid if none is set.
     *
     * A primary-partition uuid may be explicitly set, or may be
     *  dynamically set by load_route_state()
     */
    const core::uuid & get_primary_partition_uuid() const
    { return _primary_partition_uuid; }

    /*!
     * Retrieves the primary local partition of this route, or null
     *  if there is none.
     */
    const server::local_partition_ptr_t & get_primary_partition() const
    { return _primary_partition; }

    /*!
     * Retrieves whether peer-partition uuids are set on this route.
     *
     * Peer-partition uuids are either explicitly set, or dynamically
     *  set by load_route_state()
     */
    bool has_peer_partition_uuids() const
    { return !_peer_partition_uuids.empty(); }

    /*!
     * Retrieves peer-partition uuids set on this route.
     *
     * Peer-partition uuids are either explicitly set, or dynamically
     *  set by load_route_state()
     *
     * Invariant: uuids are always returned in sorted order.
     */
    const std::vector<core::uuid> get_peer_partition_uuids() const
    { return _peer_partition_uuids; }

    /*!
     * Retrieves the peer partitions of this route.
     *
     * If no primary partition is available, peer partitions will have
     *  arity equal to the table's replication factor, and represent
     *  a 'best guess' as to the actual partitions responsible for storing
     *  the key.
     *
     * If a primary partition _is_ available, peer partitions are of
     *  arity one less than the table's replication factor, and represent
     *  the _exact_ set of peers responsible for this key.
     *
     * See the chord protocol for details. 
     */
    const std::vector<server::partition_ptr_t> & get_peer_partitions() const
    { return _peer_partitions; }

    /*!
     * Sets the key which is being routed.
     *
     * Must be set prior to load_route_state()
     */
    void set_key(std::string && key);

    /*!
     * Sets the primary (local) partition of this route.
     *
     * Must be set prior to load_route_state()
     */
    void set_primary_partition_uuid(const core::uuid &);

    /*!
     * Adds a peer-partition of this route
     *
     * Requires that a primary-partition uuid is set.
     * All calls must be made prior to load_route_state()
     *
     * If any peer partition-uuid's are added, all uuids
     *  of the key's route must be added.
     */
    void add_peer_partition_uuid(const core::uuid &);

    void load_route_state(const server::table_ptr_t &);

    void reset_route_state();

private:

    std::string _key;
    uint64_t _ring_position;

    core::uuid _primary_partition_uuid;
    std::vector<core::uuid> _peer_partition_uuids;

    server::local_partition_ptr_t _primary_partition;
    std::vector<server::partition_ptr_t> _peer_partitions;
};

}
}

#endif

