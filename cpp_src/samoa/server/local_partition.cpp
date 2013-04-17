#include "samoa/server/local_partition.hpp"
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table_set.hpp"
#include "samoa/server/peer_set.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/eventual_consistency.hpp"
#include "samoa/server/local_digest.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/client/server.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/core/memory_map.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <functional>
#include <memory>
#include <set>

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end)
 :
    partition(part, range_begin, range_end, true),
    _persister(std::make_shared<persistence::persister>()),
    _persister_strand(
        std::make_shared<boost::asio::strand>(
            *core::proactor::get_proactor()->concurrent_io_service())),
    _author_id(core::random::generate_uint64())
{
    SAMOA_ASSERT(part.ring_layer_size());

    // add persister layers
    typedef spb::ClusterState::Table::Partition::RingLayer layer_t;
    for(const layer_t & layer : part.ring_layer())
    {
        if(layer.has_file_path())
        {
            _persister->add_mapped_hash_ring(
                layer.file_path(), layer.storage_size(), layer.index_size());
        }
        else
        {
            _persister->add_heap_hash_ring(
                layer.storage_size(), layer.index_size());
        }
    }

    // begin a new digest; threshold is size of leaf persister layer
    set_digest(std::make_shared<local_digest>(get_uuid()));
    _digest_gossip_threshold = _persister->used_storage();
}

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end,
    const local_partition & current)
 :
    partition(part, range_begin, range_end, true),
    _persister(current._persister),
    _persister_strand(current._persister_strand),
    _author_id(
        (current.get_range_begin() == get_range_begin() &&
         current.get_range_end()   == get_range_end()) ?
         current.get_author_id() : core::random::generate_uint64())
{
    SAMOA_ASSERT((unsigned)part.ring_layer_size() == _persister->layer_count());

    // Note on the _author_id initializer:
    //
    // It's safe to preserve current's author id only if *this has
    //  identical ranges of responsibility; otherwise, we open ourselves
    //  to the possibility of writing a new version of an existing key
    //  with an existing author clock, such that we appear to be more
    //  recent than that previously written clock

    // take over current digest & compaction threshold
    set_digest(current.get_digest());
    _digest_gossip_threshold = current._digest_gossip_threshold;
}

bool local_partition::merge_partition(
    const spb::ClusterState::Table::Partition & peer,
    spb::ClusterState::Table::Partition & local) const
{
    // with the exception of being dropped, local_partitions are
    //  only to be modified... locally
    SAMOA_ASSERT(local.lamport_ts() >= peer.lamport_ts());
    return false;
}

void local_partition::initialize(
    const context::ptr_t & context, const table::ptr_t & table)
{
    local_partition::ptr_t self = shared_from_this();
    auto isolate = [self, context, table]()
    {
        self->_persister->set_prune_callback(
            table->get_consistent_prune());

        eventual_consistency::ptr_t tmp = 
            std::make_shared<eventual_consistency>(
                context, table->get_uuid(), self->get_uuid());

        self->_persister->set_upkeep_callback(
            std::bind(&eventual_consistency::upkeep, tmp,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3));
    };
    _persister_strand->dispatch(isolate);
}

void local_partition::write(
    const local_partition::write_callback_t & write_callback,
    const datamodel::merge_func_t & merge_callback,
    const request::state::ptr_t & rstate,
    bool is_novel)
{
    _persister->put(
        std::bind(&local_partition::on_local_write, shared_from_this(),
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3,
            write_callback, rstate, is_novel),
        datamodel::merge_func_t(merge_callback),
        rstate->get_key(),
        rstate->get_remote_record(),
        rstate->get_local_record());
}

void local_partition::on_local_write(
    const boost::system::error_code & ec,
    const datamodel::merge_result & merge_result,
    const core::murmur_checksum_t & checksum,
    const local_partition::write_callback_t & client_callback,
    const request::state_ptr_t & rstate,
    bool is_novel_write)
{
    auto callback = [client_callback, rstate, merge_result]()
    {
        // update response with quorum success/failure
        rstate->get_samoa_response().set_replication_success(
            rstate->get_peer_success_count());
        rstate->get_samoa_response().set_replication_failure(
            rstate->get_peer_failure_count());

        client_callback(boost::system::error_code(), merge_result);
    };

    if(ec || (is_novel_write && !merge_result.local_was_updated))
    {
        // an error occurred, or novel write was aborted
        client_callback(ec, merge_result);
        return;
    }

    get_digest()->add(checksum);

    // count local write against the quorum
    if(rstate->peer_replication_success())
    {
        callback();
    }

    if( rstate->is_replication_finished() && \
        !is_novel_write && \
        !merge_result.remote_is_stale)
    {
        // quorum is met (size of 1), and this is an undiverged replication
        //   we don't need to fan out, and we assume all remote peers have
        //   been replicated to as well

        for(const partition::ptr_t & partition : rstate->get_peer_partitions())
        {
            if(dynamic_cast<remote_partition*>(partition.get()))
            {
                partition->get_digest()->add(checksum);
            }
        }
        return;
    }

    auto on_peer_request = [rstate](
        const samoa::client::server_request_interface & iface,
        const partition::ptr_t &) -> bool
    {
        // serialize local record to the peer
        core::protobuf::zero_copy_output_adapter zco_adapter;
        SAMOA_ASSERT(rstate->get_local_record(
            ).SerializeToZeroCopyStream(&zco_adapter));
        iface.add_data_block(zco_adapter.output_regions());

        // request should still be made
        return true;
    };

    auto on_peer_response = [callback, rstate, checksum](
        boost::system::error_code ec,
        const samoa::client::server_response_interface &,
        const partition::ptr_t & partition)
    {
        if(ec)
        {
            if(rstate->peer_replication_failure())
            {
                // quorum has failed
                callback();
            }
        }
        else
        {
            // mark successful replication to remote peers
            if(dynamic_cast<remote_partition*>(partition.get()))
            {
                partition->get_digest()->add(checksum);
            }

            if(rstate->peer_replication_success())
            {
                // quorum has passed
                callback();
            }
        }
    };

    replication::replicate(on_peer_request, on_peer_response, rstate);
}

void local_partition::read(
    const local_partition::read_callback_t & client_callback,
    const request::state_ptr_t & rstate)
{
    local_partition::ptr_t self = shared_from_this();

    auto callback = [self, client_callback, rstate]()
    {
        // update response with quorum success/failure
        rstate->get_samoa_response().set_replication_success(
            rstate->get_peer_success_count());
        rstate->get_samoa_response().set_replication_failure(
            rstate->get_peer_failure_count());

        if(rstate->had_peer_read_hit())
        {
            // speculatively write the merged remote record; as a side-effect,
            //  local-record will be populated with the merged result
            auto on_put = [self, client_callback, rstate](
                const boost::system::error_code & ec,
                const datamodel::merge_result & merge_result,
                const core::murmur_checksum_t & content_checksum)
            {
                if(!ec && merge_result.local_was_updated)
                {
                    // a repair occurred; track the new checksum
                    self->get_digest()->add(content_checksum);
                }
                client_callback(ec, true);
            };

            self->get_persister()->put(
                on_put,
                datamodel::merge_func_t(
                    rstate->get_table()->get_consistent_merge()),
                rstate->get_key(),
                rstate->get_remote_record(),
                rstate->get_local_record());
        }
        else
        {
            // no populated remote record; fall back on simple persister read
            self->get_persister()->get(
                std::bind(client_callback,
                    boost::system::error_code(), std::placeholders::_1),
                rstate->get_key(),
                rstate->get_local_record());
        }
    };

    // assume the local read will succeed
    if(rstate->peer_replication_success())
    {
        callback();
        return;
    }

    auto on_peer_request = [rstate](
        const samoa::client::server_request_interface &,
        const partition::ptr_t &) -> bool
    {
        // if quorum is already met, abort the request
        return !rstate->is_replication_finished();
    };

    auto on_peer_response = [callback, rstate](
        boost::system::error_code ec,
        const samoa::client::server_response_interface & iface,
        const partition::ptr_t & partition)
    {
        if(ec)
        {
            if(rstate->peer_replication_failure())
            {
                // quorum has failed
                callback();
            }
            return;
        }

        // is this a non-empty response?
        if(!iface.get_response_data_blocks().empty())
        {
            SAMOA_ASSERT(iface.get_response_data_blocks().size() == 1);
            const core::buffer_regions_t & regions = \
                iface.get_response_data_blocks()[0];

            // is a remote partition?
            if(dynamic_cast<remote_partition*>(partition.get()))
            {
                // checksum response content, and add to partition digest
                core::murmur_hash cs;

                cs.process_bytes(rstate->get_key().data(),
                    rstate->get_key().size());

                for(const core::buffer_region & buffer : regions)
                {
                    cs.process_bytes(buffer.begin(), buffer.size());
                }
                partition->get_digest()->add(cs.checksum());
            }

            // is the response still needed?
            if(!rstate->is_replication_finished())
            {
                // parse into local-record (used here as scratch space)
                core::protobuf::zero_copy_input_adapter zci_adapter(
                    iface.get_response_data_blocks()[0]);

                SAMOA_ASSERT(rstate->get_local_record(
                    ).ParseFromZeroCopyStream(&zci_adapter));

                // merge local-record into remote-record
                rstate->get_table()->get_consistent_merge()(
                    rstate->get_remote_record(), rstate->get_local_record());

                // mark that a peer read hit
                rstate->set_peer_read_hit();
            }
        }

        if(rstate->peer_replication_success())
        {
            // quorum has passed
            callback();
        }
    };

    replication::replicate(on_peer_request, on_peer_response, rstate);
}

void local_partition::poll_digest_gossip(const context::ptr_t & context,
    const core::uuid & table_uuid, const core::uuid & partition_uuid)
{
    // We want to synchronize swapping out digests with cluster state
    //  transactions which may be going on; isolate using the cluster
    //  state transaction io_service

    auto digest_transaction = [context, table_uuid, partition_uuid]()
    {
        cluster_state::ptr_t cluster_state = context->get_cluster_state();

        table::ptr_t table = cluster_state->get_table_set(
            )->get_table(table_uuid);

        if(!table)
        {
            // race condition: table dropped
            return;
        }

        local_partition::ptr_t self = \
            std::dynamic_pointer_cast<local_partition>(
                table->get_partition(partition_uuid));

        if(!self)
        {
            // race condition: local partition dropped
            return;
        }

        uint64_t current = self->get_persister(
            )->total_leaf_compaction_bytes();

        if(current < self->_digest_gossip_threshold)
        {
            return;
        }

        // swap out the current digest for an empty one
        digest::ptr_t digest = self->get_digest();
        self->set_digest(std::make_shared<local_digest>(self->get_uuid()));

        // new threshold for the new digest; we'll want to gossip again when
        //  we've performed leaf compactions over all current records
        self->_digest_gossip_threshold = \
            self->_persister->used_storage() + current;

        // identify this partition's location in the table ring
        table::ring_t::const_iterator this_it = std::lower_bound(
            std::begin(table->get_ring()), std::end(table->get_ring()), self,
            [](const partition::ptr_t & lhs, const partition::ptr_t & rhs)
            {
                if(lhs->get_ring_position() == rhs->get_ring_position())
                    return lhs->get_uuid() < rhs->get_uuid();

                return lhs->get_ring_position() < rhs->get_ring_position();
            });

        std::set<core::uuid> peer_servers;

        // collect servers by walking backwards over replicating partitions
        table::ring_t::const_iterator it = this_it;
        for(unsigned i = 1; i != table->get_replication_factor(); ++i)
        {
            if(it == std::begin(table->get_ring()))
                it = std::end(table->get_ring());

            peer_servers.insert((*(--it))->get_server_uuid());
        }

        // collect servers by walking forwards over replicating partitions
        it = this_it;
        for(unsigned i = 1; i != table->get_replication_factor(); ++i)
        {
            if(++it == std::end(table->get_ring()))
                it = std::begin(table->get_ring());

            peer_servers.insert((*it)->get_server_uuid());
        }

        peer_servers.erase(context->get_server_uuid());

        // begin a digest sync request for each peer
        for(const core::uuid & peer_uuid: peer_servers)
        {
            auto on_request = [peer_uuid, table_uuid, partition_uuid, digest](
                const boost::system::error_code & ec,
                samoa::client::server_request_interface iface)
            {
                if(ec)
                {
                    LOG_WARN("digest sync to " << peer_uuid << \
                        "failed: " << ec.message());
                    return;
                }

                spb::SamoaRequest & samoa_request = iface.get_message();
                samoa_request.set_type(spb::DIGEST_SYNC);

                samoa_request.mutable_table_uuid()->assign(
                    std::begin(table_uuid), std::end(table_uuid));
                samoa_request.mutable_partition_uuid()->assign(
                    std::begin(partition_uuid), std::end(partition_uuid));

                samoa_request.mutable_digest_properties(
                    )->CopyFrom(digest->get_properties());
                samoa_request.mutable_digest_properties(
                    )->clear_filter_path();

                char * r_begin = reinterpret_cast<char *>(
                    digest->get_memory_map()->get_region_address());

                iface.add_data_block(core::buffer_region(
                    r_begin,
                    r_begin + digest->get_memory_map()->get_region_size()));

                // on_response closure captures digest to guard lifetime
                auto on_response = [peer_uuid, digest](
                    const boost::system::error_code & ec,
                    samoa::client::server_response_interface iface)
                {
                    if(ec)
                    {
                        LOG_WARN("digest sync to " << peer_uuid << \
                            " failed: " << ec.message());
                        return;
                    }
                    else if(iface.get_error_code())
                    {
                        LOG_WARN("server " << peer_uuid << \
                            " remote error on digest sync" << \
                            iface.get_message().error().ShortDebugString()); 
                    }
                    iface.finish_response();
                };
                iface.flush_request(on_response);
            };
            cluster_state->get_peer_set()->schedule_request(
                on_request, peer_uuid); 
        }
    };
    context->get_cluster_state_transaction_service()->dispatch(
        digest_transaction);
}

}
}

