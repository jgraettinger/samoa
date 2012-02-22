#include "samoa/server/local_partition.hpp"
#include "samoa/server/remote_partition.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/table.hpp"
#include "samoa/server/eventual_consistency.hpp"
#include "samoa/server/digest.hpp"
#include "samoa/server/replication.hpp"
#include "samoa/client/server.hpp"
#include "samoa/persistence/persister.hpp"
#include "samoa/datamodel/clock_util.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/core/protobuf/zero_copy_output_adapter.hpp"
#include "samoa/core/protobuf/zero_copy_input_adapter.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>

namespace samoa {
namespace server {

local_partition::local_partition(
    const spb::ClusterState::Table::Partition & part,
    uint64_t range_begin, uint64_t range_end,
    const ptr_t & current)
 :  partition(part, range_begin, range_end),
    _author_id(datamodel::clock_util::generate_author_id())
{
    SAMOA_ASSERT(part.ring_layer_size());

    if(current)
    {
        _persister = current->_persister;

        if(current->get_range_begin() == get_range_begin() &&
            current->get_range_end() == get_range_end())
        {
            // safe to preserve author id only if we have identical ranges of
            //  responsibility; otherwise, we open ourselves to the possibility
            //  of writing a new version of an existing key with an existing
            //  author clock, such that we appear to be more recent than that
            //  previously written clock
            _author_id = current->get_author_id();
        }

        _digest_compaction_threshold = current->_digest_compaction_threshold;
    }
    else
    {
        _persister.reset(new persistence::persister());

        LOG_DBG("local_partition " << part.uuid() \
            << " built persister " << _persister.get());

        for(auto it = part.ring_layer().begin();
            it != part.ring_layer().end(); ++it)
        {
            if(it->has_file_path())
            {
                _persister->add_mapped_hash_ring(
                    it->file_path(), it->storage_size(), it->index_size());
            }
            else
            {
                _persister->add_heap_hash_ring(
                    it->storage_size(), it->index_size());
            }
        }

        // threshold is size of leaf persister layer
        _digest_compaction_threshold = _persister->leaf_layer().region_size();
    }
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
    _persister->set_prune_callback(
        table->get_consistent_prune());

    _persister->set_upkeep_callback(
        boost::bind(&eventual_consistency::upkeep,
            boost::make_shared<eventual_consistency>(
                context, table->get_uuid(), get_uuid()),
            _1, _2, _3));
}

void local_partition::write(
    const local_partition::write_callback_t & write_callback,
    const datamodel::merge_func_t & merge_callback,
    const request::state::ptr_t & rstate,
    bool is_novel)
{
    _persister->put(
        boost::bind(&local_partition::on_local_write,
            shared_from_this(), _1, _2, _3, write_callback, rstate, is_novel),
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
        samoa::client::server_request_interface & iface,
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
        const boost::system::error_code & ec,
        samoa::client::server_response_interface &,
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
            self->get_persister()->put(
                boost::bind(client_callback, _1, true),
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
                boost::bind(client_callback, boost::system::error_code(), _1),
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
        samoa::client::server_request_interface &,
        const partition::ptr_t &) -> bool
    {
        // if quorum is already met, abort the request
        return !rstate->is_replication_finished();
    };

    auto on_peer_response = [callback, rstate](
        const boost::system::error_code & ec,
        samoa::client::server_response_interface & iface,
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
    const table::ptr_t & table)
{
    if(_persister->total_leaf_compaction_bytes() < _digest_gossip_threshold)
    {
        return;
    }




}

}
}

