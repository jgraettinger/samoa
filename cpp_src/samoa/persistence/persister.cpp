
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash/heap_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/mapped_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/hash_ring.hpp"
#include "samoa/persistence/rolling_hash/value_zco_adapter.hpp"
#include "samoa/persistence/rolling_hash/value_zci_adapter.hpp"
#include "samoa/persistence/rolling_hash/key_gather_iterator.hpp"
#include "samoa/persistence/rolling_hash/value_gather_iterator.hpp"
#include "samoa/request/request_state.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>
#include <algorithm>

namespace samoa {
namespace persistence {

using namespace std;

persister::persister()
 : _proactor(core::proactor::get_proactor()),
   _strand(*_proactor->concurrent_io_service())
{}

persister::~persister()
{
    LOG_DBG("persister " << this);

    for(size_t i = 0; i != _layers.size(); ++i)
        delete _layers[i];
}

const rolling_hash::hash_ring & persister::layer(size_t index) const
{ return *_layers.at(index); }

void persister::add_heap_hash_ring(uint32_t storage_size, uint32_t index_size)
{
    LOG_DBG("persister " << this << " adding heap hash {"
        << storage_size << ", " << index_size << "}");

    _layers.push_back(rolling_hash::heap_hash_ring::open(
        storage_size, index_size).release());
}

void persister::add_mapped_hash_ring(const std::string & file,
    uint32_t storage_size, uint32_t index_size)
{
    LOG_DBG("persister " << this << " adding mapped hash {"
        << file << ", " << storage_size << ", " << index_size << "}");

    _layers.push_back(rolling_hash::mapped_hash_ring::open(
        file, storage_size, index_size).release());
}

void persister::get(
    get_callback_t && callback,
    const std::string & key,
    spb::PersistedRecord & record)
{
    _strand.post(
        boost::bind(&persister::on_get,
            shared_from_this(),
            std::move(callback),
            boost::cref(key),
            boost::ref(record)));
}

void persister::drop(
    drop_callback_t && callback,
    const std::string & key,
    spb::PersistedRecord & record)
{
    _strand.post(
        boost::bind(&persister::on_drop,
            shared_from_this(),
            std::move(callback),
            boost::cref(key),
            boost::ref(record)));
}

size_t persister::iteration_begin()
{
    spinlock::guard guard(_iterators_lock);

    size_t i = 0;
    for(; i != _iterators.size(); ++i)
    {
    	iterator & it = _iterators[i];
        if(it.state == iterator::DEAD)
        {
            it.state = iterator::IDLE;
            it.layer_ind = _layers.size();
            it.packet = nullptr;

            step_iterator(it);
            return i;
        }
    }

    // no iterator slots available? add one
    if(i == _iterators.size())
    {
        _iterators.push_back({iterator::IDLE, _layers.size(), nullptr});
        step_iterator(_iterators.back());
    }
    return i;
}

void persister::iteration_next(iterate_callback_t && callback, size_t ticket)
{
    spinlock::guard guard(_iterators_lock);

    SAMOA_ASSERT(_iterators.at(ticket).state != iterator::DEAD);
    SAMOA_ASSERT(_iterators.at(ticket).state != iterator::POSTED);

    _strand.post(
        boost::bind(&persister::on_iteration_next,
            shared_from_this(),
            std::move(callback),
            ticket));

    _iterators[ticket].state = iterator::POSTED;
}

void persister::put(
    put_callback_t && callback,
    datamodel::merge_func_t && merge_func,
    const std::string & key,
    const spb::PersistedRecord & remote_record,
    spb::PersistedRecord & local_record)
{
    _strand.post(
        boost::bind(&persister::on_put,
            shared_from_this(),
            std::move(callback),
            std::move(merge_func),
            boost::cref(key),
            boost::cref(remote_record),
            boost::ref(local_record)));
}

void persister::set_record_upkeep_callback(
    const persister::record_upkeep_callback_t & callback)
{
    spinlock::guard guard(_record_upkeep_lock);
    _record_upkeep = callback;
}

void persister::bottom_up_compaction(
    bottom_up_compaction_callback_t && callback)
{
    _strand.post(
        boost::bind(&persister::on_bottom_up_compaction,
            shared_from_this(),
            std::move(callback)));
}

void persister::on_get(
    const get_callback_t & callback,
    const std::string & key,
    spb::PersistedRecord & record)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        rolling_hash::hash_ring::locator loc = _layers[i]->locate_key(key);

        if(!loc.element_head) continue;

        rolling_hash::element element(_layers[i], loc.element_head);

        rolling_hash::value_zci_adapter zci_adapter(element);
        SAMOA_ASSERT(record.ParseFromZeroCopyStream(&zci_adapter));

        callback(true);
        return;
    }
    callback(false);
}

void persister::on_drop(
    const drop_callback_t & callback,
    const std::string & key,
    spb::PersistedRecord & record)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        rolling_hash::hash_ring::locator loc = _layers[i]->locate_key(key);

        if(!loc.element_head) continue;

        rolling_hash::element element(_layers[i], loc.element_head);
        rolling_hash::value_zci_adapter zci_adapter(element);

        SAMOA_ASSERT(record.ParseFromZeroCopyStream(&zci_adapter));

        if(callback(true))
        {
            // client 'commits' drop, by returning true from callback
            _layers[i]->drop_from_hash_chain(loc);
            element.set_dead();
        }
        return;
    }
    callback(false);
}

void persister::on_iteration_next(
    const iterate_callback_t & callback, size_t ticket)
{
    rolling_hash::element element;

    {
        spinlock::guard guard(_iterators_lock);
        iterator & it = _iterators.at(ticket);

        SAMOA_ASSERT(it.state == iterator::POSTED);

        if(it.packet && it.packet->is_dead())
        {
            // a ring update shifted our iterator; step to repair
            step_iterator(it);
        }

        size_t layer_ind = it.layer_ind;
        rolling_hash::packet * result = it.packet;

        step_iterator(it);

        if(!result)
        {
        	// we're returning a null element via callback;
        	//  the iteration ticket is implicitly released
            it.state = iterator::DEAD;
        }
        else
        {
            element = rolling_hash::element(_layers[layer_ind], result);
            it.state = iterator::IDLE;
        }
    }

    callback(element);
}

void persister::on_put(
    const put_callback_t & put_callback,
    const datamodel::merge_func_t & merge_func,
    const std::string & key,
    const spb::PersistedRecord & remote_record,
    spb::PersistedRecord & local_record)
{
    // garden path result
    datamodel::merge_result result;
    result.local_was_updated = true;
    result.remote_is_stale = false;

    // for efficiency, we keep locators for both the root layer
    //   (where the key may be inserted), and the layer in
    //   which the key is currently found (and may be marked dead)
    rolling_hash::hash_ring::locator root_locator = {0, nullptr, nullptr};
    rolling_hash::hash_ring::locator inner_locator = {0, nullptr, nullptr};

    // lambda to encapsulate the details of root vs leaf locator
    auto locator = [&root_locator, &inner_locator](size_t layer_ind) \
        -> rolling_hash::hash_ring::locator &
    { return layer_ind ? inner_locator : root_locator; };

    // first, attempt to find a previous record instance
    size_t layer_ind;
    for(layer_ind = 0; layer_ind != _layers.size(); ++layer_ind)
    {
        locator(layer_ind) = _layers[layer_ind]->locate_key(key);

        if(locator(layer_ind).element_head)
            break;
    }

    //LOG_INFO("root_locator " << root_locator.element_head << " " << 
    //    root_locator.previous_chained_head << " " << 
    //    root_locator.index_location);

    //LOG_INFO("inner_locator " << inner_locator.element_head << " " << 
    //    inner_locator.previous_chained_head << " " << 
    //    inner_locator.index_location);

    //LOG_INFO("layer_ind is " << layer_ind);

    rolling_hash::element old_element;

    if(locator(layer_ind).element_head)
    {
        // an element exists under this key; parse into local_record
        old_element = rolling_hash::element(_layers[layer_ind],
            locator(layer_ind).element_head);

        rolling_hash::value_zci_adapter zci_adapter(old_element);
        SAMOA_ASSERT(local_record.ParseFromZeroCopyStream(&zci_adapter));

        // give caller the opportunity to merge remote_record into local_record
        result = merge_func(local_record, remote_record);
        if(!result.local_was_updated)
        {
            // local_record wasn't updated => write should be aborted
            put_callback(boost::system::error_code(), result);
            return;
        }
    }
    else
    {
        // no element exists under this key; directly copy remote_record
        local_record.CopyFrom(remote_record);
    }

    SAMOA_ASSERT(local_record.IsInitialized());
    uint32_t required_capacity = key.size() + local_record.ByteSize();

    //LOG_INFO("required_capacity " << required_capacity);

    // do we have enough space to write over the old value, in-place?
    if(!old_element.is_null() && old_element.capacity() >= required_capacity)
    {
        // in-place write
        write_record_with_cached_sizes(local_record, old_element);

        put_callback(boost::system::error_code(), result);
        return;
    }

    // A closure (passed to top_down_compaction) which tracks whether
    //   a layer rotation invalidates either our root or inner locator
    //   (and therefore necessitates a second lookup of the key)
    bool locators_invalidated = false;

    auto invalidates_check = [&](size_t rotating_layer)
    {
        rolling_hash::hash_ring::locator & loc = locator(rotating_layer);
        rolling_hash::packet * head = _layers[rotating_layer]->head();

        if(head == loc.element_head || head == loc.previous_chained_head)
        {
            locators_invalidated = true;
        }
    };

    // attempt to allocate packets for the write
    rolling_hash::packet * new_head = \
        _layers[0]->allocate_packets(required_capacity);

    size_t total_compactions = 0;
    uint32_t max_compaction = required_capacity;

    // if insufficient space is available, compact at least
    // max_compaction * max_compaction_factor() * len(layers) bytes before
    // aborting, where max_compaction is the larger of required_capacity,
    // and the largest element observed during compaction
    while(!new_head && total_compactions < \
    	(max_compaction * max_compaction_factor() * _layers.size()))
    {
        uint32_t this_compaction = top_down_compaction(invalidates_check);

        max_compaction = std::max(max_compaction, this_compaction);
        total_compactions += this_compaction;

        new_head = _layers[0]->allocate_packets(required_capacity);
    }

    if(!new_head)
    {
        // we failed to free sufficient space; return error to caller
        put_callback(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory),
            result);
        return;
    }

    // TODO write-ahead log insertion point

    if(locators_invalidated)
    {
        //LOG_INFO("locators WERE invalidated!");
        // compaction invalidated our locators; re-query for key
        for(layer_ind = 0; layer_ind != _layers.size(); ++layer_ind)
        {
            locator(layer_ind) = _layers[layer_ind]->locate_key(key);

            if(locator(layer_ind).element_head)
            {
                old_element = rolling_hash::element(_layers[layer_ind],
                    locator(layer_ind).element_head);
                break;
            }
        }

        //LOG_INFO("NEW layer_ind is " << layer_ind);
    }
    //else
    //    LOG_INFO("locators NOT invalidated");

    //LOG_INFO("root_locator " << root_locator.element_head << " " << 
    //    root_locator.previous_chained_head << " " << 
    //    root_locator.index_location);

    //LOG_INFO("inner_locator " << inner_locator.element_head << " " << 
    //    inner_locator.previous_chained_head << " " << 
    //    inner_locator.index_location);

    if(!old_element.is_null() && layer_ind == 0)
    {
        // previous element is in root layer; take over it's chain-next
        new_head->set_hash_chain_next(old_element.head()->hash_chain_next());
    }

    // write new element key & value
    rolling_hash::element new_element(_layers[0], new_head, key);
    write_record_with_cached_sizes(local_record, new_element);

    // insert new element into root index
    _layers[0]->update_hash_chain(root_locator,
        _layers[0]->packet_offset(new_head));

    // remove previous element
    if(!old_element.is_null())
    {
        if(layer_ind != 0)
        {
            // previous element is in non-root layer; drop from layer index
            _layers[layer_ind]->drop_from_hash_chain(inner_locator);
        }
        old_element.set_dead();
    }

    put_callback(boost::system::error_code(), result);
}

void persister::step_iterator(iterator & it)
{
	// already at layer end? step to next layer
	if(!it.packet)
    {
        while(!it.packet && it.layer_ind)
        {
            it.packet = _layers[--it.layer_ind]->head();
        }
        if(!it.packet || !it.packet->is_dead())
        {
        	return;
        }
        // otherwise, need to step more deeply into layer below
    }

    do
    {
    	it.packet = _layers[it.layer_ind]->next_packet(it.packet);

        // reached layer end? step to next layer
    	if(!it.packet)
        {
            step_iterator(it);
            return;
        }
    } while(it.packet->continues_sequence() || it.packet->is_dead());
}

template<typename PreRotateLambda>
uint32_t persister::top_down_compaction(const PreRotateLambda & pre_rotate_lambda)
{
    for(size_t layer_ind = 0; layer_ind + 1 != _layers.size(); ++layer_ind)
    {
        uint32_t compacted_bytes = inner_compaction(layer_ind, pre_rotate_lambda);

    	// stop on the first successful compaction
        if(compacted_bytes)
            return compacted_bytes;
    }
    return leaf_compaction(pre_rotate_lambda);
}

void persister::on_bottom_up_compaction(
    const bottom_up_compaction_callback_t & callback)
{
	auto noop_lambda = [](size_t) {};

    leaf_compaction(noop_lambda);
    for(size_t layer_ind = _layers.size() - 1; layer_ind; --layer_ind)
    {
        inner_compaction(layer_ind - 1, noop_lambda);
    }
    callback();
}

template<typename PreRotateLambda>
uint32_t persister::inner_compaction(size_t layer_ind,
    const PreRotateLambda & pre_rotate_lambda)
{
    SAMOA_ASSERT(layer_ind + 1 != _layers.size());

    rolling_hash::hash_ring & layer = *_layers[layer_ind];
    rolling_hash::packet * head = layer.head();

    if(!head)
    {
    	return 0;
    }

    uint32_t compacted_bytes;

    if(head->is_dead())
    {
        compacted_bytes = layer.reclaim_head();

        // update any iterators pointing at the old head 
        spinlock::guard guard(_iterators_lock);

        for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
        {
        	if(it->packet == head)
        		it->packet = layer.head();
        }
        return compacted_bytes;
    }

    // element is live, and must be 'spilled' down to the next layer
	rolling_hash::element element(&layer, head);
    pre_rotate_lambda(layer_ind);

    uint32_t key_length = element.key_length();
    auto key_begin = rolling_hash::key_gather_iterator(element);
    auto key_end = rolling_hash::key_gather_iterator();

    uint32_t value_length = element.value_length();
    auto value_begin = rolling_hash::value_gather_iterator(element);

    rolling_hash::hash_ring & next_layer = *_layers[layer_ind + 1];
    rolling_hash::packet * next_head = next_layer.allocate_packets(
        key_length + value_length);

    if(!next_head)
    {
        // not enough space in next layer; compaction fails
        return 0;
    }

    // query for locator in next layer's index
    rolling_hash::hash_ring::locator next_locator = \
        next_layer.locate_key(key_begin, key_end);

    // no live element should exist under this key
    SAMOA_ASSERT(next_locator.element_head == nullptr);

    // fill key, value, and empty hash-chain-next
    rolling_hash::element next_element(&next_layer, next_head,
        key_length, key_begin, value_length, value_begin);

    // insert into next layer's index
    next_layer.update_hash_chain(next_locator,
        next_layer.packet_offset(next_head));

    // query for locator in the current layer's index
    rolling_hash::hash_ring::locator locator = \
        layer.locate_key(key_begin, key_end);

    // remove from current layer index; kill & reclaim head
    layer.drop_from_hash_chain(locator);
    element.set_dead();

    compacted_bytes = layer.reclaim_head();

    // update any iterators pointing at old head
    spinlock::guard guard(_iterators_lock);

    for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
    {
        if(it->packet == head)
        {
            it->layer_ind = layer_ind + 1;
            it->packet = next_head;
        }
    }
    return compacted_bytes;
}

template<typename PreRotateLambda>
uint32_t persister::leaf_compaction(const PreRotateLambda & pre_rotate_lambda)
{
    rolling_hash::hash_ring & layer = *_layers.back();
    rolling_hash::packet * head = layer.head();

    if(!head)
    {
    	return 0;
    }

    uint32_t compacted_bytes;

    if(head->is_dead())
    {
        compacted_bytes = layer.reclaim_head();
    }
    else
    {
    	rolling_hash::element element(&layer, head);
        pre_rotate_lambda(_layers.size() - 1);

        request::state::ptr_t rstate = boost::make_shared<request::state>();

        // extract key & parse protobuf record
        rstate->set_key(std::string(
            rolling_hash::key_gather_iterator(element),
            rolling_hash::key_gather_iterator()));

        rolling_hash::value_zci_adapter zci_adapter(element);
        SAMOA_ASSERT(rstate->get_local_record(
            ).ParseFromZeroCopyStream(&zci_adapter));

        rolling_hash::hash_ring::locator locator = \
            layer.locate_key(rstate->get_key());

        uint32_t hash_chain_next = head->hash_chain_next();

        // drop and reclaim the element
        element.set_dead();
        compacted_bytes = layer.reclaim_head();

        bool upkeep_result = true;
        {
            spinlock::guard guard(_record_upkeep_lock);

            if(_record_upkeep && !_record_upkeep(rstate))
                upkeep_result = false;
        }

        if(!upkeep_result)
        {
            // record should be discarded
            layer.drop_from_hash_chain(locator);
        }
        else
        {
            SAMOA_ASSERT(rstate->get_local_record().IsInitialized());

            // record should be re-written at ring tail
            uint32_t capacity = rstate->get_key().size() + \
                rstate->get_local_record().ByteSize();

            // allocation will always succeed, so long as record
            //  upkeep maintains or shrinks serialized value length
            rolling_hash::packet * new_head = layer.allocate_packets(capacity);
            SAMOA_ASSERT(new_head);

            // write key & value, and update hash chain
            rolling_hash::element new_element(&layer,
                new_head, rstate->get_key());

            new_head->set_hash_chain_next(hash_chain_next);

            write_record_with_cached_sizes(
                rstate->get_local_record(), new_element);

            layer.update_hash_chain(locator, layer.packet_offset(new_head));
        }
    }

    {
        // update any iterators pointing at old head
        spinlock::guard guard(_iterators_lock);

        for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
        {
        	if(it->packet == head)
        		it->packet = layer.head();
        }
    }
    return compacted_bytes;
}

void persister::write_record_with_cached_sizes(
    const spb::PersistedRecord & record,
    rolling_hash::element & element)
{
    rolling_hash::value_zco_adapter zco_adapter(element);

    bool result;
    {
        google::protobuf::io::CodedOutputStream co_stream(&zco_adapter);
        record.SerializeWithCachedSizes(&co_stream);
        result = co_stream.HadError();
        // co_stream dtor called; flushes to zco_adapter
    }
    zco_adapter.finish();
    SAMOA_ASSERT(!result);
}

}
}

