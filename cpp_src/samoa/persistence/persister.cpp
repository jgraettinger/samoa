
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash/heap_hash_ring.hpp"
#include "samoa/persistence/rolling_hash/mapped_hash_ring.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/log.hpp"
#include <boost/bind.hpp>
#include <algorithm>

namespace samoa {
namespace persistence {

using namespace std;

persister::persister()
 : _proactor(core::proactor::get_proactor()),
   _strand(*_proactor->concurrent_io_service()),
   _min_rotations(2),
   _max_rotations(10)
{}

persister::~persister()
{
    LOG_DBG("persister " << this);

    for(size_t i = 0; i != _layers.size(); ++i)
        delete _layers[i];
}

const rolling_hash::hash_ring & persister::get_layer(size_t index) const
{ return *_layers.at(index); }

void persister::add_heap_hash(unsigned storage_size, unsigned index_size)
{
    LOG_DBG("persister " << this << " adding heap hash {"
        << storage_size << ", " << index_size << "}");

    _layers.push_back(rolling_hash::heap_hash_ring::open(
        storage_size, index_size).release());
}

void persister::add_mapped_hash(const std::string & file,
    size_t storage_size, size_t index_size)
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

unsigned persister::iteration_begin()
{
    spinlock::guard guard(_iterators_lock);

    unsigned i = 0;
    for(; i != _iterators.size(); ++i)
    {
    	iterator & it = _iterators[i];
        if(it.state == iterator::DEAD)
        {
            it.state = iterator::IDLE;
            it.layer = _layers.size() - 1;
            it.packet = nullptr;

            step_iterator(it);
            return i;
        }
    }

    // no iterator slots available? add one
    if(i == _iterators.size())
    {
        _iterators.push_back({iterator::IDLE, _layers.size(), nullptr});
    }
    return i;
}

void persister::finish_iteration(unsigned ticket)
{
    spinlock::guard guard(_iterators_lock);

    SAMOA_ASSERT(_iterators.at(ticket).state != iterator::POSTED);
    _iterators[ticket].state = iterator::DEAD;
}

bool persister::iteration_next(iterate_callback_t && callback, unsigned ticket)
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
    return true;
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

void persister::on_get(
    const get_callback_t & callback,
    const std::string & key,
    spb::PersistedRecord & record)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        rolling_hash::hash_ring::locator loc = _layers[i]->locate_key(key);

        if(!loc->element_head) continue;

        rolling_hash::element element(_layers[i], loc->element_head);

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

        if(!loc->element_head) continue;

        rolling_hash::element element(_layers[i], loc->element_head);
        rolling_hash::value_zci_adapter zci_adapter(element);

        SAMOA_ASSERT(record.ParseFromZeroCopyStream(&zci_adapter));

        // commit drop?
        if(callback(true))
        {
            element->set_dead();
            compaction();
        }
        return;
    }
    callback(false);
}

void persister::step_iterator(iterator & it)
{
	// already at layer end? step to next layer
	if(!it.packet)
    {
        for(; it.layer && !it.packet; --it.layer)
        {
    	    it.packet = _layers[it.layer]->head();
        }
        return;
    }

    while(it.packet && it.packet->continues_sequence())
    {
        it.packet = _layers[it.layer]->next_packet(it.packet);
    }

    // reached layer end? step to next layer
    if(!it.packet)
    {
        step_iterator(it);
    }
}

void persister::on_iteration_next(
    const iterate_callback_t & callback,
    size_t ticket)
{
    rolling_hash::element element;

    {
        spinlock::guard guard(_iterators_lock);
        iterator & it = _iterators[ticket];

        unsigned layer = it.layer;
        rolling_hash::packet * result = it.packet;

        next_element(it);

        if(!result)
        {
        	// we're returning a null element via callback;
        	//  the iteration ticket is implicitly released
            it.state = iterator::DEAD;
        }
        else
        {
            element = rolling_hash::element(_layers[layer], result);
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
    auto locator = [&locators](unsigned layer) \
        -> rolling_hash::hash_ring::locator &
    { return layer ? inner_locator : root_locator; }

    // first, attempt to find a previous record instance
    unsigned layer = 0;
    for(; !locator(layer).element_head; ++layer)
    {
        locator(layer) = _layers[layer]->locate_key(key);
    }

    rolling_hash::element cur_element;

    if(locator(layer).element_head)
    {
        // an element exists under this key; parse into local_record
        cur_element = rolling_hash::element(_layers[layer],
            locator(layer).element_head);

        rolling_hash::value_zci_adapter zci_adapter(element);
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

    uint32_t required_capacity = key.size() + local_record.ByteSize();

    // do we have enough space to write over the old value, in-place?
    if(element.capacity() >= required_capacity)
    {
        // we've enough space to write over the old value, in-place
        rolling_hash::value_zco_adapter zco_adapter(element);
        google::protobuf::io::CodedOutputStream co_stream(&zco_adapter);
        SAMOA_ASSERT(local_record.SerializeWithCachedSizes(&co_stream));

        zco_adapter.finish();
        return;
    }

    // A closure (passed to allocate_root_element()) which tracks whether
    //   a layer rotation invalidates either our root or inner locator
    //   (and therefore necessitates a second lookup of the key)
    bool locators_invalidated = false;

    auto invalidates_check = [&](rolling_hash::packet * head)
    {
        if(head == locator(layer).element_head ||
           head == locator(layer).previous_chained_head)
        {
            locators_invalidated = true;
        }
    }

    rolling_hash::packet * head = allocate_root_element(
        required_capacity, invalidates_check);

    if(!head)
    {
        // insufficient space; return error to caller
        put_callback(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory),
            result);
        return;
    }

    if(locators_invalidated)
    {
        // compaction invalidated our locators; re-query for key
        for(layer = 0; !locator(layer).element_head; ++layer)
        {
            locator(layer) = _layers[layer]->locate_key(key);
        }
    }

    // write new element into root layer
    rolling_hash::element new_element(_layers[layer], head, key);

    rolling_hash::value_zco_adapter zco_adapter(element);
    google::protobuf::io::CodedOutputStream co_stream(&zco_adapter);
    SAMOA_ASSERT(local_record.SerializeWithCachedSizes(&co_stream));

    if(!cur_element.is_null())
    {
        if(layer != 0)
        {
            // previous element is in non-root layer; drop from index
            _layers[layer]->drop_from_hash_chain(inner_locator);
        }
        else
        {
            // previous element is in root layer; take over it's chain-next
            head->set_hash_chain_next(cur_element.head().hash_chain_next());
        }
        cur_element.set_dead();
    }

    // insert new element into root index
    _layers[0]->update_hash_chain(root_locator,
        _layers[0]->packet_offset(head));

    put_callback(boost::system::error_code(), result);
}

template<typename PreRotateLambda>
rolling_hash::packet * persister::allocate_root_element(
    uint32_t required_capacity, const PreRotateLambda & pre_rotate_lambda)
{
    rolling_hash::packet * new_head = nullptr;
    for(unsigned i = 0; i != max_compactions && !head; ++i)
    {
    	if(_layers[0]->head())
            top_down_compaction(0, pre_rotate_lambda);

        new_head = _layers[0]->allocate_packets(required_capacity);
    }
    return head;
}

template<typename PreRotateLambda>
void persister::top_down_compaction(unsigned layer_ind,
    const PreRotateLambda & pre_rotate_lambda)
{
	if(layer_ind + 1 == _layers.size())
    {
        leaf_compaction(pre_rotate_lambda);
        return;
    }

    rolling_hash::ring * layer = _layers[layer_ind];
    rolling_hash::packet * head = layer->head();

    SAMOA_ASSERT(head);

    if(head->is_dead())
    {
        layer->reclaim_head();

        // update any iterators pointing at the old head 
        spinlock::guard guard(_iterators_lock);

        for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
        {
        	if(it->packet == head)
        		it->packet = layer->head();
        }
    }
    else
    {
        // element is live, and must be 'spilled' down to the next layer
    	rolling_hash::element element(layer, head);
        pre_rotate_lambda(head);

        // attempt to write element at tail of next layer down
        rolling_hash::packet * new_head = put_key_value(
            _layers[layer_ind + 1],
            element.key_length(),
            rolling_hash::key_gather_iterator(element),
            element.value_length(),
            rolling_hash::value_gather_iterator(element));

        if(!new_head)
        {
            // not enough space; recurse
            top_down_compaction(layer_ind + 1, pre_rotate_lambda);
            return;
        }

        // query for key, and drop from layer index
        rolling_hash::hash_ring::locator locator = layer->locate_key(
            rolling_hash::key_gather_iterator(element),
            rolling_hash::key_gather_iterator());

        layer->drop_from_hash_chain(locator);

        // reclaim head
        element.set_dead();
        layer->reclaim_head();

        // update any iterators pointing at old head
        spinlock::guard guard(_iterators_lock);

        for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
        {
            if(it->packet == head)
            {
                it->layer += 1;
                it->packet = new_head;
            }
        }
    }
}

template<typename PreRotateLambda>
void persister::leaf_compaction(const PreRotateLambda & pre_rotate_lambda)
{
    rolling_hash::ring & layer = *_layers.back();
    rolling_hash::packet * head = layer->head();

    if(head->is_dead())
    {
        layer.reclaim_head();
        return;
    }

	rolling_hash::element element(layer, head);
    pre_rotate_lambda(head);

    request::state::ptr_t rstate = boost::make_shared<request::state>();

    // extract key & parse protobuf record
    rstate->set_key(std::string(
        rolling_hash::key_gather_iterator(element),
        rolling_hash::key_gather_iterator()));

    rolling_hash::value_zci_adapter zci_adapter(element);
    SAMOA_ASSERT(rstate->get_local_record(
        ).ParseFromZeroCopyStream(&zci_adapter));

    rolling_hash::hash_ring::locator locator = \
        layer.locate_key(rstate.key());

    // drop and reclaim the element
    element.set_dead();
    layer.reclaim_head();

    if(!_record_upkeep(rstate))
    {
        // record should be discarded
        layer.drop_from_hash_chain(locator);
    }
    else
    {
        // record should be re-written at ring tail
        uint32_t capacity = rstate->key().size() + \
            rstate->local_record().ByteSize();

        // allocation will always succeed, so long as record
        //  upkeep maintains or shrinks serialized value length
        rolling_hash::packet * new_head = layer.allocate_packets(capacity);
        SAMOA_ASSERT(new_head);

        // write key & value, and update hash chain
        rolling_hash::element new_element(layer, new_head, rstate->key());

        rolling_hash::value_zco_adapter zco_adapter(element);
        google::protobuf::io::CodedOutputStream co_stream(&zco_adapter);
        SAMOA_ASSERT(rstate->local_record(
            ).SerializeWithCachedSizes(&co_stream));

        layer.update_hash_chain(locator, layer->packet_offset(new_head));
    }
}


rolling_hash::packet * persister::put_key_value(
    rolling_hash::hash_ring & layer,
    uint32_t key_length, const rolling_hash::key_gather_iterator & key_it,
    uint32_t val_length, const rolling_hash::value_gather_iterator & val_it)
{
    rolling_hash::packet * head = layer->allocate_packets(
        key_length + val_length);

    if(!head)
    	return nullptr;

    rolling_hash::hash_ring::locator locator = layer->locate_key(
        key_it, rolling_hash::key_gather_iterator());

    // take over the previous key location's chain-next,
    //   if a previous key location exists
    uint32_t hash_chain_next = locator.element_head ? \
        locator.element_head->hash_chain_next() : 0;

    // fill key, value, and chain-next
    rolling_hash::element element(&layer, head,
        key_length, key_it, val_length, val_it, hash_chain_next);

    layer.update_hash_chain(locator, layer.packet_offset(head));

    if(locator.element_head)
    {
        // mark replaced element as dead
    	rolling_hash::element(layer, locator.element_head).set_dead();
    }
    return head;
}

}
}

