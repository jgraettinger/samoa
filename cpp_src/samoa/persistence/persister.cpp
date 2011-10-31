
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/heap_rolling_hash.hpp"
#include "samoa/persistence/mapped_rolling_hash.hpp"
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

void persister::add_heap_hash(
    size_t storage_size,
    size_t index_size)
{
    LOG_DBG("persister " << this << " adding heap hash {"
        << storage_size << ", " << index_size << "}");

    _layers.push_back(new heap_rolling_hash(storage_size, index_size));
}

void persister::add_mapped_hash(
    const std::string & file,
    size_t storage_size,
    size_t index_size)
{
    LOG_DBG("persister " << this << " adding mapped hash {"
        << file << ", " << storage_size << ", " << index_size << "}");

    _layers.push_back(mapped_rolling_hash::open(
        file, storage_size, index_size).release());
}

void persister::get(
    get_callback_t && callback,
    const std::string & key,
    spb::PersistedRecord & precord)
{
    _strand.post(
        boost::bind(&persister::on_get,
            shared_from_this(),
            std::move(callback),
            boost::cref(key),
            boost::ref(precord)));
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

void persister::drop(
    drop_callback_t && callback,
    const std::string & key,
    spb::PersistedRecord & precord)
{
    _strand.post(
        boost::bind(&persister::on_drop,
            shared_from_this(),
            std::move(callback),
            boost::cref(key),
            boost::ref(precord)));
}

unsigned persister::begin_iteration()
{
    spinlock::guard guard(_iterators_lock);

    unsigned i = 0;
    for(; i != _iterators.size(); ++i)
    {
        if(_iterators[i].state == iterator::DEAD)
        {
            _iterators[i].state = iterator::BEGIN;
            _iterators[i].layer = 0;
            _iterators[i].rec = 0; 
            return i;
        }
    }

    // no iterator slots available? add one
    if(i == _iterators.size())
    {
        _iterators.push_back(iterator({iterator::BEGIN, 0, 0}));
    }
    return i;
}

bool persister::iterate(iterate_callback_t && callback, unsigned ticket)
{
    spinlock::guard guard(_iterators_lock);

    SAMOA_ASSERT(_iterators.at(ticket).state != iterator::DEAD);

    if(_iterators.at(ticket).state == iterator::END)
    {
        _iterators[ticket].state = iterator::DEAD;

        if(ticket + 1 == _iterators.size())
            _iterators.pop_back();

        return false;
    }

    _strand.post(
        boost::bind(&persister::on_iterate,
            shared_from_this(),
            std::move(callback),
            ticket));

    return true;
}

const rolling_hash & persister::get_layer(size_t index) const
{ return *_layers.at(index); }

void persister::on_get(
    const get_callback_t & callback,
    const std::string & key,
    spb::PersistedRecord & precord)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        const record * rec = _layers[i]->get(key.begin(), key.end());

        if(!rec) continue;

        SAMOA_ASSERT(precord.ParseFromArray(
            rec->value_begin(), rec->value_length()));

        callback(true);
        return;
    }
    callback(false);
}

void persister::on_put(
    const put_callback_t & put_callback,
    const datamodel::merge_func_t & merge_func,
    const std::string & key,
    const spb::PersistedRecord & remote_precord,
    spb::PersistedRecord & local_precord)
{
    unsigned value_length = remote_precord.ByteSize();

    // garden path result
    datamodel::merge_result result;
    result.local_was_updated = true;
    result.remote_is_stale = false;

    // first, find a previous record instance
    size_t cur_layer = 0;
    rolling_hash::offset_t root_hint = 0, cur_hint = 0;
    const record * rec = _layers[0]->get(key.begin(), key.end(), &root_hint);

    while(!rec && ++cur_layer != _layers.size())
    {
        rec = _layers[cur_layer]->get(key.begin(), key.end(), &cur_hint);
    }

    if(rec)
    {
        // provision a value size equal to the length of the
        //  update, _plus_ the length of the old record
        value_length += rec->value_length();
    }

    if(make_room(key.length(), value_length, root_hint, cur_hint, cur_layer))
    {
        // while making room, we invalidated the previously found hints,
        //  and we need to find them again

        cur_layer = 0;
        rec = _layers[0]->get(key.begin(), key.end(), &root_hint);

        while(!rec && ++cur_layer != _layers.size())
        {
            rec = _layers[cur_layer]->get(key.begin(), key.end(), &cur_hint);
        }
    }

    if(!_layers[0]->would_fit(key.length(), value_length))
    {
        // won't fit? return error to caller
        put_callback(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory),
            result);
        return;
    }

    record * new_rec = _layers[0]->prepare_record(
        key.begin(), key.end(), value_length);

    if(rec)
    {
        // a previous record exists under this key;
        //  give caller the opportunity to merge them

        SAMOA_ASSERT(local_precord.ParseFromArray(
            rec->value_begin(), rec->value_length()));

        result = merge_func(local_precord, remote_precord);

        if(!result.local_was_updated)
        {
            // merge-callback aborted the write
            put_callback(boost::system::error_code(), result);
            return;
        }
    }
    else
    {
        // no existing record; directly copy remote_record
        local_precord.CopyFrom(remote_precord);
    }

    // re-compute local record length
    value_length = local_precord.ByteSize();
    SAMOA_ASSERT(value_length <= new_rec->value_length());

    // write, trim, & commit record
    local_precord.SerializeWithCachedSizesToArray(
        reinterpret_cast<google::protobuf::uint8*>(
            new_rec->value_begin()));

    new_rec->trim_value_length(value_length);
    _layers[0]->commit_record(root_hint);

    if(rec && cur_layer)
    {
        // previous record isn't in top layer: mark old location for collection
        _layers[cur_layer]->mark_for_deletion(key.begin(), key.end(), cur_hint);
    }

    put_callback(boost::system::error_code(), result);
}

void persister::on_drop(
    const drop_callback_t & callback,
    const std::string & key,
    spb::PersistedRecord & precord)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        rolling_hash & layer = *_layers[i];
        rolling_hash::offset_t hint = 0;
        const record * rec = layer.get(key.begin(), key.end(), &hint);

        if(!rec) continue;

        SAMOA_ASSERT(precord.ParseFromArray(
            rec->value_begin(), rec->value_length()));

        if(callback(true))
        {
            layer.mark_for_deletion(key.begin(), key.end(), hint);
            make_room(0, 0, 0, 0, 0);
        }
        return;
    }
    callback(false);
}

void persister::on_iterate(
    const iterate_callback_t & callback,
    size_t ticket)
{
    spinlock::guard guard(_iterators_lock);

    iterator & iter = _iterators[ticket];
    assert(!iter.state == iterator::DEAD);

    if(iter.state == iterator::BEGIN)
    {
        iter.state = iterator::LIVE;
        iter.layer = _layers.size();
        iter.rec = 0;
    }

    const record * next_rec = 0;

    while(!next_rec)
    {
        // reached the end of this layer? begin the next one up
        while(iter.rec == 0 && iter.layer)
            iter.rec = _layers[--iter.layer]->head();

        if(iter.rec == 0)
        {
            // end of sequence
            iter.state = iterator::END;
            break;
        }

        if(!iter.rec->is_dead())
        {
            next_rec = iter.rec;
        }

        iter.rec = _layers[iter.layer]->step(iter.rec);
    }

    callback(next_rec);
}

bool persister::make_room(size_t key_length, size_t val_length,
    record::offset_t root_hint, record::offset_t cur_hint,
    size_t cur_layer)
{
    bool invalid = false;

    size_t cur_rotation = 0;

    auto invalidates_check = [&](size_t layer)
    {
        if(layer == 0 && _layers[0]->head_invalidates(root_hint))
        {
            invalid = true;
        }
        else if(layer == cur_layer && \
            _layers[cur_layer]->head_invalidates(cur_hint))
        {
            invalid = true;
        }
    };

    auto iterator_step = [&](rolling_hash & layer, const record * r)
    {
        spinlock::guard guard(_iterators_lock);
        for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
        {
            if(it->state == iterator::LIVE && it->rec == r)
                it->rec = layer.step(r);
        }
    };

    auto rotate_down = [&](rolling_hash & layer, rolling_hash & next)-> bool
    {
        const record * head = layer.head();

        assert(next.would_fit(head->key_length(), head->value_length()));

        // allocate new record copy at tail of the next layer down
        record * new_rec = next.prepare_record(
            head->key_begin(), head->key_end(), head->value_length());

        std::copy(head->value_begin(), head->value_end(),
            new_rec->value_begin());

        next.commit_record();

        // shift any iterators pointed at head down to
        //  the new record on the lower layer
        {
            spinlock::guard guard(_iterators_lock);
            for(auto it = _iterators.begin(); it != _iterators.end(); ++it)
            {
                if(it->state == iterator::LIVE && it->rec == head)
                {
                    it->layer += 1;
                    it->rec = new_rec;
                }
            }
        }

        // mark original head record as dead, & reclaim
        layer.mark_for_deletion(head->key_begin(), head->key_end());
        layer.reclaim_head();
        return true;
    };

    auto prep_leaf = [&](size_t trg_key, size_t trg_val) -> bool
    {
        rolling_hash & hash = *_layers.back();

        for(const record * head = hash.head(); head &&
            !hash.would_fit(trg_key, trg_val); head = hash.head())
        {
            if(++cur_rotation == _max_rotations)
                return false;

            invalidates_check(_layers.size() - 1);
            iterator_step(hash, head);

            if(head->is_dead())
                hash.reclaim_head();
            else
                hash.rotate_head();
        }
        return true;
    };

    boost::function<bool(size_t, size_t, size_t)> prep_inner;

    auto prep = [&](size_t layer, size_t trg_key, size_t trg_val) -> bool
    {
        if(layer + 1 == _layers.size())
            return prep_leaf(trg_key, trg_val);
        else
            return prep_inner(layer, trg_key, trg_val);
    };

    prep_inner = [&](size_t layer, size_t trg_key, size_t trg_val) -> bool
    {
        rolling_hash & hash = *_layers[layer];

        for(const record * head = hash.head(); head &&
            !hash.would_fit(trg_key, trg_val); head = hash.head())
        {
            if(++cur_rotation == _max_rotations)
                return false;

            if(head->is_dead())
            {
                invalidates_check(layer);
                iterator_step(hash, head);
                hash.reclaim_head();
            }
            else
            {
                // record is live; spill over to the next layer down
                if(!prep(layer + 1, head->key_length(), head->value_length()))
                    return false;

                invalidates_check(layer);
                rotate_down(hash, *_layers[layer+1]);
            }
        }
        return true;
    };

    prep(0, key_length, val_length);

    if(cur_rotation < _min_rotations)
    {
        // require that at least _min_rotations were performed;
        //  if we're not there yet, apply additional maintenance
        //  rotations of the bottom layer
        rolling_hash & hash = *_layers.back();

        for(const record * head = hash.head(); head; head = hash.head())
        {
            if(cur_rotation++ == _min_rotations)
                break;

            invalidates_check(_layers.size() - 1);
            iterator_step(hash, head);

            if(head->is_dead())
                hash.reclaim_head();
            else
                hash.rotate_head();
        }
    }

    return invalid;
}

}
}

