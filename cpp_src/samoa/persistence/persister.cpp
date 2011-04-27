
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/heap_rolling_hash.hpp"
#include "samoa/persistence/mapped_rolling_hash.hpp"
#include <algorithm>

#include <iostream>

namespace samoa {
namespace persistence {

using namespace std;

persister::persister(core::proactor & p)
 : _strand(*p.concurrent_io_service()),
   _max_rotations(5000)
{ }

persister::~persister()
{
    for(size_t i = 0; i != _layers.size(); ++i)
        delete _layers[i];
}

void persister::add_heap_hash(size_t region_size, size_t table_size)
{
    _layers.push_back(new heap_rolling_hash(region_size, table_size));
}

void persister::add_mapped_hash(std::string file,
    size_t region_size, size_t table_size)
{
    _layers.push_back(mapped_rolling_hash::open(
        file, region_size, table_size).release());
}

void persister::get(
    persister::get_callback_t && callback,
    std::string && key)
{
    _strand.post(boost::bind(&persister::on_get, shared_from_this(),
        std::move(key), std::move(callback)));
}

void persister::put(
    persister::put_callback_t && callback,
    std::string && key,
    size_t value_length)
{
    _strand.post(boost::bind(&persister::on_put, shared_from_this(),
        std::move(key), value_length, std::move(callback)));
}

void persister::drop(
    persister::drop_callback_t && callback,
    std::string && key)
{
    _strand.post(boost::bind(&persister::on_drop, shared_from_this(),
        std::move(key), std::move(callback)));
}

const rolling_hash & persister::get_layer(size_t index) const
{ return *_layers.at(index); }

void persister::on_get(const std::string & key,
    const persister::get_callback_t & callback)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        const record * rec = _layers[i]->get(key.begin(), key.end());

        if(!rec) continue;

        callback(boost::system::error_code(), rec);
        return;
    }
    callback(boost::system::error_code(), 0);
}

void persister::on_put(const std::string & key,
    size_t est_value_length,
    const persister::put_callback_t & callback)
{
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
        est_value_length += rec->value_length();
    }

    if(make_room(key.length(), est_value_length, root_hint, cur_hint, cur_layer))
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

    if(!_layers[0]->would_fit(key.length(), est_value_length))
    {
        // won't fit? return error to caller
        callback(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory), 0, 0);
        return;
    }

    record * new_rec = _layers[0]->prepare_record(
        key.begin(), key.end(), est_value_length);

    if(!callback(boost::system::error_code(), rec, new_rec))
    {
        // caller aborted write
        return;
    }

    _layers[0]->commit_record(root_hint);

    if(rec && cur_layer)
    {
        // previous record isn't in top layer: mark old location for collection
        _layers[cur_layer]->mark_for_deletion(key.begin(), key.end(), cur_hint);
    }
}

void persister::on_drop(const std::string & key,
    const persister::drop_callback_t & callback)
{
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        rolling_hash & layer = *_layers[i];
        rolling_hash::offset_t hint = 0;
        const record * rec = layer.get(key.begin(), key.end(), &hint);

        if(!rec) continue;

        bool commit = callback(boost::system::error_code(), rec);

        if(commit)
            layer.mark_for_deletion(key.begin(), key.end(), hint);

        return;
    }
    callback(boost::system::error_code(), 0);
}

bool persister::make_room(size_t key_length, size_t val_length,
    record::offset_t root_hint, record::offset_t cur_hint, size_t cur_layer)
{
    // turn root_hint into a root record?
    // turn found_hint into a found record?
    bool invalid = false;

    size_t cur_rotation = 0;

    auto invalidate = [&](size_t layer, const record * r)
    {
        invalid = true;
    };

    auto iterator_step = [&](rolling_hash & layer, const record * r)
    {
        for(size_t i = 0; i != _iterators.size(); ++i)
        {
            if(_iterators[i].second == r)
                _iterators[i].second = layer.step(r);
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
        for(size_t i = 0; i != _iterators.size(); ++i)
        {
            if(_iterators[i].second != head)
                continue;

            _iterators[i].first += 1;
            _iterators[i].second = new_rec;
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

            invalidate(_layers.size() - 1, head);
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
                invalidate(layer, head);
                iterator_step(hash, head);
                hash.reclaim_head();
            }
            else
            {
                // record is live, and needs to spill over to the next layer down
                if(!prep(layer + 1, head->key_length(), head->value_length()))
                    return false;

                invalidate(layer, head);
                rotate_down(hash, *_layers[layer+1]);
            }
        }
        return true;
    };

    prep(0, key_length, val_length);
    return invalid;
}

}
}

