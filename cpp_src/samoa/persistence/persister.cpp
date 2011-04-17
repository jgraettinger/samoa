
#include "samoa/persistence/persister.hpp"
#include "samoa/persistence/rolling_hash.hpp"
#include "samoa/persistence/heap_rolling_hash.hpp"
#include "samoa/persistence/mapped_rolling_hash.hpp"
#include <algorithm>

#include <iostream>

namespace samoa {
namespace persistence {

using namespace std;

float overflow_threshold = 0.95;

persister::persister(core::proactor & p)
 : _strand(*p.concurrent_io_service()),
   _max_rotations(50)
{ }

persister::~persister()
{
    for(unsigned i = 0; i != _layers.size(); ++i)
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
    unsigned value_length)
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

const rolling_hash & persister::get_layer(unsigned index) const
{ return *_layers.at(index); }

void persister::on_get(const std::string & key,
    const persister::get_callback_t & callback)
{
    for(unsigned i = 0; i != _layers.size(); ++i)
    {
        const record * rec = _layers[i]->get(key.begin(), key.end());

        if(!rec) continue;

        callback(boost::system::error_code(), rec);
        return;
    }
    callback(boost::system::error_code(), 0);
}

void persister::on_put(const std::string & key,
    unsigned value_length,
    const persister::put_callback_t & callback)
{
    upkeep(record::allocated_size(key.length(), value_length));

    // first, find a previous record instance
    unsigned layer = 0;
    rolling_hash::offset_t root_hint = 0, found_hint = 0;
    const record * rec = _layers[0]->get(key.begin(), key.end(), &root_hint);

    while(!rec && ++layer != _layers.size())
    {
        rec = _layers[layer]->get(key.begin(), key.end(), &found_hint);
    }

    if(rec)
    {
        // provision a value size equal to the length of the
        //  update, _plus_ the length of the old record
        value_length += rec->value_length();
    }

    if(!_layers[0]->would_fit(key.length(), value_length))
    {
        // won't fit? return error to caller
        callback(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory), 0, 0);
        return;
    }

    record * new_rec = _layers[0]->prepare_record(
        key.begin(), key.end(), value_length);

    if(!callback(boost::system::error_code(), rec, new_rec))
    {
        // caller aborted write
        return;
    }

    _layers[0]->commit_record(root_hint);

    if(rec && layer)
    {
        // previous record isn't in top layer: mark old location for collection
        _layers[layer]->mark_for_deletion(key.begin(), key.end(), found_hint);
    }
}

void persister::on_drop(const std::string & key,
    const persister::drop_callback_t & callback)
{
    upkeep(0);

    for(unsigned i = 0; i != _layers.size(); ++i)
    {
        rolling_hash & layer = *_layers.back();
        rolling_hash::offset_t hint = 0;
        const record * rec = layer.get(key.begin(), key.end(), &hint);

        if(!rec) continue;

        bool commit = callback(boost::system::error_code(), rec);

        if(commit)
            _layers[i]->mark_for_deletion(key.begin(), key.end(), hint);

        return;
    }
    callback(boost::system::error_code(), 0);
}

void persister::upkeep(size_t target_size)
{
    // target size is max of the write target, or the largest
    //  head of any layer's ring
    for(size_t i = 0; i != _layers.size(); ++i)
    {
        const record * head = _layers[i]->head();

        if(!head) continue;

        target_size = std::max<size_t>(target_size,
            record::allocated_size(head->key_length(), head->value_length()));
    }

    upkeep_leaf(*_layers.back(), target_size);

    for(size_t ind = _layers.size() - 1; ind-- != 0;)
    {
        upkeep_inner(*_layers[ind], *_layers[ind+1], target_size);
    }
}

void persister::upkeep_leaf(rolling_hash & layer, size_t target_size)
{
    // Leaf layer update:
    //  - rotate/reclaim [1, _max_rotations] heads, until
    //     a write of target_size would succeed
    //  - proactively reclaim up to max_rotations dead ring heads
    for(size_t r = 0; r != _max_rotations; ++r)
    {
        const record * head = layer.head();

        if(!head)
        {
            // layer is empty
            break;
        }

        if(r && !head->is_dead() && layer.would_fit(target_size))
        {
            // we've rotated/reclaimed at least one head,
            //  the current one is live, but we've already
            //  got enough space for an immediate write
            break;
        }

        // step any iterators pointing at head
        for(unsigned i = 0; i != _iterators.size(); ++i)
        {
            if(_iterators[i].second == head)
                _iterators[i].second = layer.step(head);
        }

        if(head->is_dead())
            layer.reclaim_head();
        else
            layer.rotate_head();
    }
}

void persister::upkeep_inner(rolling_hash & layer,
    rolling_hash & next_layer, size_t target_size)
{
    // Inner layer update:
    //  - overflow record heads until a write of target_size would succeed
    //  - proactively reclaim up to max_rotations dead ring heads

    for(size_t r = 0; r != _max_rotations; ++r)
    {
        const record * head = layer.head();

        if(!head)
        {
            // layer is empty
            break;
        }

        if(!head->is_dead() && layer.would_fit(target_size))
        {
            // the current head is live, but we've already
            //   got enough space for an immediate write
            break;
        }

        if(head->is_dead())
        {
            // dead head: step iterators
            for(unsigned i = 0; i != _iterators.size(); ++i)
            {
                if(_iterators[i].second == head)
                    _iterators[i].second = layer.step(head);
            }
        }
        else
        {
            // record is live, and needs to spill over to the next layer down

            // won't fit? bail out
            if(!next_layer.would_fit(head->key_length(), head->value_length()))
                break;

            // allocate new record copy at tail of the next layer down
            record * new_rec = next_layer.prepare_record(
                head->key_begin(), head->key_end(), head->value_length());

            std::copy(head->value_begin(), head->value_end(),
                new_rec->value_begin());

            next_layer.commit_record();

            // shift any iterators pointed at head down to
            //  the new record on the lower layer
            for(unsigned i = 0; i != _iterators.size(); ++i)
            {
                if(_iterators[i].second != head)
                    continue;

                _iterators[i].first += 1;
                _iterators[i].second = new_rec;
            }

            // mark original head record as dead
            layer.mark_for_deletion(head->key_begin(), head->key_end());
        }

        // if head wasn't dead, it is now
        layer.reclaim_head();
    }
}

}
}

