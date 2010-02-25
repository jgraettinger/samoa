
#include "samoa/partition.hpp"
#include "samoa/request.hpp"
#include "samoa/client.hpp"
#include "samoa/fwd.hpp"
#include "common/mapped_rolling_hash.hpp"
#include "common/buffer_region.hpp"

namespace samoa {

const size_t RECORDS_PER_ITERATION = 2;

namespace {
    common::const_buffer_region _resp_newline("\r\n");
    common::const_buffer_region _resp_hit("HIT ");
    common::const_buffer_region _resp_miss("MISS\r\n");
    common::const_buffer_region _resp_ok("OK\r\n");
    common::const_buffer_region _resp_err_no_space("-ERR insufficient space\r\n");
};

typedef common::mapped_rolling_hash<
    rolling_hash_t::offset_bytes,
    rolling_hash_t::key_len_bytes,
    rolling_hash_t::val_len_bytes,
    rolling_hash_t::expire_bytes
> mapped_rolling_hash_t;

partition::partition(const std::string & file, size_t region_size, size_t table_size)
 : _table( mapped_rolling_hash_t::open_mapped_rolling_hash(file, region_size, table_size))
{
}

void partition::handle_request(const client_ptr_t & client)
{
    request & req = client->get_request();
    
    if(req.req_type == REQ_GET || req.req_type == REQ_FGET)
    {
        size_t hint;
        rolling_hash_t::record * rec;
        _table->get(req.key.begin(), req.key.end(), rec, hint);
        
        if(rec)
        {
            const char * val = rec->value();
            size_t val_len = rec->value_length();
            
            char buf[21];
            sprintf(buf, "%lu", val_len);
            
            client->queue_write(_resp_hit);
            client->queue_write(buf); 
            client->queue_write(_resp_newline);
            client->queue_write(val, val + val_len);
            client->queue_write(_resp_newline);
        }
        else
        {
            client->queue_write(_resp_miss);
        }
    }
    else if(req.req_type == REQ_SET)
    {
        // step any iterating clients which are on head
        for(std::set<client_ptr_t>::iterator it = _iterating_clients.begin();
            it != _iterating_clients.end(); ++it)
        {
            const mapped_rolling_hash_t::record * & cur_rec = (*it)->get_request().cur_rec;
            
            if(cur_rec == _table->head())
                cur_rec = _table->step(cur_rec);
        }
        
        if(!_table->migrate_head())
            _table->drop_head();
        
        if(_table->would_fit(req.key.size(), req.req_data_length))
        {
            _table->set(
                req.key.begin(), req.key.end(),
                common::buffers_iterator_t::begin(req.req_data),
                common::buffers_iterator_t::end(req.req_data));
            
            client->queue_write(_resp_ok);
        }
        else
            client->queue_write(_resp_err_no_space);
    }
    else if(req.req_type == REQ_ITER_KEYS)
    {
        req.cur_rec = _table->head();
        client->queue_write( _resp_ok);
        _iterating_clients.insert(client);
        on_iteration(boost::system::error_code(), client);
        return;
    }
    
    client->finish_response();
    return;
}

void partition::on_iteration(
    const boost::system::error_code & ec,
    const client_ptr_t & client)
{
    if(ec)
    {
        std::cerr << "error on_iteration" << std::endl;
        _iterating_clients.erase(client);
        return;
    }
    
    request & req = client->get_request();
    
    if(!req.cur_rec)
    {
        client->queue_write(_resp_ok);
        client->finish_response();
        _iterating_clients.erase(client);
        return;
    }
    
    for(size_t i = 0; i != RECORDS_PER_ITERATION && req.cur_rec; ++i)
    {
        if(req.cur_rec->is_dead())
        {
            req.cur_rec = _table->step(req.cur_rec);
            continue;
        }
        
        const char * key = req.cur_rec->key();
        size_t key_len = req.cur_rec->key_length();
        
        const char * val = req.cur_rec->value();
        size_t val_len = req.cur_rec->value_length();
        
        char buf[32];
        sprintf(buf, " %lu\r\n", val_len);
        
        client->queue_write(key, key + key_len);
        client->queue_write(buf); 
        client->queue_write(val, val + val_len);
        client->queue_write(_resp_newline);
        
        req.cur_rec = _table->step(req.cur_rec);
    }
    
    client->write_queued( boost::bind(
        &partition::on_iteration,
        shared_from_this(),
        _1,
        client
    ));
    return;
}

};

