
#include "samoa/client.hpp"
#include "samoa/server.hpp"
#include "common/stream_protocol.hpp"
#include "common/ref_buffer.hpp"
#include <boost/regex.hpp>
#include <boost/bind.hpp>
#include <string.h>
#include <map>

namespace samoa {

using boost::asio::ip::tcp;
using namespace common;

// Common member-method signature for command handlers
typedef void (client::*command_handler_t)(
    const stream_protocol::match_results_t &);

// Regex's for parsing client commands
std::vector<boost::regex> _cmd_re;
// Handlers for regex matches, of same arity & order as _cmd_re
std::vector<command_handler_t> _cmd_handler;

// Canned responses
const_buffer_region _resp_newline("\r\n");
const_buffer_region _resp_ok_version("SAMOA v0.1\r\n");
const_buffer_region _resp_ok_pong("PONG\r\n");
const_buffer_region _resp_ok_set("OK\r\n");
const_buffer_region _resp_ok_get_hit("hit ");
const_buffer_region _resp_ok_get_miss("miss\r\n");
const_buffer_region _resp_err_no_space("-ERR insufficient space\r\n");
const_buffer_region _resp_err_cmd_malformed("-ERR command malformed\r\n");
const_buffer_region _resp_err_cmd_overflow("-ERR command overflow\r\n");
const_buffer_region _resp_err_data_malformed("-ERR data malformed\r\n");
const_buffer_region _resp_err_data_overflow("-ERR data overflow\r\n");

client::client(
	const server::ptr_t & server,
    std::auto_ptr<tcp::socket> sock
) : 
    common::stream_protocol(sock),
    _server(server),
    _close_pending(false)
{ }

void client::init()
{
    if(_cmd_re.empty())
        build_command_table();
    
    _request.reset( new request(shared_from_this()));
    
    _request->resp_type = RESP_OK_VERSION;
    finish_response();
    return;
}

void client::on_ping(const stream_protocol::match_results_t & m)
{
    _request->req_type  = REQ_PING;
    _request->resp_type = RESP_OK_PONG;
    finish_response();
    return;
}
void client::on_shutdown(const stream_protocol::match_results_t & m)
{
    _server->shutdown();
    // don't finish response, starting another read
    return;
}
void client::on_malformed(const stream_protocol::match_results_t & m)
{
    _request->req_type  = REQ_INVALID;
    _request->resp_type = RESP_ERR_CMD_MALFORMED;
    finish_response();
    return;
}
void client::on_get(const stream_protocol::match_results_t & m)
{
    _request->req_type = distance(m[1].first, m[1].second) == 3 ? \
        REQ_GET : REQ_FGET;
    
    _request->key.resize( distance(m[2].first, m[2].second));
    copy( m[2].first, m[2].second, _request->key.begin());
    
    // DISPATCH 
    
    // lookup
    size_t hint;
    common::rolling_hash<>::record * rec;
    _server->_rhash.get(_request->key.begin(), _request->key.end(), rec, hint);
    
    if(rec)
    {
        _request->resp_type = RESP_OK_GET_HIT;
        
        // copy in value
        const char * val = rec->value();
        size_t val_len = rec->value_length();
        _server->_bring.produce_range(val, val + val_len);
        _request->resp_data_length = _server->_bring.available_read();
        _server->_bring.get_read_regions( _request->resp_data);
        _server->_bring.consumed( _server->_bring.available_read());
    }
    else
    {
        _request->resp_type = RESP_OK_GET_MISS;
    }
    
    finish_response();
    return;
}
void client::on_set(const stream_protocol::match_results_t & m)
{
    _request->req_type = REQ_SET;
    
    _request->key.resize( distance(m[1].first, m[1].second));
    copy( m[1].first, m[1].second, _request->key.begin());
    
    size_t data_length_size = distance(m[2].first, m[2].second);
    
    char buf[21];
    if(data_length_size + 1 >= sizeof(buf))
    {
        _request->resp_type = RESP_ERR_DATA_OVERFLOW;
        finish_response();
        return;
    }
    
    buf[data_length_size] = '\0';
    copy( m[2].first, m[2].second, buf);
    sscanf(buf, "%lu", &_request->req_data_length);
    
    if(_request->req_data_length > MAX_DATA_SIZE)
    {
        _request->resp_type = RESP_ERR_DATA_OVERFLOW;
        finish_response();
        return;
    }
    
    // schedule a read of data + "\r\n"
    read_data(
        _request->req_data_length + 2,
        _request->req_data,
        boost::bind(
            &client::on_data,
            shared_from_this(),
            _1, _2
        )
    );
    return;
}
void client::on_data(const boost::system::error_code & ec, size_t read_length)
{
    if(ec) {
        std::cerr << "client read error: " << ec.message() << std::endl;
        return;
    }
    
    buffer_regions_t::iterator it = --(_request->req_data.end());
    
    // trim & extract trailing 2 characters
    char c2 = it->rgetc(), c1 = it->rgetc();
    if(c1 == '\0' && !it->size())
        c1 = (--it)->rgetc();
    
    if(c1 != '\r' || c2 != '\n')
    {
        _request->resp_type = RESP_ERR_DATA_MALFORMED;
        finish_response();
        return;
    }
    
    // DISPATCH 
    
    if(!_server->_rhash.migrate_head())
        _server->_rhash.drop_head();
    
    if(_server->_rhash.would_fit(_request->key.size(), _request->req_data_length))
    {
        _server->_rhash.set(
            _request->key.begin(), _request->key.end(),
            buffers_iterator_t::begin(_request->req_data),
            buffers_iterator_t::end(_request->req_data));
        _request->resp_type = RESP_OK_SET;
    }
    else
        _request->resp_type = RESP_ERR_NO_SPACE;
    
    finish_response();
    return;
}

void client::finish_response()
{
    if(_request->resp_type == RESP_OK_VERSION)
        queue_write(_resp_ok_version);
    else if(_request->resp_type == RESP_OK_PONG)
        queue_write(_resp_ok_pong);
    else if(_request->resp_type == RESP_OK_SET)
        queue_write(_resp_ok_set);
    else if(_request->resp_type == RESP_OK_GET_HIT)
    {
        char buf[21];
        sprintf(buf, "%lu", _request->resp_data_length);
        queue_write(_resp_ok_get_hit);
        queue_write(buf);
        queue_write(_resp_newline);
        queue_write(_request->resp_data);
        queue_write(_resp_newline);
    }
    else if(_request->resp_type == RESP_OK_GET_MISS)
        queue_write(_resp_ok_get_miss);
    else if(_request->resp_type == RESP_ERR_NO_SPACE)
        queue_write(_resp_err_no_space);
    else if(_request->resp_type == RESP_ERR_CMD_MALFORMED)
        queue_write(_resp_err_cmd_malformed);
    else if(_request->resp_type == RESP_ERR_CMD_OVERFLOW)
    {
        queue_write(_resp_err_cmd_overflow);
        _close_pending = true;
    }
    else if(_request->resp_type == RESP_ERR_DATA_OVERFLOW)
    {
        queue_write(_resp_err_data_overflow);
        _close_pending = true;
    }
    else if(_request->resp_type == RESP_ERR_DATA_MALFORMED)
    {
        queue_write(_resp_err_data_malformed);
        _close_pending = true;
    }
    
    // Flush accumulated writes to client
    assert(!in_write());
    write_queued( boost::bind(
        &client::on_write, shared_from_this(), _1));
    
    return;
}

void client::on_command(
    const boost::system::error_code & ec,
    const stream_protocol::match_results_t & m,
    size_t matched_re_ind)
{
    if(ec == boost::system::errc::value_too_large)
    {
        _request->resp_type = RESP_ERR_CMD_OVERFLOW;
        finish_response();
        return;
    }
    else if(ec)
    {
        std::cerr << "client read error: " << ec.message() << std::endl;
        return;
    }
    
    // de-reference pointer to member method, & dispatch
    (this->*(_cmd_handler[matched_re_ind]))(m);
}
void client::on_write( const boost::system::error_code & ec)
{
    if(ec) {
        std::cerr << "client write error: " << ec.message() << std::endl;
        socket().close();
    }
    
    if(_close_pending)
    {
        socket().close();
        return;
    }
    
    // Set up for next request
    _request->reset();
    
    // Note: we issue the read operation here, rather than
    //  in finish_response(), because this guarantees that
    //  only one read/write operation for a given client
    //  will have been issued, and thus locking is not
    //  required.
    
    read_regex(
        _cmd_re.begin(), _cmd_re.end(),
        MAX_COMMAND_SIZE,
        boost::bind(
            &client::on_command,
            shared_from_this(),
            _1, _2, _3
        )
    );
    return;
}

// Populates _cmd_re & _cmd_handler
void client::build_command_table()
{
    _cmd_re.push_back(boost::regex(
        "^(?:PING|ping)\r\n"));
    _cmd_handler.push_back( &client::on_ping);
    
    _cmd_re.push_back(boost::regex(
        "^(FGET|fget|GET|get) ([^ ]+)\r\n"));
    _cmd_handler.push_back( &client::on_get);
    
    _cmd_re.push_back(boost::regex(
        "^(?:SET|set) ([^ ]+) ([\\d+]+)\r\n"));
    _cmd_handler.push_back( &client::on_set);
    
    _cmd_re.push_back(boost::regex(
        "shutdown\r\n"));
    _cmd_handler.push_back( &client::on_shutdown);

    // a full line not matching elsewhere is malformed
    _cmd_re.push_back(boost::regex(
        "^.*?\r\n"));
    _cmd_handler.push_back( &client::on_malformed);
    
    return;
}

} // end namespace samoa 
