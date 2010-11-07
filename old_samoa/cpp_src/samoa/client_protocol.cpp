
#include "samoa/client_protocol.hpp"
#include "samoa/server.hpp"
#include "samoa/partition.hpp"
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
typedef void (client_protocol::*command_handler_t)(
    const stream_protocol::match_results_t &);

// Regex's for parsing client commands
std::vector<boost::regex> _cmd_re;
// Handlers for regex matches, of same arity & order as _cmd_re
std::vector<command_handler_t> _cmd_handler;

// Canned responses
namespace {
    const_buffer_region _resp_newline("\r\n");
    const_buffer_region _resp_version("SAMOA v0.1\r\n");
    const_buffer_region _resp_pong("PONG\r\n");
    const_buffer_region _resp_ok("OK\r\n");
    const_buffer_region _resp_err_cmd_malformed("-ERR command malformed\r\n");
    const_buffer_region _resp_err_cmd_overflow("-ERR command overflow\r\n");
    const_buffer_region _resp_err_data_malformed("-ERR data malformed\r\n");
    const_buffer_region _resp_err_data_overflow("-ERR data overflow\r\n");
};

client_protocol::ptr_t client_protocol::new_client_protocol(
	const server::ptr_t & server,
    std::auto_ptr<tcp::socket> sock)
{
    if(_cmd_re.empty())
        build_command_table();
    
    client_protocol::ptr_t new_client( new client_protocol(server, sock));
    new_client->queue_write(_resp_version);
    new_client->finish_response();
    return new_client;
}

void client_protocol::finish_response()
{
    // Flush accumulated writes to client
    assert(!in_write());
    write_queued( boost::bind(
        &client_protocol::on_finish, shared_from_this(), _1));
    return;
}

client_protocol::client_protocol(
	const server::ptr_t & server,
    std::auto_ptr<tcp::socket> sock
) : 
    common::stream_protocol(sock),
    _server(server)
{ }

void client_protocol::on_ping(const stream_protocol::match_results_t & m)
{
    queue_write( _resp_pong);
    finish_response();
    return;
}
void client_protocol::on_shutdown(const stream_protocol::match_results_t & m)
{
    _server->shutdown();
    queue_write(_resp_ok);
    _request.close_on_finish = true;
    finish_response();
    return;
}
void client_protocol::on_malformed(const stream_protocol::match_results_t & m)
{
    // we may be dealing w/ a human, so don't close
    queue_write(_resp_err_cmd_malformed);
    finish_response();
    return;
}
void client_protocol::on_iter_keys(const stream_protocol::match_results_t & m)
{
    _request.req_type = REQ_ITER_KEYS;
    _server->_partition->handle_request(shared_from_this());
    return;
}
void client_protocol::on_get(const stream_protocol::match_results_t & m)
{
    _request.req_type = distance(m[1].first, m[1].second) == 3 ? \
        REQ_GET : REQ_FGET;
    
    _request.key.resize( distance(m[2].first, m[2].second));
    copy( m[2].first, m[2].second, _request.key.begin());
    
    _server->_partition->handle_request(shared_from_this());
    return;
}
void client_protocol::on_set(const stream_protocol::match_results_t & m)
{
    _request.req_type = REQ_SET;
    
    _request.key.resize( distance(m[1].first, m[1].second));
    copy( m[1].first, m[1].second, _request.key.begin());
    
    size_t data_length_size = distance(m[2].first, m[2].second);
    
    char buf[21];
    if(data_length_size + 1 >= sizeof(buf))
    {
        queue_write(_resp_err_data_overflow);
        _request.close_on_finish = true;
        finish_response();
        return;
    }
    
    buf[data_length_size] = '\0';
    copy( m[2].first, m[2].second, buf);
    sscanf(buf, "%lu", &_request.req_data_length);
    
    if(_request.req_data_length > MAX_DATA_SIZE)
    {
        queue_write(_resp_err_data_overflow);
        _request.close_on_finish = true;
        finish_response();
        return;
    }
    
    // schedule a read of data + "\r\n"
    read_data(
        _request.req_data_length + 2,
        _request.req_data,
        boost::bind(
            &client_protocol::on_data,
            shared_from_this(),
            _1, _2
        )
    );
    return;
}
void client_protocol::on_data(const boost::system::error_code & ec, size_t read_length)
{
    if(ec) {
        std::cerr << "client read error: " << ec.message() << std::endl;
        return;
    }
    
    buffer_regions_t::iterator it = --(_request.req_data.end());
    
    // trim & extract trailing 2 characters
    char c2 = it->rgetc(), c1 = it->rgetc();
    if(c1 == '\0' && !it->size())
        c1 = (--it)->rgetc();
    
    if(c1 != '\r' || c2 != '\n')
    {
        queue_write(_resp_err_data_malformed);
        _request.close_on_finish = true;
        finish_response();
        return;
    }
    
    _server->_partition->handle_request(shared_from_this());
    return;
}

void client_protocol::on_command(
    const boost::system::error_code & ec,
    const stream_protocol::match_results_t & m,
    size_t matched_re_ind)
{
    if(ec == boost::system::errc::value_too_large)
    {
        queue_write(_resp_err_cmd_overflow);
        _request.close_on_finish = true;
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

void client_protocol::on_finish(const boost::system::error_code & ec)
{
    if(ec) {
        std::cerr << "client write error: " << ec.message() << std::endl;
        socket().close();
    }
    
    if(_request.close_on_finish)
    {
        socket().close();
        return;
    }
    
    // Set up for next request
    _request.reset();
    
    // Note: we issue the read operation here, rather than
    //  in finish_response(), because this guarantees that
    //  only one read/write operation for a given client
    //  will have been issued, and thus locking is not
    //  required.
    
    read_regex(
        _cmd_re.begin(), _cmd_re.end(),
        MAX_COMMAND_SIZE,
        boost::bind(
            &client_protocol::on_command,
            shared_from_this(),
            _1, _2, _3
        )
    );
    return;
}

// Populates _cmd_re & _cmd_handler
void client_protocol::build_command_table()
{
    _cmd_re.push_back(boost::regex(
        "^(?:PING|ping)\r\n"));
    _cmd_handler.push_back( &client_protocol::on_ping);
    
    _cmd_re.push_back(boost::regex(
        "^(FGET|fget|GET|get) ([^ ]+)\r\n"));
    _cmd_handler.push_back( &client_protocol::on_get);
    
    _cmd_re.push_back(boost::regex(
        "^(?:SET|set) ([^ ]+) ([\\d+]+)\r\n"));
    _cmd_handler.push_back( &client_protocol::on_set);
    
    _cmd_re.push_back(boost::regex(
        "shutdown\r\n"));
    _cmd_handler.push_back( &client_protocol::on_shutdown);
    
    _cmd_re.push_back(boost::regex(
        "iter_keys\r\n"));
    _cmd_handler.push_back( &client_protocol::on_iter_keys);
    
    // a full line not matching elsewhere is malformed
    _cmd_re.push_back(boost::regex(
        "^.*?\r\n"));
    _cmd_handler.push_back( &client_protocol::on_malformed);
    
    return;
}

} // end namespace samoa

