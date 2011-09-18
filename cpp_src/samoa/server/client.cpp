
#include "samoa/server/client.hpp"
#include "samoa/server/context.hpp"
#include "samoa/server/protocol.hpp"
#include "samoa/server/request_state.hpp"
#include "samoa/server/command_handler.hpp"
#include "samoa/core/stream_protocol.hpp"
#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <sstream>

#define MAX_DATA_BLOCK_LENGTH 4194304

namespace samoa {
namespace server {

using namespace boost::asio;

// default timeout of 1 minute
unsigned default_timeout_ms = 60 * 1000;

client::client(context::ptr_t context, protocol::ptr_t protocol,
    core::io_service_ptr_t io_srv,
    std::unique_ptr<ip::tcp::socket> & sock)
 : core::stream_protocol(io_srv, sock),
   core::tasklet<client>(io_srv),
   _context(context),
   _protocol(protocol),
   _timeout_ms(default_timeout_ms),
   _timeout_timer(*get_io_service())
{
    LOG_DBG("created");

    set_tasklet_name("client@<" + get_remote_address() + ":" +
        boost::lexical_cast<std::string>(get_remote_port()) + ">"); 
}

client::~client()
{
    LOG_DBG("destroyed");
}

void client::run_tasklet()
{
    on_next_request();

    // start a timeout timer, waiting for requests from the client
    _timeout_timer.expires_from_now(
        boost::posix_time::milliseconds(_timeout_ms));
    _timeout_timer.async_wait(boost::bind(
        &client::on_timeout, shared_from_this(), _1));

    _ignore_timeout = false;
}

void client::halt_tasklet()
{
    close();
    _timeout_timer.cancel();
}

void client::finish_response(bool close)
{
    // command handler may have written additional data,
    //   and already waited for those writes to finish
    if(has_queued_writes())
    {
        write_queued(boost::bind(&client::on_response_finish,
            shared_from_this(), _1, close));
    }
    else
    {
        on_response_finish(boost::system::error_code(), close);
    }
    return;
}

void client::close()
{
    _timeout_timer.cancel();
    core::stream_protocol::close();
}

void client::on_next_request()
{
    // start request read
    read_data(boost::bind(&client::on_request_length,
        shared_from_this(), _1, _3), 2);
}

void client::on_request_length(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec.message());
        return;
    }

    uint16_t len;
    std::copy(buffers_begin(read_body), buffers_end(read_body),
        (char*) &len);

    read_data(boost::bind(&client::on_request_body,
        shared_from_this(), _1, _3), ntohs(len));
}

void client::on_request_body(const boost::system::error_code & ec,
    const core::buffer_regions_t & read_body)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec.message());
        return;
    }

    request_state::ptr_t rstate = boost::make_shared<request_state>(
        shared_from_this());

    core::zero_copy_input_adapter zci_adapter(read_body);
    if(!rstate->get_samoa_request().ParseFromZeroCopyStream(&zci_adapter))
    {
        rstate->send_client_error(400, "protobuf parse error", true);
        return;
    }

    on_request_data_block(boost::system::error_code(),
        0, core::buffer_regions_t(), rstate);
}

void client::on_request_data_block(const boost::system::error_code & ec,
    unsigned ind, const core::buffer_regions_t & data,
    const request_state::ptr_t & rstate)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec.message());
        return;
    }

    spb::SamoaRequest & samoa_request = rstate->get_samoa_request();
    std::vector<core::buffer_regions_t> & data_blocks = \
        rstate->get_request_data_blocks();

    if(ind < data_blocks.size())
    {
        data_blocks[ind] = data;
        ++ind;
    }
    else
    {
        // this is first entrance into on_request_data_block();
        //   resize data_blocks appropriately
        data_blocks.resize(samoa_request.data_block_length_size());
    }

    if(ind != data_blocks.size())
    {
        // still more data blocks to read
        unsigned block_len = samoa_request.data_block_length(ind);

        if(block_len > MAX_DATA_BLOCK_LENGTH)
        {
            rstate->send_client_error(400, "data block too large", true);
            return;
        }

        read_data(boost::bind(&client::on_request_data_block,
            shared_from_this(), _1, ind, _3, rstate), block_len);
        return;
    }

    // we're done reading data blocks

    // we've recieved a complete request in the timeout period
    _ignore_timeout = true;

    if(!rstate->load_from_samoa_request(get_context()))
    {
        // an error has already been sent to the client
        return;
    }

    command_handler::ptr_t handler = _protocol->get_command_handler(
        samoa_request.type());

    if(!handler)
    {
        rstate->send_client_error(501, "unknown operation type");
        return;
    }

    handler->handle(rstate);
    return;
}

void client::on_response_finish(const boost::system::error_code & ec,
    bool should_close)
{
    if(ec)
    {
        close();
        LOG_WARN("connection error: " << ec.message());
        return;
    }

    if(should_close)
    {
        close();
        return;
    }

    // post to begin next request
    get_io_service()->post(boost::bind(
        &client::on_next_request, shared_from_this()));
}

void client::on_timeout(boost::system::error_code ec)
{
    // _timeout_timer will call on_timeout with ec == 0 on timeout
    if(!ec && _ignore_timeout)
    {
        // start a timeout timer, waiting for requests from the client
        _timeout_timer.expires_from_now(
            boost::posix_time::milliseconds(_timeout_ms));
        _timeout_timer.async_wait(boost::bind(
            &client::on_timeout, shared_from_this(), _1));

        _ignore_timeout = false;
    }
    else if(!ec && !_ignore_timeout)
    {
        // treat this timeout as an error
        ec = boost::system::errc::make_error_code(
            boost::system::errc::stream_timeout);

        close();
        LOG_INFO("client timeout");
        return;
    }
}

}
}

