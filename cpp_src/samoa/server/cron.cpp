
#include "samoa/server/cron.hpp"
#include "samoa/core/proactor.hpp"
#include "samoa/core/server_time.hpp"
#include <boost/asio.hpp>
#include <ctime>

namespace samoa {
namespace server {

cron::cron()
 :  _timer(*core::proactor::get_proactor()->serial_io_service()),
    _shutdown(false)
{
}

void cron::initialize()
{
    {
        spinlock::guard guard(_lock);

        _timer.expires_from_now(boost::posix_time::seconds(1));
        _timer.async_wait(std::bind(&cron::on_cron, shared_from_this(),
            std::placeholders::_1));
    }
}

void cron::shutdown()
{
    spinlock::guard guard(_lock);
    _shutdown = true;
    _timer.cancel();
}

void cron::invoke_next_second(cron_callback_t callback)
{
    spinlock::guard guard(_lock);
    _callbacks.push_back(std::move(callback));
}

void cron::on_cron(boost::system::error_code ec)
{
    if(ec == boost::system::errc::operation_canceled)
        return;

    SAMOA_ASSERT(!ec);

    std::vector<cron_callback_t> callbacks;
    {
        spinlock::guard guard(_lock);
        callbacks = std::move(_callbacks);

        if(!_shutdown)
        {
            _timer.expires_from_now(boost::posix_time::seconds(1));
            _timer.async_wait(std::bind(&cron::on_cron, shared_from_this(),
                std::placeholders::_1));
        }
    }

    // update server clock
    core::server_time::set_time(std::time(NULL));

    LOG_INFO("Set server time to " << core::server_time::get_time());

    for(const auto & callback : callbacks)
    {
        callback();
    }
}

}
}

