#ifndef SAMOA_SERVER_CRON_HPP
#define SAMOA_SERVER_CRON_HPP

#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <boost/shared_ptr.hpp>
#include <functional>
#include <vector>

namespace samoa {
namespace server {

class cron :
    public boost::enable_shared_from_this<cron>
{
public:

    typedef std::function<void(void)> cron_callback_t;

    cron();

    void initialize();
    void shutdown();

    void invoke_next_second(cron_callback_t);

private:

    void on_cron(boost::system::error_code);

    boost::asio::deadline_timer _timer;
    std::vector<cron_callback_t> _callbacks;
    bool _shutdown;

    spinlock _lock;
};

}
}

#endif

