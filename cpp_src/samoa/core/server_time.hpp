#ifndef SAMOA_CORE_SERVER_TIME_HPP
#define SAMOA_CORE_SERVER_TIME_HPP

#include <cstdint>

namespace samoa {
namespace core {

class server_time
{
public:

    // not locked, as it's one word
    static uint64_t get_time()
    { return _time; }

    static void set_time(uint64_t);

private:

    static uint64_t _time;
};

}
}

#endif

