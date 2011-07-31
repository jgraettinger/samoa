
#include "samoa/core/server_time.hpp"
#include <ctime>

namespace samoa {
namespace core {

uint64_t server_time::_time = std::time(NULL);

void server_time::set_time(uint64_t new_time)
{
    _time = new_time;
}

}
}

