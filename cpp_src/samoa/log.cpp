#include "samoa/log.hpp"

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

namespace samoa {
namespace log {

unsigned gettid()
{
    return syscall(SYS_gettid);
}

}
}

