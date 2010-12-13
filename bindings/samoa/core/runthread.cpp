#include "samoa/core/runthread.hpp"

namespace samoa {
namespace core {

// Global run-thread variable w/ extern linkage
PyThreadState * _run_thread = 0;

}
}

