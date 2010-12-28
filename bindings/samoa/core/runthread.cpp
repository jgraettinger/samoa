#include "samoa/core/scoped_python.hpp"

namespace samoa {
namespace core {

// Python thread which entered proactor.run()
PyThreadState * _run_thread = 0;

// Atomic reentrance-count of scoped_python struct
boost::detail::atomic_count _scoped_python_count(0L);

}
}

