#include "scoped_python.hpp"

namespace pysamoa {

void null_cleanup(PyThreadState *)
{}

// Python thread which entered proactor.run()
boost::thread_specific_ptr<PyThreadState> _saved_python_thread(
    &null_cleanup);

}

