
#include "common/reactor.hpp"
#include <iostream>
#include <Python.h>

using namespace std;

namespace common {

void reactor::run()
{
    Py_BEGIN_ALLOW_THREADS; 
    
    bool running = true;
    while(running)
    {
        try
        {
            _io_service.run();
            // clean exit => no more work
            running = false;
            _io_service.reset();
        }
        catch(const std::exception & e)
        {
            cerr << "Caught: " << e.what() << endl;
        }
    }
    
    Py_END_ALLOW_THREADS; 
}

}

