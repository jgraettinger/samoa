
#include "samoa/server.hpp"
#include "samoa/partition.hpp"
#include "common/reactor.hpp"
#include <boost/asio.hpp>

#include <iostream>

using namespace std;
using namespace boost;

int main(int argc, const char ** argv)
{
    common::reactor::ptr_t r( new common::reactor());

    samoa::server::ptr_t srv( new samoa::server(
        "127.0.0.1",
        "5646",
        100,
        r
    ));
    
    /*
    srv->_partition.reset(
        new samoa::partition(
            "test_mapped_file",
            1L << 30,
            1L << 22)
    );
    */
    
    srv->_partition.reset(
        new samoa::partition(
            "test_mapped_file",
            1L << 25,
            1L << 17)
    );
    
    r->run();
    srv.reset();
    r.reset();
    
    cerr << "run returned" << endl;
    return 0;
}
