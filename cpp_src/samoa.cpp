
#include "samoa/server.hpp"
#include "common/reactor.hpp"
#include <boost/asio.hpp>

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

    r->run();
    return 0;
}
