
#include <boost/system/system_error.hpp>
#include <boost/system/error_code.hpp>
#include <iostream>

namespace samoa {

void checked_throw(int ev, const char * where = 0)
{
    if(ev)
    {
        throw boost::system::system_error(ev,
            boost::system::system_category(), where);
    }
}

void checked_throw(const boost::system::error_code & ec,
    const char * where = 0)
{
    if(ec)
    {
        throw boost::system::system_error(ec, where);
    }
}

void checked_abort(int ev, const char * where = 0)
{
    if(ev)
    {
        boost::system::system_error err(ev,
            boost::system::system_category(), where);

        std::cerr << err.what() << std::endl;
        abort();
    }
}

};

