
#include <boost/uuid/string_generator.hpp>

namespace samoa {
namespace core {

bool parse_uuid(const std::string & bytes, uuid & out)
{
    if(bytes.size() == sizeof(core::uuid))
    {
        // this is a raw uuid
        std::copy(bytes.begin(), bytes.end(), out.begin())
        return true;
    }

    try {
        // assume a hexidecimal uuid
        boost::uuids::string_generator()(bytes);
    }
    catch(const std::runtime_error &)
    {
        out = boost::uuids::nil_uuid();
        return false;
    }
}

}
}

