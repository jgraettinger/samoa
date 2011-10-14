
#include "samoa/core/uuid.hpp"
#include "samoa/error.hpp"
#include <boost/uuid/string_generator.hpp>

namespace samoa {
namespace core {

uuid try_parse_uuid(const std::string & bytes) throw()
{
    uuid out;

    if(bytes.size() == sizeof(core::uuid))
    {
        // this is a raw uuid
        std::copy(bytes.begin(), bytes.end(), out.begin());
    }
    else
    {
        try {
            // assume a hexidecimal uuid
            out = boost::uuids::string_generator()(bytes);
        }
        catch(const std::runtime_error &)
        {
            out = boost::uuids::nil_uuid();
        }
    }
    return out;
}

uuid parse_uuid(const std::string & bytes)
{
    uuid out = try_parse_uuid(bytes);
    SAMOA_ASSERT(!out.is_nil());
    return out;
}

}
}

