#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

template<typename ValueCallback>
void blob::value(const spb::PersistedRecord & record,
    const ValueCallback & callback)
{
    for(const std::string & value : record.consistent_blob_value())
    {
        SAMOA_ASSERT(value.size());
        callback(value);
    }
    for(const std::string & value : record.blob_value())
    {
        if(value.size())
            callback(value);
    }
}

}
}

