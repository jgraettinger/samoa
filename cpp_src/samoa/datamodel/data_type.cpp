#include "samoa/datamodel/data_type.hpp"
#include "samoa/error.hpp"

namespace samoa {
namespace datamodel {

data_type data_type_from_string(const std::string & s)
{
    if(s == "BLOB_TYPE")
        return BLOB_TYPE;
    if(s == "COUNTER_TYPE")
        return COUNTER_TYPE;

    SAMOA_ASSERT(0 && "no such data_type");
    return BLOB_TYPE; // not reached
}

std::string to_string(data_type d)
{
    if(d == BLOB_TYPE)
        return "BLOB_TYPE";
    if(d == COUNTER_TYPE)
        return "COUNTER_TYPE";

    SAMOA_ASSERT(0 && "data_type not in valid enum range");
    return ""; // not reached
}


}
}

