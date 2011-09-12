#ifndef SAMOA_DATA_MODEL_DATA_TYPE_HPP
#define SAMOA_DATA_MODEL_DATA_TYPE_HPP

#include <string>

namespace samoa {
namespace datamodel {

enum data_type {

    BLOB_TYPE,
    COUNT_TYPE,
    MAP_TYPE
};

// throws on conversion failure
data_type data_type_from_string(const std::string &);

std::string to_string(data_type);

}
}

#endif

