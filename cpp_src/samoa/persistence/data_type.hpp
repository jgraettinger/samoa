#ifndef SAMOA_PERSISTENCE_DATA_TYPE_HPP
#define SAMOA_PERSISTENCE_DATA_TYPE_HPP

#include <string>

namespace samoa {
namespace persistence {

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

