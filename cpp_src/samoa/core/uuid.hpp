#ifndef SAMOA_CORE_UUID_HPP
#define SAMOA_CORE_UUID_HPP

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>

namespace samoa {
namespace core {

typedef boost::uuids::uuid uuid;

inline uuid uuid_from_hex(const std::string & hex)
{ return boost::uuids::string_generator()(hex); }

inline std::string to_hex(const uuid & uuid)
{ return boost::uuids::to_string(uuid); }

};
};

#endif

