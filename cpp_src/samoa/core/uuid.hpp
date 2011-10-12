#ifndef SAMOA_CORE_UUID_HPP
#define SAMOA_CORE_UUID_HPP

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <string>

namespace samoa {
namespace core {

typedef boost::uuids::uuid uuid;

bool parse_uuid(const std::string & bytes, uuid & out);

inline std::string to_hex(const uuid & uuid)
{ return boost::uuids::to_string(uuid); }

inline std::string to_bytes(const uuid & uuid)
{ return std::string(uuid.begin(), uuid.end()); }

};
};

#endif

