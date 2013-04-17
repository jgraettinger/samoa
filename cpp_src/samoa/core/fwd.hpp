#ifndef SAMOA_CORE_FWD_HPP
#define SAMOA_CORE_FWD_HPP

#include <boost/asio.hpp>
#include <memory>
#include <array>

namespace samoa {
namespace core {

class proactor;
typedef std::shared_ptr<proactor> proactor_ptr_t;
typedef std::shared_ptr<boost::asio::io_service> io_service_ptr_t;

typedef std::shared_ptr<boost::asio::strand> strand_ptr_t;
typedef std::shared_ptr<boost::asio::deadline_timer> timer_ptr_t;

class connection_factory;
typedef std::shared_ptr<connection_factory> connection_factory_ptr_t;

typedef std::array<uint64_t, 2> murmur_checksum_t;
class murmur_hash;

class memory_map;
typedef std::unique_ptr<memory_map> memory_map_ptr_t;

namespace random {
    uint64_t generate_uint64();
}

}
}

#endif

