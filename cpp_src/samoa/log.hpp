#ifndef SAMOA_LOG_HPP
#define SAMOA_LOG_HPP

#define LOG_HEADER "[" << ::samoa::log::gettid() << "] " << __FILE__ << ":" << __LINE__ << " {" << __PRETTY_FUNCTION__ << "}: "

#define LOG_DBG(what)  (std::cerr << "DBG " << LOG_HEADER << what << std::endl)
#define LOG_INFO(what) (std::cerr << "INF " << LOG_HEADER << what << std::endl)
#define LOG_WARN(what) (std::cerr << "WRN " << LOG_HEADER << what << std::endl)
#define LOG_ERR(what)  (std::cerr << "ERR " << LOG_HEADER << what << std::endl)

// to disable debug logging at compile-time
//#define LOG_DBG(what)

namespace samoa {
namespace log {

unsigned gettid();

}
}

#endif

