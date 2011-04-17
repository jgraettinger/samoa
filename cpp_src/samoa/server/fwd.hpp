#ifndef SAMOA_SERVER_FWD_HPP
#define SAMOA_SERVER_FWD_HPP

#include <boost/shared_ptr.hpp>
#include <boost/smart_ptr/enable_shared_from_this.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace samoa {
namespace server {

class context;
typedef boost::shared_ptr<context> context_ptr_t;

class protocol;
typedef boost::shared_ptr<protocol> protocol_ptr_t;

class listener;
typedef boost::shared_ptr<listener> listener_ptr_t;

class client;
typedef boost::shared_ptr<client> client_ptr_t;

class command_handler;
typedef boost::shared_ptr<command_handler> command_handler_ptr_t;

};
};

#endif

