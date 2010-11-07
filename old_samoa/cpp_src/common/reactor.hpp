#ifndef COMMON_REACTOR_HPP
#define COMMON_REACTOR_HPP

#include <boost/bind/protect.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <iostream>

namespace common {

class reactor :
    private boost::noncopyable,
    public  boost::enable_shared_from_this<reactor>
{
public:
    
    typedef boost::shared_ptr<reactor> ptr_t;
    
    reactor()
    { }
    
    boost::asio::io_service & io_service()
    { return _io_service; }
    
    void run();
    
    template<typename Callback>
    void call_later(
        const Callback & callback,
        size_t milli_sec = 0
    )
    {
        if(milli_sec)
        {
            boost::asio::deadline_timer t(_io_service);
            
            t.expires_from_now(
                boost::posix_time::milliseconds(milli_sec)
            );
            t.async_wait(
                boost::bind(
                    &reactor::template call_later_handler<
                        boost::_bi::protected_bind_t<Callback>
                    >,
                    shared_from_this(),
                    _1,//boost::asio::placeholders::error,
                    boost::protect(callback)
                )
            );
        }
        else
        {
            _io_service.post(
                boost::bind(
                    &reactor::template call_later_handler<
                        boost::_bi::protected_bind_t<Callback>
                    >,
                    shared_from_this(),
                    boost::system::error_code(),
                    boost::protect(callback)
                )
            );
        }
        return;
    }
    
private:
    
    template<typename Callback>
    void call_later_handler(
        const boost::system::error_code & ec,
        const Callback & c
    )
    {
        if(ec)
        {
            std::cerr << "call_later " << ec.value() << " " << ec.message() << std::endl;
        }
        
        c();}
    
    boost::asio::io_service _io_service;
};

} // end common

#endif
