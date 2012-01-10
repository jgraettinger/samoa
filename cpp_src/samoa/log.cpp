#include "samoa/log.hpp"

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <cstdio>
#include <sstream>

namespace samoa {
namespace log {

unsigned gettid()
{
    return syscall(SYS_gettid);
}

std::string ascii_escape(const std::string & in)
{
    std::stringstream out;
    char buf[10];

    out << '"';
    for(unsigned char c : in)
    {
        if(c == '\r')
            out << "\\r";
        else if(c == '\n')
            out << "\\n";
        else if(c == '\t')
            out << "\\t";
        else if(c == '\\')
            out << "\\\\";
        else if(c == '"')
            out << "\\\"";
        else if(c < 0x20 || c >= 0x7f)
        {
            std::sprintf(buf, "\\x%02x", c);
            out << buf;
        }
        else
            out << c;
    }
    out << '"';
    return out.str();
}

}
}

