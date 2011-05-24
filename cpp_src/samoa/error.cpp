#include "samoa/error.hpp"
#include "samoa/log.hpp"
#include <sstream>

namespace samoa {
namespace error {

// instantiate samoa_exception in libsamoa
samoa_exception::samoa_exception(
    const std::string & _type,
    const std::string & _msg,
    const std::string & _file,
    const std::string & _func,
    unsigned _line_no,
    bool _abort)
 :  type(_type),
    msg(_msg),
    file(_file),
    func(_func),
    line_no(_line_no),
    abort(_abort)
{
    std::stringstream s;

    s << "samoa_exception<" << type << ">: " << msg;

    if(!file.empty())
    {
        s << "\n\t" << file << ":" << line_no << " {" << func << "}";
    }

    p_what = s.str();
}

void throw_not_found(const std::string & what,
    const std::string & ident)
{
    throw samoa_exception("not_found",
        what + " " + ident);
}

void generate_core_file()
{
    LOG_ERR(__PRETTY_FUNCTION__ << ": I would if i could...");
}


}
}

