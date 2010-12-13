
#include <boost/python.hpp>

#include <iostream>


namespace samoa {
namespace core {

namespace bpl = boost::python;

extern bpl::object _current_generator;

class generator_scope
{
public:

    generator_scope(const bpl::object & gen)
    {
        if(_current_generator.ptr() != Py_None)
            throw std::runtime_error("_current_generator isn't none");

        _current_generator = gen;
    }

    ~generator_scope()
    {
        _current_generator = bpl::object();
    }

    void send_value(const bpl::object & arg)
    {
        try
        {
            bpl::object res = _current_generator.attr("send")(arg);

            if(res.ptr() != Py_None)
                throw std::runtime_error("send_value(): non-None result");
        }
        catch(bpl::error_already_set)
        {
            if(!PyErr_ExceptionMatches(PyExc_StopIteration))
                throw;

            PyErr_Clear();
            std::cerr << "send_value() caught StopIteration" << std::endl;
        }
    }

    void send_exception(const std::string & msg)
    {
        bpl::object ex_type = bpl::object(
            (bpl::detail::borrowed_reference) PyExc_RuntimeError);

        try
        {
            bpl::object res = _current_generator.attr("throw")(ex_type, msg);

            if(res.ptr() != Py_None)
                throw std::runtime_error("send_exception(): non-None result");
        }
        catch(bpl::error_already_set)
        {
            if(!PyErr_ExceptionMatches(PyExc_StopIteration))
                throw;

            PyErr_Clear();
            std::cerr << "send_exception() caught StopIteration" << std::endl;
        }
    }
};

/*
// Only one generator can be running at any given time, 
class generator;

class generator
{
public:

    void send_result();
    void send_exception();
    
    

};

// Retrieves the currently-active generator. Note there There may be only one
//   in a given stack
generator * get_generator();

}
}



i, r = submethod(), None
while True:
    try:
        r = i.send(r)
    except StopIteration:
        pass

*/
}
}

