#ifndef SAMOA_DATAMODEL_PACKED_UNSIGNED_HPP
#define SAMOA_DATAMODEL_PACKED_UNSIGNED_HPP

namespace samoa {
namespace datamodel {

struct packed_unsigned
{
public:

    uint64_t value;

    packed_unsigned(uint64_t v)
     : value(v)
    { }

    packed_unsigned()
     : value(0)
    { }

    static unsigned serialized_length(uint64_t v)
    {
        return sizeof(uint64_t);
    }

    operator uint64_t() const
    { return value; }
};

inline std::ostream & operator << (
    std::ostream & s, const packed_unsigned & r)
{
    s.write((char*) &r.value, sizeof(uint64_t));
    return s;
}

inline std::istream & operator >> (
    std::istream & s, packed_unsigned & r)
{
    s.read((char*) &r.value, sizeof(uint64_t));
    return s;
}

}
}

#endif

