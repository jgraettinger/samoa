#include "samoa/core/buffer_region.hpp"

namespace samoa {
namespace core {

void copy_regions_into(const buffer_regions_t & in,
    std::string & out)
{
    size_t total_size = 0;
    for(const buffer_region & region : in)
    {
        total_size += region.size();
    }

    out.resize(total_size);
    std::string::iterator it_out = out.begin();

    for(const buffer_region & region : in)
    {
        it_out = std::copy(region.begin(), region.end(), it_out);
    }
}

}
}

