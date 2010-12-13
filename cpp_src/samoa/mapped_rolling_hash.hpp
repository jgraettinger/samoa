#ifndef SAMOA_MAPPED_ROLLING_HASH_HPP
#define SAMOA_MAPPED_ROLLING_HASH_HPP

#include "samoa/rolling_hash.hpp"
#include <memory>

namespace samoa {

class mapped_rolling_hash : public rolling_hash
{
public:

    static std::auto_ptr<mapped_rolling_hash> open(
        const std::string & file, size_t region_size, size_t table_size);

    virtual ~mapped_rolling_hash();

private:

    struct pimpl_t;
    typedef std::auto_ptr<pimpl_t> pimpl_ptr_t;

    mapped_rolling_hash(pimpl_ptr_t);

    pimpl_ptr_t _pimpl;
};

};

#endif

