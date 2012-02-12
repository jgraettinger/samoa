#ifndef SAMOA_CORE_MURMUR_CHECKSHUMMER_HPP
#define SAMOA_CORE_MURMUR_CHECKSHUMMER_HPP

#include <array>

namespace samoa {
namespace core {

/*
 * This specific source file (murmur_checksum.hpp) follows in the MurmurHash
 *  tradition, and is placed in the public domain. The author disclaims all
 *  copyright to this particular source file.
 */

/*!
 * This checksummer is a streaming adaptation of
 *  Austin Appleby's MurmurHash3_x64_128();
 *  see http://code.google.com/p/smhasher/
 */
class murmur_checksummer
{
public:

    typedef std::array<uint64_t, 2> checksum_t;

    murmur_checksummer(uint32_t seed)
     :  _h1(seed),
        _h2(seed),
        _block({{0,0}}),
        _block_pos(0),
        _total_length(0)
    { }

    void process_bytes(const uint8_t * data, unsigned length)
    {
        _total_length += length;
        while(length >= (16 - _block_pos))
        {
            std::copy(data, data + (16 - _block_pos),
                reinterpret_cast<uint8_t*>(&_block) + _block_pos);

            // See: MurmurHash3_x64_128
            {
                uint64_t & k1 = _block[0];
                uint64_t & k2 = _block[1];

                k1 *= _c1; k1  = rotl(k1,31); k1 *= _c2; _h1 ^= k1;

                _h1 = rotl(_h1,27); _h1 += _h2; _h1 = _h1*5+0x52dce729;

                k2 *= _c2; k2  = rotl(k2,33); k2 *= _c1; _h2 ^= k2;

                _h2 = rotl(_h2,31); _h2 += _h1; _h2 = _h2*5+0x38495ab5;
            }

            _block[0] = _block[1] = 0;

            data += (16 - _block_pos);
            length -= (16 - _block_pos);
            _block_pos = 0;
        }

        std::copy(data, data + length,
            reinterpret_cast<uint8_t*>(&_block) + _block_pos);
        _block_pos += length;
    }

    checksum_t checksum() const
    {
        checksum_t result = {{_h1, _h2}};

        uint64_t & h1 = result[0];
        uint64_t & h2 = result[1];
        const uint8_t * tail = reinterpret_cast<const uint8_t*>(&_block);

        uint64_t k1 = 0;
        uint64_t k2 = 0;
      
        switch(_block_pos & 15)
        {
        case 15: k2 ^= uint64_t(tail[14]) << 48;
        case 14: k2 ^= uint64_t(tail[13]) << 40;
        case 13: k2 ^= uint64_t(tail[12]) << 32;
        case 12: k2 ^= uint64_t(tail[11]) << 24;
        case 11: k2 ^= uint64_t(tail[10]) << 16;
        case 10: k2 ^= uint64_t(tail[ 9]) << 8;
        case  9: k2 ^= uint64_t(tail[ 8]) << 0;
                 k2 *= _c2; k2  = rotl(k2,33); k2 *= _c1; h2 ^= k2;
      
        case  8: k1 ^= uint64_t(tail[ 7]) << 56;
        case  7: k1 ^= uint64_t(tail[ 6]) << 48;
        case  6: k1 ^= uint64_t(tail[ 5]) << 40;
        case  5: k1 ^= uint64_t(tail[ 4]) << 32;
        case  4: k1 ^= uint64_t(tail[ 3]) << 24;
        case  3: k1 ^= uint64_t(tail[ 2]) << 16;
        case  2: k1 ^= uint64_t(tail[ 1]) << 8;
        case  1: k1 ^= uint64_t(tail[ 0]) << 0;
                 k1 *= _c1; k1  = rotl(k1,31); k1 *= _c2; h1 ^= k1;
        };
      
        //----------
        // finalization

        h1 ^= _total_length; h2 ^= _total_length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);
      
        h1 += h2;
        h2 += h1;

        return result;
    }

private:

    inline uint64_t rotl(uint64_t x, int8_t r) const
    { return (x << r) | (x >> (64 - r)); }

    // Finalization mix - force all bits of a hash block to avalanche
    uint64_t fmix(uint64_t k) const
    {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccd;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53;
        k ^= k >> 33;

        return k;
    }

    static const uint64_t _c1 = 0x87c37b91114253d5;
    static const uint64_t _c2 = 0x4cf5ad432745937f;

    uint64_t _h1, _h2;

    std::array<uint64_t, 2> _block;
    uint8_t _block_pos;

    unsigned _total_length;
};

}
}

#endif 
