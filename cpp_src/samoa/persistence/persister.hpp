#ifndef SAMOA_PERSISTENCE_PERSITER_HPP
#define SAMOA_PERSISTENCE_PERSITER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/persistence/record.hpp"
#include "samoa/core/proactor.hpp"
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <string>
#include <vector>

namespace samoa {
namespace persistence {

class persister :
    public boost::enable_shared_from_this<persister>
{
public:

    typedef persister_ptr_t ptr_t;

    typedef boost::function<
        void (const boost::system::error_code &, const record *)
    > get_callback_t;

    typedef boost::function<
        bool (const boost::system::error_code &, const record *, record *)
    > put_callback_t;

    typedef boost::function<
        bool (const boost::system::error_code &, const record *)
    > drop_callback_t;

    persister(core::proactor &);
    virtual ~persister();

    void add_heap_hash(size_t region_size, size_t table_size);

    void add_mapped_hash(std::string file, size_t region_size, size_t table_size);

    void get(get_callback_t &&, std::string && key);

    void put(put_callback_t && put_callback,
        std::string && key, size_t value_length);

    void drop(drop_callback_t && drop_callback, std::string && key);

    size_t get_layer_count() const
    { return _layers.size(); }

    const rolling_hash & get_layer(size_t index) const;

private:
    
    void on_get(const std::string &, const persister::get_callback_t &);

    void on_put(const std::string &, size_t,
        const persister::put_callback_t &);

    void on_drop(const std::string &, const persister::drop_callback_t &);

    bool make_room(size_t, size_t, record::offset_t, record::offset_t, size_t);

    std::vector<std::pair<size_t, const record *>> _iterators;
    std::vector<rolling_hash*> _layers;

    boost::asio::strand _strand;

    size_t _max_rotations;
};

}
}

#endif

