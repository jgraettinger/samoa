#ifndef SAMOA_PERSISTENCE_PERSITER_HPP
#define SAMOA_PERSISTENCE_PERSITER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/persistence/record.hpp"
#include "samoa/core/protobuf/fwd.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <string>
#include <vector>

namespace samoa {
namespace persistence {

namespace spb = samoa::core::protobuf;

class persister :
    public boost::enable_shared_from_this<persister>
{
public:

    typedef persister_ptr_t ptr_t;

    typedef boost::function<void(
        const boost::system::error_code &,
        const spb::PersistedRecord_ptr_t &)
    > callback_t;

    typedef boost::function<void(
        const boost::system::error_code &,
        const std::vector<spb::PersistedRecord_ptr_t> &)
    > iterate_callback_t;

    typedef boost::function<spb::PersistedRecord_ptr_t(
        const spb::PersistedRecord_ptr_t &, // current record
        const spb::PersistedRecord_ptr_t &) // new record
    > merge_callback_t;


    persister();
    virtual ~persister();

    void add_heap_hash(size_t storage_size, size_t index_size);

    void add_mapped_hash(const std::string & file,
        size_t storage_size, size_t index_size);

    void get(callback_t &&, const std::string & key);

    void put(callback_t &&, merge_callback_t &&, const std::string & key,
        const spb::PersistedRecord_ptr_t &);

    void drop(callback_t &&, const std::string & key);

    /*
    * No preconditions
    * 
    * Postconditions:
    *  - an iteration ticket is returned, to be passed to iterate()
    */
    size_t begin_iteration();

    /*
    * Preconditions:
    *  - ticket was previously returned by begin_iteration, and is still valid
    *
    * Postcodition:
    *  - if false is returned, this ticket has completed iteration, and
    *     is no longer valid. iterate_callback will not be called.
    *
    *  - if true is returned, iterate_callback will be called from
    *     persister's io_service, and [1, max_elements_per_callback]
    *     records will be returned by-argument
    */ 
    bool iterate(iterate_callback_t &&, size_t ticket);


    size_t get_layer_count() const
    { return _layers.size(); }

    const rolling_hash & get_layer(size_t index) const;

private:
    
    void on_get(const callback_t &, const std::string &);

    void on_put(const callback_t &, const merge_callback_t &,
        const std::string &, spb::PersistedRecord_ptr_t);

    void on_drop(const callback_t &, const std::string &);

    void on_iterate(const iterate_callback_t &, size_t);

    bool make_room(size_t, size_t, record::offset_t, record::offset_t, size_t);

    std::vector<rolling_hash*> _layers;

    struct iterator {
        enum {
            DEAD,
            BEGIN,
            LIVE,
            END
        } state;
        size_t layer;
        const record * rec;
    };
    std::vector<iterator> _iterators;

    spinlock _iterators_lock;

    std::vector<spb::PersistedRecord_ptr_t> _tmp_record_vec;

    core::proactor_ptr_t _proactor;
    boost::asio::strand _strand;

    size_t _max_iter_records;
    size_t _max_rotations;
};

}
}

#endif

