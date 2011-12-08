#ifndef SAMOA_PERSISTENCE_PERSISTER_HPP
#define SAMOA_PERSISTENCE_PERSISTER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/persistence/record.hpp"
#include "samoa/datamodel/merge_func.hpp"
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

    typedef boost::function<void(bool) // found
    > get_callback_t;

    typedef boost::function<bool(bool) // found
    > drop_callback_t;

    typedef boost::function<void(element)
    > iterate_callback_t;

    typedef boost::function<void(
        const boost::system::error_code &,
        const datamodel::merge_result &)
    > put_callback_t;

    persister();
    virtual ~persister();

    size_t get_layer_count() const
    { return _layers.size(); }

    const rolling_hash::hash_ring & get_layer(size_t index) const;

    void add_heap_hash_ring(unsigned storage_size, unsigned index_size);

    void add_mapped_hash_ring(const std::string & file,
        unsigned storage_size, unsigned index_size);

    void get(get_callback_t &&,
        const std::string & key, // referenced
        spb::PersistedRecord &); // referenced

    void drop(drop_callback_t &&,
        const std::string & key, // referenced
        spb::PersistedRecord &); // referenced

    /*!
     * No preconditions
     * 
     * @returns A non-zero iteration ticket, to be passed to iterate()
     */
    unsigned iteration_begin();

    /*!
     * Preconditions:
     *  - ticket was previously returned by begin_iteration, and is still valid
     *
     * Postcodition:
     *  - if false is returned, this ticket has completed iteration, and
     *     is no longer valid. iterate_callback will not be called.
     *
     *  - if true is returned, iterate_callback will be called from
     *     persister's io_service, and a record will be returned by-argument
     */ 
    void iteration_next(iterate_callback_t &&, unsigned ticket);

    void put(put_callback_t &&,
        datamodel::merge_func_t &&,
        const std::string & key, //referenced
        const spb::PersistedRecord &, // referenced, remote record
        spb::PersistedRecord &); // referenced, local record

private:

    struct iterator {
        enum {
        	DEAD,
        	IDLE,
        	POSTED
        } state;

        size_t layer;
        const packet * packet;
    };

    void on_get(const get_callback_t &,
        const std::string &,
        spb::PersistedRecord &);

    void on_drop(const drop_callback_t &,
        const std::string &,
        spb::PersistedRecord &);

    void on_iteration_next(const iterate_callback_t &, size_t);

    void on_put(const put_callback_t &,
        const datamodel::merge_func_t &,
        const std::string &,
        const spb::PersistedRecord &,
        spb::PersistedRecord &);

    std::vector<rolling_hash::hash_ring *> _layers;

    spinlock _iterators_lock;
    std::vector<iterator> _iterators;

    core::proactor_ptr_t _proactor;
    boost::asio::strand _strand;

    size_t _min_rotations;
    size_t _max_rotations;
};

}
}

#endif

