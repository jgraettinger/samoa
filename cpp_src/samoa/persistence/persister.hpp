#ifndef SAMOA_PERSISTENCE_PERSISTER_HPP
#define SAMOA_PERSISTENCE_PERSISTER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/persistence/rolling_hash/fwd.hpp"
#include "samoa/persistence/rolling_hash/element.hpp"
#include "samoa/datamodel/merge_func.hpp"
#include "samoa/request/fwd.hpp"
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

    typedef boost::function<void(rolling_hash::element)
    > iterate_callback_t;

    typedef boost::function<void(
        const boost::system::error_code &,
        const datamodel::merge_result &)
    > put_callback_t;

    typedef boost::function<bool(const request::state_ptr_t &)
    > record_upkeep_callback_t;


    persister();
    virtual ~persister();

    size_t layer_count() const
    { return _layers.size(); }

    const rolling_hash::hash_ring & layer(size_t index) const;

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
     */ 
    void iteration_next(iterate_callback_t &&, unsigned ticket);

    void put(put_callback_t &&,
        datamodel::merge_func_t &&,
        const std::string & key, //referenced
        const spb::PersistedRecord &, // referenced, remote record
        spb::PersistedRecord &); // referenced, local record

    const record_upkeep_callback_t & record_upkeep_callback() const
    { return _record_upkeep; }

    void set_record_upkeep_callback(const record_upkeep_callback_t &);

    unsigned max_on_demand_compactions() const
    { return 10; }

private:

    struct iterator {
        enum {
        	DEAD,
        	IDLE,
        	POSTED
        } state;

        size_t layer;
        const rolling_hash::packet * packet;
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

    record_upkeep_callback_t _record_upkeep;

    core::proactor_ptr_t _proactor;
    boost::asio::strand _strand;
};

}
}

#endif

