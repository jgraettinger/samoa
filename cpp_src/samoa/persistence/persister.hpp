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
        const datamodel::merge_result &,
        const core::murmur_checksummer::checksum_t &)
    > put_callback_t;

    typedef boost::function<void(
        const request::state_ptr_t &,
        const core::murmur_checksummer::checksum_t & old_checksum,
        const core::murmur_checksummer::checksum_t & new_checksum)
    > upkeep_callback_t;

    typedef boost::function<void(void)
    > bottom_up_compaction_callback_t;

    persister();
    virtual ~persister();

    size_t layer_count() const
    { return _layers.size(); }

    const rolling_hash::hash_ring & layer(size_t index) const;

    void add_heap_hash_ring(uint32_t storage_size, uint32_t index_size);

    void add_mapped_hash_ring(
        const std::string & file,
        uint32_t storage_size,
        uint32_t index_size);

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
    size_t iteration_begin();

    /*!
     * Preconditions:
     *  - ticket was previously returned by begin_iteration, and is still valid
     */ 
    void iteration_next(iterate_callback_t &&, size_t ticket);

    void put(put_callback_t &&,
        datamodel::merge_func_t &&,
        const std::string & key, //referenced
        const spb::PersistedRecord &, // referenced, remote record
        spb::PersistedRecord &); // referenced, local record

    void set_prune_callback(const datamodel::prune_func_t &);

    void set_upkeep_callback(const upkeep_callback_t &);

    float max_compaction_factor() const
    { return 3.0; }

    void bottom_up_compaction(bottom_up_compaction_callback_t &&);

private:

    struct iterator {
        enum {
        	DEAD,
        	IDLE,
        	POSTED
        } state;

        size_t layer_ind;
        rolling_hash::packet * packet;
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

    void step_iterator(iterator &);

    template<typename PreRotateLambda>
    uint32_t top_down_compaction(const PreRotateLambda &);

    void on_bottom_up_compaction(
        const bottom_up_compaction_callback_t & callback);

    template<typename PreRotateLambda>
    uint32_t inner_compaction(size_t, const PreRotateLambda &);

    template<typename PreRotateLambda>
    uint32_t leaf_compaction(const PreRotateLambda &);

    void write_record_with_cached_sizes(
        const spb::PersistedRecord &, rolling_hash::element &);

    core::proactor_ptr_t _proactor;
    boost::asio::strand _strand;

    std::vector<rolling_hash::hash_ring *> _layers;

    spinlock _iterators_lock;
    std::vector<iterator> _iterators;

    spinlock _prune_lock;
    datamodel::prune_func_t _prune;

    spinlock _upkeep_lock;
    upkeep_callback_t _upkeep;
};

}
}

#endif

