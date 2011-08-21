#ifndef SAMOA_PERSISTENCE_PERSITER_HPP
#define SAMOA_PERSISTENCE_PERSITER_HPP

#include "samoa/persistence/fwd.hpp"
#include "samoa/persistence/record.hpp"
#include "samoa/core/fwd.hpp"
#include "samoa/spinlock.hpp"
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

    /*
    Put:
        * Call persister::put(put_callback, merge_callback, key, Record_ptr_t record)

        * If record exists:
          - merge_callback(Record_ptr_t current_record, Record_ptr_t new_record);
          - put_callback(ec, Record_ptr_t new_record);
             - new_record is null iff put fails
       
    Get:
        * Call persister::get(get_callback, key);

        get_callback(ec, Record_ptr_t current_record)
          - current_record is null if record doesn't exist
    * /

    / * \brief Signature for get_callback
    *
    *  Argument record is null if a record under the key doesn't exist.
    * /
    typedef boost::function<
        void (const boost::system::error_code &, const Record_ptr_t &)
    > get_callback_t;

    / * \brief Signature for merge_callback
    *
    *  Precondition: A put operation is in progress, but an existing record exists under the key
    *  An existing  
    * /
    typedef boost::function<
        bool (const samoa::core::protobuf::Record &, samoa::core::protobuf::Record &)
    > merge_callback_t;

    / * \brief Signature for put_callback
    *
    * Argument 
    */

    typedef boost::function<
        void (const boost::system::error_code &, const record *)
    > get_callback_t;

    typedef boost::function<
        bool (const boost::system::error_code &, const record *, record *)
    > put_callback_t;

    typedef boost::function<
        bool (const boost::system::error_code &, const record *)
    > drop_callback_t;

    typedef boost::function<
        void (const boost::system::error_code &, const std::vector<const record*> &)
    > iterate_callback_t;

    persister();
    virtual ~persister();

    void add_heap_hash(size_t storage_size, size_t index_size);

    void add_mapped_hash(std::string file, size_t storage_size, size_t index_size);

    void get(get_callback_t &&, const std::string & key);

    void put(put_callback_t &&, const std::string & key, size_t value_length);

    void drop(drop_callback_t &&, const std::string & key);

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
    
    void on_get(const std::string &, const get_callback_t &);

    void on_put(const std::string &, size_t, const put_callback_t &);

    void on_drop(const std::string &, const drop_callback_t &);

    void on_iterate(size_t, const iterate_callback_t &);

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

    std::vector<const record *> _tmp_record_vec;

    core::proactor_ptr_t _proactor;
    boost::asio::strand _strand;

    size_t _max_iter_records;
    size_t _max_rotations;
};

}
}

#endif

