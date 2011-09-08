
class request_state
{
public:

    typedef boost::shared_ptr<request_state> ptr_t;

    typedef std::list<partition_ptr_t> peer_partitions_t;
    typedef std::list<samoa::client::server::ptr_t> peer_servers_t;

    const client_ptr_t & get_client()
    { return _client; }

    const peer_set_ptr_t & get_peer_set()
    { return _peer_set; }

    const table_ptr_t & get_table()
    { return _table; }

    const std::string & get_key()
    { return _key; }

    const local_partition_ptr_t & get_primary_partition()
    { return _primary_partition; }

    const peer_partitions_t & get_peer_partitions()
    { return _peer_partitions; }

    const peer_servers_t & get_peer_servers()
    { return _peer_servers; } 

    spb::PersistedRecord & get_local_record()
    { return _local_record; }

    spb::PersistedRecord & get_remote_record()
    { return _remote_record; }

    unsigned get_quorom_count()
    { return _quorom_count; }

    unsigned get_peer_error_count()
    { return _error_count; }

    unsigned get_peer_success_count()
    { return _success_count; }

    core::zero_copy_input_adapter & get_zci_adapter()
    { return _zci_adapter; }

    core::zero_copy_output_adapter & get_zco_adapter()
    { return _zco_adapter; }

    const core::io_service_ptr_t & get_io_service()
    { return _io_srv; }

    static ptr_t extract_request_state(const client_ptr_t &);

private:

    client_ptr_t _client;
    peer_set_ptr_t _peer_set;
    table_ptr_t _table;
    std::string _key;

    local_partition_ptr_t _primary_partition;
    peer_partitions_t _peer_partitions;
    peer_servers_t _peer_servers;

    spb::PersistedRecord _local_record;
    spb::PersistedRecord _remote_record;

    unsigned _quorom_count;
    unsigned _error_count;
    unsigned _success_count;

    core::zero_copy_input_adapter  _zci_adapter;
    core::zero_copy_output_adapter _zco_adapter;

    core::io_service_ptr_t _io_srv;
};

/*

merge_result
{
    bool local_was_updated;
    bool remote_is_stale;
}

merge_result consistent_merge(local, remote)


get_blob.handle(client):

    rs = load_request_state(client)

    if quorum > 1:

        for peer in rs.peer_partitions:
            peer.replication_read(key, get_blob.on_peer_response)
    else:
        persister.get(rs.key(), get_blob.on_get)

get_blob.on_peer_response():

    if response.is_error():
        rs.peer_error += 1
            if response.is_error():
                r_s.peer_error += 1
            else:
                r_s.peer_success += 1

            if r_s.peer_success > quorum:
                # we've already returned to client, and are just waiting
                #  for other peer callouts to complete 
                return

            if (r_s.peer_error + r_s.peer_success) == r_s.peer_partitions.size():
                client.send_error()
                return

            r_s.remote_record.ParseFrom(response)
            r_s.table->get_consistent_merge()(r_s.local_record, r_s.remote_record);

            if r_s.peer_success == quorum:

                if r_s.remote_record.cluster_clock:

                persister.put(r_s.key, r_s.local

                    on_peer_read_complete(r_s)
            

        if r_s.remote_record.has_cluster_clock():

            persister.put(r_s.key, r_s.local, r_s.remote, consistent_merge)

            // note: don't care if a value was actually written

            return_blob_record(r_s)

        persister.get(r_s.key, r_s.local)

        return_blob_record(r_s)

    r_s.finish_response()


set_blob():

    r_s = load_request_state()

    build_remote_record(r_s)


request_state:

    client
    peer_set
    table
    key
    primary_partition
    peer_partitions
    peer_servers
    local_record
    remote_record

    quorum_count
    peer_success
    peer_error

    serial_io_service (synchronization)
    zero_copy_input_adapter
    zero_copy_output_adapter

    * finish_response() is delegated, & client is set to nullptr
    * key accessed after client.finish_response() (must be copied out)
    * peer_partitions may be populated by table.route_ring_position,
      or directly from a replication request

    * persister/datatype needs to support multiple kinds of merge() callbacks
        - a 'non-modifying' merge, which performs datatype-specific eventual consistency
        - a 'mutating' merge (eg, set_blob ticks clock & replaces value)
*/
