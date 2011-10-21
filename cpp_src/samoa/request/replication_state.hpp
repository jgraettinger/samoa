#ifndef SAMOA_REQUEST_REPLICATION_STATE_HPP
#define SAMOA_REQUEST_REPLICATION_STATE_HPP

namespace samoa {
namespace request {

class replication_state
{
public:

    replication_state();

    virtual ~replication_state();

    /*!
     * Sets the target quorum size of the replication.
     *
     * If quorum_count is 0, the quorum-count is implicitly all peers.
     *
     * Samoa will wait for successful replication responses from
     *  quorum-count peers, or all replication responses (successful
     *  or not) before responding to the client.
     */
    void set_quorum_count(unsigned quorum_count);

    /*!
     * Retrieves the requested quorum-count.
     *
     * If the set quorum-count was 0, the true quorum-count
     *  is reflected only after load_replication_state()
     */
    unsigned get_quorum_count() const
    { return _quorum_count; }

    /// Retrieves the number of successful peer replications
    unsigned get_peer_success_count() const
    { return _success_count; }

    /// Retrieves the number of failed peer replications
    unsigned get_peer_failure_count() const
    { return _failure_count; }

    /*!
     * Indicates whether one of peer_replication_failure() or
     * peer_replication_success() have returned True, and the
     * quorum has already been met (or failed).
     */
    bool is_replication_finished() const;

    /*!
     * \brief To be called on failed peer replication.
     *
     * Increments peer_failure_count, and returns true iff the client should be
     * responded to as a result of this specific completion. Eg, because we
     * haven't yet responded, and this completion was the last remaining
     * replication.
     * 
     * Note: Only one of peer_replication_failure() or
     *  peer_replication_success() will return True for a
     *  given replication_state.
     */
    bool peer_replication_failure();

    /*!
     *  \brief To be called on successful peer replication.
     *  
     * Increments peer_success, and returns true iff the client should be
     * responded to as a result of this specific completion. Eg, because
     * this completion caused us to meet the client-requested quorum, or
     * because this completion was the last remaining replication.
     *
     * Note: Only one of peer_replication_failure() or
     *  peer_replication_success() will return True for a
     *  given replication_state.
     */
    bool peer_replication_success();

    /*!
     * Initializes replication_state for a replication operation.
     *
     * \param replication_factor The _effective_ replication-factor
     *   of the table.
     */
    void load_replication_state(unsigned replication_factor);

    void reset_replication_state();

private:

    unsigned _replication_factor;
    unsigned _quorum_count;

    unsigned _failure_count;
    unsigned _success_count;
};

}
}

#endif

