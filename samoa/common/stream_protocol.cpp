

typedef std::vector<boost::regex> regex_set;
typedef boost::shared_ptr<py_regex_set> regex_set_ptr;


bpl::object py_compile_regex_set(bpl::object )
{



}

void py_read_regex(stream_protocol::ptr_t,  size_t max_match_length)
{
    Py_END_ALLOW_THREADS;

    PyGreenlet * glet = PyGreenlet_GetCurrent();

}

void py_on_regex_read(
    stream_protocol::ptr_t & sp,
    PyGreenlet * glet,
    const boost::system::error_code & ec,
    const stream_protocol::match_results_t & match,
    size_t matched_index)
{
    Py_END_ALLOW_THREADS;

    // unpack match into new python tuple

    PyGreenlet_Switch(

}



