
# enumeration of persisted data-types
BLOB_TYPE = 1
COUNT_TYPE = 2
MAP_TYPE = 3

data_types_int_to_str = {
    1: 'blob',
    2: 'count',
    3: 'map',
}
data_types_str_to_int = dict((v,k) for k,v in data_types_int_to_str.items())

