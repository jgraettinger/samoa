
DATA_SCHEMA = """
create if not exists table entries (
    key           str not null primary key,
    value         str not null,
    version_clock str not null,
    expire        integer
);

create index if not exists entries_expire
    on entries (expire);
"""

SERVERS_SCHEMA = """
create table if not exists servers (
    server_id       integer primary key auto_increment,
    
    server_uid      str not null,
    datacenter_uid  str not null,
    
    internal_host   str not null,
    external_host   str not null,
    port            integer not null,
    
    unique(server_uid)
);
"""

PARTITIONS_SCHEMA = """
create table if not exists partitions (
    partition_id         integer primary key auto_increment,
    
    parent_partition_id  integer,
    partition_ind        integer not null,
    
    partition_uid   str not null,
    server_id       integer not null,
    
    unique(parent_partition_id, partition_ind),
    unique(partition_uid)
);
"""

