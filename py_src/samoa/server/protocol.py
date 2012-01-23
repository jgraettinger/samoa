
import getty

import samoa.server.command.ping
import samoa.server.command.create_table
import samoa.server.command.alter_table
import samoa.server.command.drop_table
import samoa.server.command.create_partition
import samoa.server.command.drop_partition
import samoa.server.command.cluster_state
import samoa.server.command.get_blob
import samoa.server.command.set_blob
import samoa.server.command.replicate
import samoa.server.command.update_counter
import samoa.server.command.counter_value

import samoa.server.command as cmd
from samoa.core.protobuf import CommandType

import _server

class Protocol(_server.Protocol):

    @getty.requires(
        ping = cmd.ping.PingHandler,
        create_table = cmd.create_table.CreateTableHandler,
        alter_table = cmd.alter_table.AlterTableHandler,
        drop_table = cmd.drop_table.DropTableHandler,
        create_partition = cmd.create_partition.CreatePartitionHandler,
        drop_partition = cmd.drop_partition.DropPartitionHandler,
        cluster_state = cmd.cluster_state.ClusterStateHandler,
        get_blob = cmd.get_blob.GetBlobHandler,
        set_blob = cmd.set_blob.SetBlobHandler,
        replicate = cmd.replicate.ReplicateHandler,
        update_counter = cmd.update_counter.UpdateCounterHandler,
        counter_value = cmd.counter_value.CounterValueHandler,
    )
    def __init__(self,
           ping,
           create_table,
           alter_table,
           drop_table,
           create_partition,
           drop_partition,
           cluster_state,
           get_blob,
           set_blob,
           update_counter,
           counter_value,
           replicate,
           ):

        _server.Protocol.__init__(self)

        self.set_command_handler(
            CommandType.PING, ping)
        self.set_command_handler(
            CommandType.CREATE_TABLE, create_table)
        self.set_command_handler(
            CommandType.ALTER_TABLE, alter_table)
        self.set_command_handler(
            CommandType.DROP_TABLE, drop_table)
        self.set_command_handler(
            CommandType.CREATE_PARTITION, create_partition)
        self.set_command_handler(
            CommandType.DROP_PARTITION, drop_partition)
        self.set_command_handler(
            CommandType.CLUSTER_STATE, cluster_state)
        self.set_command_handler(
            CommandType.GET_BLOB, get_blob)
        self.set_command_handler(
            CommandType.SET_BLOB, set_blob)
        self.set_command_handler(
            CommandType.UPDATE_COUNTER, update_counter)
        self.set_command_handler(
            CommandType.COUNTER_VALUE, counter_value)
        self.set_command_handler(
            CommandType.REPLICATE, replicate)

