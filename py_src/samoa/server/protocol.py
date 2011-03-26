
import getty

import samoa.command.error
import samoa.command.ping
import samoa.command.shutdown
import samoa.command.cluster_state
import samoa.command.declare_table
import samoa.command.drop_table
import samoa.command.create_partition
import samoa.command.drop_partition

import samoa.command as cmd
import samoa.core.protobuf as proto

import _server

class Protocol(_server.Protocol):

    @getty.requires(
        error = cmd.error.ErrorHandler,
        ping = cmd.ping.PingHandler,
        shutdown = cmd.shutdown.ShutdownHandler,
        cluster_state = cmd.cluster_state.ClusterStateHandler,
        declare_table = cmd.declare_table.DeclareTableHandler,
        drop_table = cmd.drop_table.DropTableHandler,
        create_partition = cmd.create_partition.CreatePartitionHandler,
        drop_partition = cmd.drop_partition.DropPartitionHandler)
    def __init__(self,
           error,
           ping,
           shutdown,
           cluster_state,
           declare_table,
           drop_table,
           create_partition,
           drop_partition):

        _server.Protocol.__init__(self)

        self.set_command_handler(
            proto.CommandType.ERROR, error)
        self.set_command_handler(
            proto.CommandType.PING, ping)
        self.set_command_handler(
            proto.CommandType.SHUTDOWN, shutdown)
        self.set_command_handler(
            proto.CommandType.CLUSTER_STATE, cluster_state)
        self.set_command_handler(
            proto.CommandType.DECLARE_TABLE, declare_table)
        self.set_command_handler(
            proto.CommandType.DROP_TABLE, drop_table)
        self.set_command_handler(
            proto.CommandType.CREATE_PARTITION, create_partition)
        self.set_command_handler(
            proto.CommandType.DROP_PARTITION, drop_partition)
