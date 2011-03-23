
import samoa.core.protobuf
import samoa.command.error
import samoa.command.ping
import samoa.command.shutdown
import samoa.command.cluster_state
import samoa.command.declare_table
import samoa.command.drop_table

import _server

class Protocol(_server.Protocol):

    def __init__(self):
        _server.Protocol.__init__(self)

        self.set_command_handler(
            samoa.core.protobuf.CommandType.ERROR,
            samoa.command.error.ErrorHandler())
        self.set_command_handler(
            samoa.core.protobuf.CommandType.PING,
            samoa.command.ping.PingHandler())
        self.set_command_handler(
            samoa.core.protobuf.CommandType.SHUTDOWN,
            samoa.command.shutdown.ShutdownHandler())
        self.set_command_handler(
            samoa.core.protobuf.CommandType.CLUSTER_STATE,
            samoa.command.cluster_state.ClusterStateHandler())
        self.set_command_handler(
            samoa.core.protobuf.CommandType.DECLARE_TABLE,
            samoa.command.declare_table.DeclareTableHandler())
        self.set_command_handler(
            samoa.core.protobuf.CommandType.DROP_TABLE,
            samoa.command.drop_table.DropTableHandler())

