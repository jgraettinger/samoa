
import sys
import logging
import gevent
import getty
import random

import samoa.model
import samoa.module
import samoa.server
import samoa.runtime.remote_server
import samoa.command

logging.basicConfig(level = logging.DEBUG, stream = sys.stdout)

port = random.randint(2000, 65000)

inj = samoa.module.Module(
    '/home/johng/samoa/server_1.db'
    ).configure(getty.Injector())

inj.bind_instance(getty.Config, port,
    with_annotation = 'port')


def make_test_table(meta):

    session = meta.session
    tab = samoa.model.Table('test_table')

    tab.local_partitions.append(
        samoa.model.LocalPartition('test_part1',
            0, '/tmp/test_part1.tab'))

    tab.local_partitions.append(
        samoa.model.LocalPartition('test_part2',
            (1<<31), '/tmp/test_part2.tab'))

    session.add(tab)
    session.commit()

try:
    make_test_table(inj.get_instance(samoa.model.Meta))
except:
    pass

server = inj.get_instance(samoa.server.Server)
gevent.spawn(server.service_loop)

remote_srv = samoa.runtime.remote_server.RemoteServer('localhost', port)

remote_srv.wait_until_online()
print remote_srv.forward(samoa.command.GET('test_table', 'foobar')).get()


