import sys
import logging
import gevent
import getty
import random
import samoa.server
import samoa.remote_server
import samoa.command

logging.basicConfig(level = logging.DEBUG, stream = sys.stdout)

port = random.randint(2000, 65000)

inj = getty.Injector()
inj.bind_instance(samoa.meta_db.MetaDBPath,
    '/home/johng/samoa/server_1.db')
inj.bind_instance(getty.Injector, inj)
inj.bind_instance(samoa.Config('port'), port)


meta_db = inj.get_instance(samoa.meta_db.MetaDB)

server = inj.get_instance(samoa.Server)
gevent.spawn(server.service_loop)

remote_srv = samoa.remote_server.RemoteServer('localhost', port)

remote_srv.wait_until_online()
print remote_srv.forward(samoa.command.GET('foobar')).get()


