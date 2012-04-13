package main

import (
	"fmt"
	"net"
)

type Connection struct {
	connection *net.TCPConn
}

func Connect(hostname string, port uint16) (conn Connection, err error) {

	addrStr := fmt.Sprintf("%v:%d", hostname, port)

	addr, err := net.ResolveTCPAddr("tcp", addrStr)
	if err != nil {
		return
	}
	tcpConn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return
	}
	return Connection{tcpConn}, err
}

func (self *Connection) Request(payload []byte) (result string, err error) {

	self.connection.Write(payload)
	_, err = self.connection.Read(payload)

	if err != nil {
		return
	}
	return string(payload), err
}

func main() {

	conn, err := Connect("google.com", 80)
	if err != nil {
		panic(err)
	}

	result, err := conn.Request([]byte("GET / HTTP/1.0\r\n\r\n"))
	if err != nil {
		panic(err)
	}

	println(string(result))
}
