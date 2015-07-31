package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/DimShadoWWW/power-pg/common"
)

var (
	connid = uint64(0)
)

// Start function
func Start(localHost, remoteHost *string, powerCallback common.Callback) {
	fmt.Printf("Proxying from %v to %v\n", localHost, remoteHost)

	localAddr, remoteAddr := getResolvedAddresses(localHost, remoteHost)
	listener := getListener(localAddr)

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			fmt.Printf("Failed to accept connection '%s'\n", err)
			continue
		}
		connid++

		p := &proxy{
			lconn:  *conn,
			laddr:  localAddr,
			raddr:  remoteAddr,
			erred:  false,
			errsig: make(chan bool),
			prefix: fmt.Sprintf("Connection #%03d ", connid),
		}
		go p.start(powerCallback)
	}
}

func getResolvedAddresses(localHost, remoteHost *string) (*net.TCPAddr, *net.TCPAddr) {
	laddr, err := net.ResolveTCPAddr("tcp", *localHost)
	check(err)
	raddr, err := net.ResolveTCPAddr("tcp", *remoteHost)
	check(err)
	return laddr, raddr
}

func getListener(addr *net.TCPAddr) *net.TCPListener {
	listener, err := net.ListenTCP("tcp", addr)
	check(err)
	return listener
}

type proxy struct {
	sentBytes     uint64
	receivedBytes uint64
	laddr, raddr  *net.TCPAddr
	lconn, rconn  net.TCPConn
	erred         bool
	errsig        chan bool
	prefix        string
}

func (p *proxy) err(s string, err error) {
	if p.erred {
		return
	}
	if err != io.EOF {
		warn(p.prefix+s, err)
	}
	p.errsig <- true
	p.erred = true
}

func (p *proxy) start(powerCallback common.Callback) {
	// defer p.lconn.conn.Close()
	//connect to remote
	rconn, err := net.DialTCP("tcp", nil, p.raddr)
	if err != nil {
		p.err("Remote connection failed: %s", err)
		return
	}
	p.rconn = *rconn
	// p.rconn.alive = true
	// defer p.rconn.conn.Close()
	//bidirectional copy
	go p.pipe(p.lconn, p.rconn, powerCallback)
	go p.pipe(p.rconn, p.lconn, nil)
	//wait for close...
	<-p.errsig
}

func (p *proxy) pipe(src, dst net.TCPConn, powerCallback common.Callback) {
	//data direction
	islocal := src == p.lconn
	//directional copy (64k buffer)
	buff := make(readBuf, 0xffff)
	newPacket := true
	var msg string
	remainingBytes := 0
	if islocal {
		for {
			if remainingBytes == 0 {
				newPacket = true
			}
			var r readBuf
			n, err := src.Read(buff)
			if err != nil {
				p.err("Read failed '%s'\n", err)
				return
			}
			fmt.Printf("Readed bytes: %d\n", n)
			b := buff[:n]
			//write out result
			n, err = dst.Write(b)
			if err != nil {
				p.err("Write failed '%s'\n", err)
				return
			}

			r = buff[:n]
			// fmt.Printf("%#v", string(buff[:n]))
			fmt.Printf("Remaining bytes: %d\n", remainingBytes)
			if remainingBytes > 0 {
				if remainingBytes <= n {
					fmt.Println("1")
					newPacket = true
					msg = msg + string(r.next(remainingBytes))
					remainingBytes = n - remainingBytes
					fmt.Println("msg: ", string(msg))
				} else {
					fmt.Println("2")
					newPacket = false
					msg = msg + string(r.next(remainingBytes))
					remainingBytes = remainingBytes - n
				}
			}
			fmt.Println("3")
		NewP:
			fmt.Println("4")
			if newPacket && len(r) > 0 {
				fmt.Println("5")
				remainingBytes = 0
				newPacket = false
				fmt.Println("msg 0 : ", string(msg))
				msg = ""
				t := r.byte()
				n = n - 1
				fmt.Println("t: ", string(t))
				switch t {
				case query:
					// c.rxReadyForQuery(r)
					remainingBytes = r.int32()
					remainingBytes = remainingBytes - 4
					if remainingBytes > 0 {
						if remainingBytes <= n {
							newPacket = true
							msg = msg + string(r.next(remainingBytes))
							remainingBytes = n - remainingBytes
							fmt.Println("msg: ", string(msg))
							// fmt.Println(msg)
							goto NewP
						} else {
							newPacket = false
							msg = msg + string(r.next(remainingBytes))
							remainingBytes = remainingBytes - n
						}
						fmt.Printf("Remaining bytes: %d\n", remainingBytes)
					}
				// case rowDescription:
				// case dataRow:
				// case bindComplete:
				// case commandComplete:
				// 	commandTag = CommandTag(r.readCString())
				default:
					// if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
					// 	softErr = e
					// }
				}
			}
			// r = append(r, buff[:]...)

			// fmt.Println("a")
			// c := src
			// c.reader = bufio.NewReader(src.conn)
			// c.mr.reader = c.reader
			//
			// var t byte
			// var r *msgReader
			// fmt.Println("b")
			// t, r, err := c.rxMsg()
			// fmt.Println("c")
			// if err != nil {
			// 	fmt.Println(err)
			// 	return
			// }
			// fmt.Println("d")
			//
			// fmt.Printf("t: %#v\n", t)

			// n, err := src.Read(buff)
			// if err != nil {
			// 	p.err("Read failed '%s'\n", err)
			// 	return
			// }
			// b := buff[:n]
			// //show output
			//
			//
			// b = getModifiedBuffer(b, powerCallback)
			// n, err = dst.Write(b)
			// //
			// //write out result
			// n, err = dst.Write(b)
			// if err != nil {
			// 	p.err("Write failed '%s'\n", err)
			// 	return
			// }
		}
	} else {
		for {
			n, err := src.Read(buff)
			if err != nil {
				p.err("Read failed '%s'\n", err)
				return
			}
			b := buff[:n]
			//write out result
			n, err = dst.Write(b)
			if err != nil {
				p.err("Write failed '%s'\n", err)
				return
			}
			if err != nil {
				p.err("Write failed '%s'\n", err)
				return
			}
		}
	}
}

func getModifiedBuffer(buffer []byte, powerCallback common.Callback) []byte {
	if powerCallback == nil || len(buffer) < 1 || string(buffer[0]) != "Q" || string(buffer[5:11]) != "power:" {
		return buffer
	}
	query := powerCallback(string(buffer[5:]))
	return makeMessage(query)
}

func makeMessage(query string) []byte {
	queryArray := make([]byte, 0, 6+len(query))
	queryArray = append(queryArray, 'Q', 0, 0, 0, 0)
	queryArray = append(queryArray, query...)
	queryArray = append(queryArray, 0)
	binary.BigEndian.PutUint32(queryArray[1:], uint32(len(queryArray)-1))
	return queryArray

}

func check(err error) {
	if err != nil {
		warn(err.Error())
		os.Exit(1)
	}
}

func warn(f string, args ...interface{}) {
	fmt.Printf(f+"\n", args...)
}
