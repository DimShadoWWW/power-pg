package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/common"
	"github.com/op/go-logging"
)

var (
	connid = uint64(0)
)

// Pkg PostgreSQL package structure
type Pkg struct {
	Type    byte
	Content []byte
	Time    time.Time
}

// Start function
func Start(localHost, remoteHost *string, remotePort *string, msgBytes chan []byte, msgCh chan Pkg, recreate bool, log *logging.Logger) {
	fmt.Printf("Proxying from %v to %v\n", localHost, remoteHost)

	localAddr, remoteAddr := getResolvedAddresses(localHost, remoteHost, remotePort)
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
			log:    log,
		}
		go p.start(msgBytes, msgCh, recreate)
	}
}

func getResolvedAddresses(localHost, remoteHost, remotePort *string) (*net.TCPAddr, *net.TCPAddr) {
	laddr, err := net.ResolveTCPAddr("tcp", *localHost)
	check(err)
	raddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", *remoteHost, *remotePort))
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
	result        *[]string
	log           *logging.Logger
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

func (p *proxy) start(msgBytes chan []byte, msgCh chan Pkg, recreate bool) {
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
	go p.pipe(p.lconn, p.rconn, msgBytes, msgCh, recreate, p.log)
	go p.pipe(p.rconn, p.lconn, nil, nil, recreate, p.log)
	//wait for close...
	<-p.errsig
}

func (p *proxy) pipe(src, dst net.TCPConn, msgBytes chan []byte, msgCh chan Pkg, recreate bool, log *logging.Logger) {
	//data direction
	islocal := src == p.lconn
	//directional copy (64k buffer)
	buff := make(ReadBuf, 0xffff)

	// spaces := regexp.MustCompile("[\n\t ]+")
	if islocal {
		for {
			remainingBytes := 0
			var r ReadBuf

			// fmt.Println("1111")
			n, err := src.Read(buff)
			if err != nil {
				p.err("Read failed '%s'\n", err)
				return
			}

			// if msgBytes != nil {
			// log.Debug("Readed bytes: %d\n", n)
			// }
			b := buff[:n]
			// log.Info("Readed: %v\n", b)
			msgBytes <- b
			//write out result
			if !recreate {
				n, err = dst.Write(b)
				if err != nil {
					p.err("Write failed '%s'\n", err)
					return
				}
			}
			r = buff[:n]
			// log.Debug("PostgreSQL full message: %s\n", string(r))
			// // log.Debug("Remaining bytes: %d\n", remainingBytes)
			// log.Debug("len(r) : %v\n", len(r))
			// fmt.Println("3")
			if len(r) > 4 {
				// fmt.Println("4")
				// log.Debug("2 Remaining bytes: %d\n", remainingBytes)

				var msg []byte
				// log.Debug("1 n: %d\n", n)
				t := r.Byte()
				// fmt.Println("t: ", string(t))
				switch t {
				// case 'Q', 'B', 'C', 'd', 'c', 'f', 'D', 'E', 'H', 'F', 'P', 'p', 'S', 'X':
				case 'B', 'P':
					log.Debug("PostgreSQL pkg type: %s\n", string(t))
					remainingBytes = r.Int32() - 4
					r = r[:remainingBytes]
					if remainingBytes < 4 {
						fmt.Println("ERROR: remainingBytes can't be less than 4 bytes if int32")
					} else {
						if remainingBytes > 0 {
							msg = append(msg, r.Next(remainingBytes)[:]...)
							msgCh <- Pkg{
								Type:    t,
								Content: msg,
								Time:    time.Now(),
							}
						}
					}
				case 'Q':
					if !bytes.Contains(r, []byte("DEALLOCATE")) {
						log.Debug("PostgreSQL pkg type: %s\n", string(t))
						remainingBytes = r.Int32() - 4
						r = r[:remainingBytes]
						msgCh <- Pkg{
							Type:    t,
							Content: r,
							Time:    time.Now(),
						}
					}
				}
			}
			// fmt.Println("8")
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

func stripchars(str, chr string) string {
	return strings.Map(func(r rune) rune {
		if strings.IndexRune(chr, r) < 0 {
			return r
		}
		return -1
	}, str)
}
