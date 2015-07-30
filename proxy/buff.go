package proxy

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/lib/pq/oid"
)

type readBuf []byte

func (b *readBuf) int32() (n int) {
	n = int(int32(binary.BigEndian.Uint32(*b)))
	*b = (*b)[4:]
	return
}

func (b *readBuf) oid() (n oid.Oid) {
	n = oid.Oid(binary.BigEndian.Uint32(*b))
	*b = (*b)[4:]
	return
}

// N.B: this is actually an unsigned 16-bit integer, unlike int32
func (b *readBuf) int16() (n int) {
	n = int(binary.BigEndian.Uint16(*b))
	*b = (*b)[2:]
	return
}

func (b *readBuf) string() string {
	i := bytes.IndexByte(*b, 0)
	if i < 0 {
		log.Fatalf("invalid message format; expected string terminator")
	}
	s := (*b)[:i]
	*b = (*b)[i+1:]
	return string(s)
}

func (b *readBuf) next(n int) (v []byte) {
	v = (*b)[:n]
	*b = (*b)[n:]
	return
}

func (b *readBuf) byte() byte {
	return b.next(1)[0]
}
