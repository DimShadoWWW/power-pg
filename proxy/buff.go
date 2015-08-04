package proxy

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/lib/pq/oid"
)

// ReadBuf byte array helper
type ReadBuf []byte

// Int32 get 4 bytes as int32
func (b *ReadBuf) Int32() (n int) {
	n = int(int32(binary.BigEndian.Uint32(*b)))
	*b = (*b)[4:]
	return
}

// Oid get 4 bytes as oid
func (b *ReadBuf) Oid() (n oid.Oid) {
	n = oid.Oid(binary.BigEndian.Uint32(*b))
	*b = (*b)[4:]
	return
}

// Int16 N.B: this is actually an unsigned 16-bit integer, unlike int32
func (b *ReadBuf) Int16() (n int) {
	n = int(binary.BigEndian.Uint16(*b))
	*b = (*b)[2:]
	return
}

func (b *ReadBuf) String() string {
	i := bytes.IndexByte(*b, 0)
	if i < 0 {
		log.Fatalf("invalid message format; expected string terminator")
	}
	s := (*b)[:i]
	*b = (*b)[i+1:]
	return string(s)
}

// Next get next N bytes
func (b *ReadBuf) Next(n int) (v []byte) {
	v = (*b)[:n]
	*b = (*b)[n:]
	return
}

// Byte get one byte
func (b *ReadBuf) Byte() byte {
	return b.Next(1)[0]
}
