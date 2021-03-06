package proxy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

// msgReader is a helper that reads values from a PostgreSQL message.
type msgReader struct {
	reader            *bufio.Reader
	buf               [128]byte
	msgBytesRemaining int32
	err               error
}

// Err returns any error that the msgReader has experienced
func (r *msgReader) Err() error {
	return r.err
}

// fatal tells r that a Fatal error has occurred
func (r *msgReader) fatal(err error) {
	r.err = err
}

// rxMsg reads the type and size of the next message.
func (r *msgReader) rxMsg() (t byte, err error) {
	if r.err != nil {
		return 0, err
	}
	if r.msgBytesRemaining > 0 {
		io.CopyN(ioutil.Discard, r.reader, int64(r.msgBytesRemaining))
	}
	b := r.buf[0:5]
	fmt.Println("1")
	min := len(b)
	var n int
	fmt.Println(len(b))
	for n < min && err == nil {
		var nn int
		fmt.Println("2")
		nn, err = r.reader.Read(b[n:])
		fmt.Println("3")
		n += nn
	}
	fmt.Println("4")
	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	// a, err := io.ReadAtLeast(r.reader, b, len(b))
	// ReadFull(r.reader, b)
	fmt.Printf("a: %#v\n", n)
	fmt.Println(err)
	if r.err != nil {
		fmt.Println(err)
	}
	r.msgBytesRemaining = int32(binary.BigEndian.Uint32(b[1:])) - 4
	return b[0], err
}

func (r *msgReader) readByte() byte {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 1
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		r.fatal(err)
		return 0
	}

	return b
}

func (r *msgReader) readInt16() int16 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 2
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:2]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int16(binary.BigEndian.Uint16(b))
}

func (r *msgReader) readInt32() int32 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 4
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:4]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int32(binary.BigEndian.Uint32(b))
}

func (r *msgReader) readInt64() int64 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 8
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:8]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int64(binary.BigEndian.Uint64(b))
}

func (r *msgReader) readOid() Oid {
	return Oid(r.readInt32())
}

// readCString reads a null terminated string
func (r *msgReader) readCString() string {
	if r.err != nil {
		return ""
	}

	b, err := r.reader.ReadBytes(0)
	if err != nil {
		r.fatal(err)
		return ""
	}

	r.msgBytesRemaining -= int32(len(b))
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	return string(b[0 : len(b)-1])
}

// readString reads count bytes and returns as string
func (r *msgReader) readString(count int32) string {
	if r.err != nil {
		return ""
	}

	r.msgBytesRemaining -= count
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	var b []byte
	if count <= int32(len(r.buf)) {
		b = r.buf[0:int(count)]
	} else {
		b = make([]byte, int(count))
	}

	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return ""
	}

	return string(b)
}

// readBytes reads count bytes and returns as []byte
func (r *msgReader) readBytes(count int32) []byte {
	if r.err != nil {
		return nil
	}

	r.msgBytesRemaining -= count
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return nil
	}

	b := make([]byte, int(count))

	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return nil
	}

	return b
}
