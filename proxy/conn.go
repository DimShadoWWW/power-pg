package proxy

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type CommandTag string

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (such as "CREATE TABLE") then it returns 0
func (ct CommandTag) RowsAffected() int64 {
	s := string(ct)
	index := strings.LastIndex(s, " ")
	if index == -1 {
		return 0
	}
	n, _ := strconv.ParseInt(s[index+1:], 10, 64)
	return n
}

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	Host              string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port              uint16 // default: 5432
	Database          string
	User              string // default: OS user name
	Password          string
	TLSConfig         *tls.Config // config for TLS connection -- nil disables TLS
	UseFallbackTLS    bool        // Try FallbackTLSConfig if connecting with TLSConfig fails. Used for preferring TLS, but allowing unencrypted, or vice-versa
	FallbackTLSConfig *tls.Config // config for fallback TLS connection (only used if UseFallBackTLS is true)-- nil disables TLS
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	conn             net.Conn      // the underlying TCP or unix domain socket connection
	lastActivityTime time.Time     // the last time the connection was used
	reader           *bufio.Reader // buffered reader to improve read performance
	wbuf             [1024]byte
	Pid              int32             // backend pid
	SecretKey        int32             // key to use to send a cancel query message to the server
	RuntimeParams    map[string]string // parameters that have been reported by the server
	// PgTypes            map[Oid]PgType    // oids to PgTypes
	config   ConnConfig // config used when establishing this connection
	TxStatus byte
	// preparedStatements map[string]*PreparedStatement
	notifications []*Notification
	alive         bool
	causeOfDeath  error
	// logger             Logger
	mr msgReader
	// fp                 *fastpath
}

type Notification struct {
	Pid     int32  // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Conn) processContextFreeMsg(t byte, r *msgReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	case emptyQueryResponse:
		return nil
	case notificationResponse:
		c.rxNotificationResponse(r)
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}
}

func (c *Conn) rxMsg() (t byte, r *msgReader, err error) {
	if !c.alive {
		return 0, nil, ErrDeadConn
	}

	t, err = c.mr.rxMsg()
	if err != nil {
		c.die(err)
	}

	c.lastActivityTime = time.Now()

	return t, &c.mr, err
}

// func (c *Conn) rxAuthenticationX(r *msgReader) (err error) {
// 	switch r.readInt32() {
// 	case 0: // AuthenticationOk
// 	case 3: // AuthenticationCleartextPassword
// 		err = c.txPasswordMessage(c.config.Password)
// 	case 5: // AuthenticationMD5Password
// 		salt := r.readString(4)
// 		digestedPassword := "md5" + hexMD5(hexMD5(c.config.Password+c.config.User)+salt)
// 		err = c.txPasswordMessage(digestedPassword)
// 	default:
// 		err = errors.New("Received unknown authentication message")
// 	}
//
// 	return
// }

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Conn) rxParameterStatus(r *msgReader) {
	key := r.readCString()
	value := r.readCString()
	c.RuntimeParams[key] = value
}

func (c *Conn) rxErrorResponse(r *msgReader) (err PgError) {
	for {
		switch r.readByte() {
		case 'S':
			err.Severity = r.readCString()
		case 'C':
			err.Code = r.readCString()
		case 'M':
			err.Message = r.readCString()
		case 'D':
			err.Detail = r.readCString()
		case 'H':
			err.Hint = r.readCString()
		case 'P':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.Position = int32(n)
		case 'p':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.InternalPosition = int32(n)
		case 'q':
			err.InternalQuery = r.readCString()
		case 'W':
			err.Where = r.readCString()
		case 's':
			err.SchemaName = r.readCString()
		case 't':
			err.TableName = r.readCString()
		case 'c':
			err.ColumnName = r.readCString()
		case 'd':
			err.DataTypeName = r.readCString()
		case 'n':
			err.ConstraintName = r.readCString()
		case 'F':
			err.File = r.readCString()
		case 'L':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.Line = int32(n)
		case 'R':
			err.Routine = r.readCString()

		case 0: // End of error message
			if err.Severity == "FATAL" {
				c.die(err)
			}
			return
		default: // Ignore other error fields
			r.readCString()
		}
	}
}

func (c *Conn) rxBackendKeyData(r *msgReader) {
	c.Pid = r.readInt32()
	c.SecretKey = r.readInt32()
}

func (c *Conn) rxReadyForQuery(r *msgReader) {
	c.TxStatus = r.readByte()
}

func (c *Conn) rxRowDescription(r *msgReader) (fields []FieldDescription) {
	fieldCount := r.readInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.readCString()
		f.Table = r.readOid()
		f.AttributeNumber = r.readInt16()
		f.DataType = r.readOid()
		f.DataTypeSize = r.readInt16()
		f.Modifier = r.readInt32()
		f.FormatCode = r.readInt16()
	}
	return
}

func (c *Conn) rxParameterDescription(r *msgReader) (parameters []Oid) {
	// Internally, PostgreSQL supports greater than 64k parameters to a prepared
	// statement. But the parameter description uses a 16-bit integer for the
	// count of parameters. If there are more than 64K parameters, this count is
	// wrong. So read the count, ignore it, and compute the proper value from
	// the size of the message.
	r.readInt16()
	parameterCount := r.msgBytesRemaining / 4

	parameters = make([]Oid, 0, parameterCount)

	for i := int32(0); i < parameterCount; i++ {
		parameters = append(parameters, r.readOid())
	}
	return
}

func (c *Conn) rxNotificationResponse(r *msgReader) {
	n := new(Notification)
	n.Pid = r.readInt32()
	n.Channel = r.readCString()
	n.Payload = r.readCString()
	c.notifications = append(c.notifications, n)
}

func (c *Conn) die(err error) {
	c.alive = false
	c.causeOfDeath = err
	c.conn.Close()
}
