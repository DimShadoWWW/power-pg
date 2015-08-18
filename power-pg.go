package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"mime"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/proxy"
	"github.com/DimShadoWWW/power-pg/utils"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"github.com/parnurzeal/gorequest"
	"github.com/yosssi/gohtml"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// log "github.com/Sirupsen/logrus"

var (
	baseDir       = flag.String("b", "/", "directorio base para archivos")
	localHost     = flag.String("l", ":5432", "puerto local para escuchar")
	dbHostname    = flag.String("h", "localhost", "hostname del servidor PostgreSQL")
	dbPort        = flag.String("r", "5432", "puerto del servidor PostgreSQL")
	dbName        = flag.String("d", "dbname", "nombre de la base de datos")
	dbUsername    = flag.String("u", "username", "usuario para acceder al servidor PostgreSQL")
	dbPassword    = flag.String("p", "password", "password para acceder al servidor PostgreSQL")
	remoteService = flag.String("s", "", "http://localhost:8080/query")
	recreate      = flag.Bool("R", false, "recreacion de escenario")
	// messages      = []string{}
)

type msgStruct struct {
	Type    string
	Content string
}

type sqlStruct struct {
	Completed bool
	Printed   bool
	Template  string
	First     string
	Count     int
}

type sqlStructList map[string]sqlStruct

var (
	run      = make(chan bool)
	end      = make(chan bool)
	msgs     = make(chan string)
	msgBytes = make(chan []byte)
	msgCh    = make(chan proxy.Pkg)
	msgOut   = make(chan msgStruct, 100)

	db  *sql.DB
	log = logging.MustGetLogger("")
)

func main() {
	flag.Parse()

	logBackend := logging.NewLogBackend(os.Stdout, "", 0)
	logBackendFormatter := logging.NewBackendFormatter(logBackend,
		logging.MustStringFormatter("%{color}%{time:15:04:05} %{longfunc:.15s} ▶ %{level:.5s} %{id:03d}%{color:reset} %{message}"))
	logging.SetBackend(logBackend, logBackendFormatter)

	go func() {
		log.Debug(http.ListenAndServe(":6060", nil).Error())
	}()

	if *recreate {
		go recreateFunc()
		go recreateLogDoNothing()
	} else {
		if *remoteService != "" {
			go callURIs()
		}
		go recreateLogDump()
	}
	go baseLog()
	go logReport()
	go base()

	time.Sleep(time.Second * 3)
	run <- true
	proxy.Start(localHost, dbHostname, dbPort, msgBytes, msgCh, *recreate, log)
}

func getQueryModificada(queryOriginal string) string {
	log.Debug(queryOriginal)
	return queryOriginal
}

// Advanced Unicode normalization and filtering,
// see http://blog.golang.org/normalization and
// http://godoc.org/golang.org/x/text/unicode/norm for more
// details.
func stripCtlAndExtFromUnicode(str string) string {
	isOk := func(r rune) bool {
		return r < 32 || r >= 127
	}
	// The isOk filter is such that there is no need to chain to norm.NFC
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
	// This Transformer could also trivially be applied as an io.Reader
	// or io.Writer filter to automatically do such filtering when reading
	// or writing data anywhere.
	str, _, _ = transform.String(t, str)
	return str
}
func callURIs() {
	select {
	case <-run:
		inFile, _ := os.Open("canales_list.txt")
		defer inFile.Close()
		scanner := bufio.NewScanner(inFile)
		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			time.Sleep(time.Millisecond * 1500)

			// send message for new channel
			msgOut <- msgStruct{Type: "C", Content: scanner.Text()}

			resp, body, errs := gorequest.New().Get(fmt.Sprintf("%s%s", *remoteService, scanner.Text())).End()
			if errs != nil {
				log.Fatalf("log failed: %v", errs)
			}
			encoding := ""
			for _, item := range strings.Split(resp.Header.Get("Content-Type"), " ") {
				if strings.Contains(item, "charset=") {
					encoding = strings.Split(item, "=")[1]
				}
			}

			ct, _, _ := mime.ParseMediaType(http.DetectContentType([]byte(body)))
			mime := ""
			switch ct {
			case "text/html":
				mime = "html"
			case "application/xml", "text/xml":
				mime = "xml"
			case "application/xslt+xml":
				mime = "xslt"
			case "application/json":
				mime = "json"
			}

			if encoding != "" {
				msgOut <- msgStruct{Type: "B", Content: fmt.Sprintf("Codificacion: %s", encoding)}
			}

			if mime != "" {
				msgOut <- msgStruct{Type: "B", Content: fmt.Sprintf("Mime: %s", mime)}
			}

			msgOut <- msgStruct{Type: "O", Content: gohtml.Format(stripCtlAndExtFromUnicode(body))}
		}
	case <-end:
		log.Info("done")
		os.Exit(0)
	}
}

func retrieveROM(filename string) ([]byte, error) {
	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	stats, statsErr := file.Stat()
	if statsErr != nil {
		return nil, statsErr
	}

	size := int64(stats.Size())
	bytes := make([]byte, size)

	bufr := bufio.NewReader(file)
	_, err = bufr.Read(bytes)

	return bytes, err
}

func recreateFunc() {
	laddr, err := net.ResolveTCPAddr("tcp", *localHost)
	if err != nil {
		log.Critical("Local recreation's connection failed: %s", err)
		return
	}

	l, err := retrieveROM(fmt.Sprintf("%s/all.txt", *baseDir))
	if err != nil {
		panic(err)
	}
	listBytes := bytes.Split(l, []byte("ENDMSG"))
	time.Sleep(time.Second * 10)

	conn, err := net.DialTCP("tcp", nil, laddr)
	if err != nil {
		log.Critical("Local recreation's connection failed: %s", err)
		return
	}
	for _, msg := range listBytes {
		for <-run {
			time.Sleep(time.Millisecond * 500)
			_, err := conn.Write(msg)
			if err != nil {
				log.Critical("Write failed '%s'\n", err)
			}
		}
	}
}

func recreateLogDump() {
	f, err := os.OpenFile(fmt.Sprintf("%s/all.txt", *baseDir), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for msg := range msgBytes {
		// log.Debug(string(msg))
		log.Debug(".")

		_, err := f.Write(msg)
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}

		_, err = f.Write([]byte("ENDMSG"))
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}
	}
}

func recreateLogDoNothing() {
	for i := range msgBytes {
		log.Error(string(i))
	}
}

func baseLog() {
	f, err := os.OpenFile(fmt.Sprintf("%s/all2.txt", *baseDir), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for msg := range msgs {
		// log.Debug(msg)
		log.Debug("*")
		_, err := f.WriteString(fmt.Sprintf("%s\n", msg))
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}
	}
}

func logReport() {
	// var f *os.File

	spaces := regexp.MustCompile("[ \t\n]+")
	// pdo_stmt_ := regexp.MustCompile("pdo_stmt_[0-9a-fA-F]{8}")
	multipleSpaces := regexp.MustCompile("    ")
	fname := ""
	var sqlIndex sqlStructList
	for msg := range msgOut {
		log.Debug("+")
		switch msg.Type {
		// New file
		case "C":
			log.Debug("CHANNEL")
			sqlIndex = make(sqlStructList)
			fname = fmt.Sprintf("%s/reports/report-%s.md", *baseDir, msg.Content)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
			// c = 0
			if err != nil {
				panic(err)
			}
			_, err = f.WriteString(fmt.Sprintf("# %s\n", msg.Content))
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()

			// SQL Query
		case "M":
			log.Debug("SQL")
			m := spaces.ReplaceAll([]byte(msg.Content), []byte{' '})
			m = multipleSpaces.ReplaceAll(m, []byte{' '})
			sqlIdx := string(m[:20])
			log.Info("m %s\n", string(m))
			log.Info("sqlIdx %s\n", string(sqlIdx))
			for index, _ := range sqlIndex {
				fmt.Printf("%s : '%v'\n", index, sqlIndex[index].Printed)
			}
			if val, ok := sqlIndex[sqlIdx]; ok {
				log.Info("Exists")
				log.Info("sqlIndex[sqlIdx] %#s\n", sqlIndex[sqlIdx])

				// if val.Completed  {
				// 	m = []byte(val.Template)
				// } else {
				if !sqlIndex[sqlIdx].Completed && sqlIndex[sqlIdx].First != "" {
					log.Info("Not completed")
					sqlIndex[sqlIdx] = sqlStruct{
						First:     sqlIndex[sqlIdx].First,
						Completed: true,
						Printed:   false,
						Template:  utils.GetVariables(string(m), val.First),
						Count:     1,
					}
				} else {
					sqlIndex[sqlIdx] = sqlStruct{
						First:     sqlIndex[sqlIdx].First,
						Completed: true,
						Printed:   false,
						Template:  sqlIndex[sqlIdx].Template,
						Count:     sqlIndex[sqlIdx].Count + 1,
					}
				}
			} else {
				log.Info("Not exists")
				sqlIndex[sqlIdx] = sqlStruct{
					First:     string(m),
					Completed: false,
					Count:     0,
					Printed:   false,
				}
			}
			if sqlIndex[sqlIdx].Count <= 2 {
				var m1 = []byte("\n```sql,classoffset=1,morekeywords={XXXXXX},keywordstyle=\\color{OliveGreen},classoffset=0,\n")
				log.Info("Count %d", sqlIndex[sqlIdx].Count)
				if sqlIndex[sqlIdx].Completed && !sqlIndex[sqlIdx].Printed && sqlIndex[sqlIdx].Count == 2 {
					log.Info("Count %d == 2", sqlIndex[sqlIdx].Count)
					// log.Info("1")
					// go func() {
					// }()
					m1 = append(m1, []byte(sqlIndex[sqlIdx].Template)[:]...)
					m1 = append(m1, []byte("\n```\n")[:]...)
					log.Info("2")
					sqlIndex[sqlIdx] = sqlStruct{
						Count:     3,
						Printed:   true,
						Completed: true,
					}
					log.Info("3")
					// m1 = append(m1, []byte("\n\n> $\uparrow$ Esto es una plantilla que se repite\n\n")[:]...)
					msgOut <- msgStruct{Type: "BM", Content: string(m1) + "\n\n" + `> $\uparrow$ Esto es una plantilla que se repite` + "\n\n"}
				} else {
					m1 = append(m1, m[:]...)
					m1 = append(m1, []byte("\n```\n")[:]...)
					f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
					_, err = f.Write(m1)
					if err != nil {
						log.Fatalf("log failed: %v", err)
					}
					f.Close()
					log.Info("Printed")
				}
			}

			// SQL template
		case "BM":
			log.Debug("SQL template")

			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write([]byte(msg.Content))
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
			log.Info("Printed")

			// Output
		case "O":
			log.Debug("RESULT")
			m1 := []byte("\n```xml\n")
			m1 = append(m1, msg.Content[:]...)
			m1 = append(m1, []byte("\n```\n")[:]...)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(m1)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()

			// Block comment
		case "B":
			log.Debug("BLOCK")
			m1 := []byte("> ")
			m1 = append(m1, msg.Content[:]...)
			m1 = append(m1, []byte("\n\n")[:]...)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(m1)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
		}
		// }
	}
}

func base() {
	temp := ""
	for {
		select {
		case msg := <-msgCh:
			// for msg := range msgCh {
			// log.Warning("0 temp ---->%s\n", temp)
			// log.Warning("received ---->%s\n", msg.Content)
			// msgs <- fmt.Sprintf("received ---->%#v\n", msg)
			switch msg.Type {
			case byte('P'):
				pkg, tmp := handlePType(msg)
				// log.Warning("pkg.Content, temp ---->%s , %s\n", pkg.Content, temp)
				if tmp == "" {
					msgOut <- pkg
				} else {
					temp = tmp
				}
			case byte('Q'):
				msgOut <- msgStruct{Type: "M", Content: string(bytes.Trim(msg.Content, "\x00"))}
			case byte('B'):
				pkg := handleBType(temp, msg)
				msgOut <- pkg
			default:
				log.Error("unknown query type")
			}
			// log.Warning("1 temp ---->%s\n", temp)

		case <-time.After(5 * time.Minute):
			log.Error("timed out\n")
			run <- true
		}
	}
}

func handlePType(msg proxy.Pkg) (msgStruct, string) {
	newMsg := ""
	myMsgStruct := msgStruct{}

	if strings.Contains(string(msg.Content), "$1") {
		// msgs <- fmt.Sprintf("1 received ---->%#v\n", msg)
		// msgs <- fmt.Sprintf("1 received ---->%d bytes\n", len(msg.Content))
		var newM proxy.ReadBuf
		newM = msg.Content
		newM.Int32()

		// The name of the destination portal (an empty string selects the unnamed portal).
		p := bytes.Index(newM, []byte{112, 100, 111, 95, 115, 116, 109, 116, 95})
		// remove first string
		stringSize := 14
		// msgs <- fmt.Sprintf("msg ---->%#v\n", newM)
		// msgs <- fmt.Sprintf("msg ---->%s\n", string(newM))
		// msgs <- fmt.Sprintf("first string ---->%#v\n", newM[:stringSize+14])
		// msgs <- fmt.Sprintf("first string ---->%s\n", string(newM[:stringSize+14]))
		newM = newM[p+stringSize+1:]
		p = bytes.Index(newM, []byte{0})
		newMsg = string(newM[:p])
		// log.Debug("0 newMsg   ----->%s\n", newMsg)

		// temp = string(newMsg)
		// msgs <- fmt.Sprintf("1 temp ---->%#v\n", temp)
		// log.Notice("Returning: pkg %s -- temp %s", myMsgStruct.Content, newMsg)
		return myMsgStruct, newMsg
	} else {
		// msgs <- fmt.Sprintf("2 received ---->%#v\n", msg)
		selectIdx := strings.Index(string(msg.Content), string([]byte{83, 69, 76, 69, 67, 84, 32}))
		if selectIdx == -1 {
			selectIdx = 0
		}
		sepIdx := strings.Index(string(msg.Content), string([]byte{0, 0, 1, 0, 0}))
		if sepIdx == -1 || sepIdx+5 > len(msg.Content) {
			sepIdx := strings.Index(string(msg.Content), string([]byte{0, 1, 0, 0}))
			if sepIdx == -1 || sepIdx+4 > len(msg.Content) {
				sepIdx = len(msg.Content) - 4
			} else {
				sepIdx = len(msg.Content) - 1
			}
		}
		if sepIdx == -1 {
			sepIdx = len(msg.Content)
		}
		if selectIdx == -1 {
			selectIdx = 0
		}

		// log.Debug("SEP index ----->%v\n", sepIdx)
		// log.Debug("SEP len   ----->%v\n", len(msg.Content))
		// log.Debug("SEP CONT  ----->%v\n", msg.Content)
		// messages = append(messages, string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00")))
		myMsgStruct = msgStruct{Type: "M", Content: string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00"))}
		newMsg = ""
		// log.Notice("Returning: pkg %s -- temp %s", myMsgStruct.Content, newMsg)
		return myMsgStruct, newMsg
	}
	// log.Notice("Returning: pkg %s -- temp %s", myMsgStruct.Content, newMsg)
	// return myMsgStruct, newMsg
}

func handleBType(temp string, msg proxy.Pkg) msgStruct {
	msgs <- fmt.Sprintf("3 received ---->%#v\n", msg)
	msgs <- fmt.Sprintf("3 temp ---->%#v\n", temp)
	msgs <- fmt.Sprintf("3 len(msg.Content) ---->%#v\n", len(msg.Content))
	// if msg.Type == byte('B') && temp != "" && len(msg.Content) > 23 {
	msgs <- fmt.Sprintf("4 received ---->%#v\n", msg)
	var newMsg proxy.ReadBuf
	newMsg = msg.Content

	msgs <- fmt.Sprintf("5 received ---->%#v\n", msg)
	// The name of the destination portal (an empty string selects the unnamed portal).
	p := bytes.Index(newMsg, []byte{0})
	// remove first string
	msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
	msgs <- fmt.Sprintf("first string ---->%#v\n", newMsg[:p+1])
	newMsg = newMsg[p+1:]
	msgs <- fmt.Sprintf("0 newMsg   ----->%#v\n", newMsg)

	if newMsg[0] == 0 {
		p := bytes.Index(newMsg, []byte{0})
		// remove first string
		msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
		msgs <- fmt.Sprintf("1.1 first string ---->%#v\n", newMsg[:p+1])
		newMsg = newMsg[p:]
		msgs <- fmt.Sprintf("0.1 newMsg   ----->%#v\n", newMsg)
	}
	// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
	p = bytes.Index(newMsg, []byte{0})
	// remove second string
	msgs <- fmt.Sprintf("second string ---->%#v\n", newMsg[:p+1])
	newMsg = newMsg[p+1:]
	msgs <- fmt.Sprintf("1 newMsg   ----->%#v\n", newMsg)

	totalVar := newMsg.Int16()
	msgs <- fmt.Sprintf("vars types numbers ---->%#v\n", totalVar)
	// read variables' types
	for t1 := 0; t1 < totalVar; t1++ {
		varType := newMsg.Int16()
		msgs <- fmt.Sprintf("var #%d typeId----->%#v\n", t1+1, varType)
	}

	msgs <- fmt.Sprintf("23 newMsg   ----->%#v\n", newMsg)
	msgs <- fmt.Sprintf("23 t        ----->%#v\n", totalVar)
	// var total
	newMsg.Int16()
	vars := make(map[int]string)
	var varsIdx []int
	// if (totalVar == 0 && len(newMsg) > 4) || totalVar > len(newMsg) {
	// 	msgs <- fmt.Sprintf("23.1 newMsg   ----->%#v\n", newMsg)
	// 	msgs <- fmt.Sprintf("0 totalVar  ----->%d\n", totalVar)
	// 	for totalVar := 0; totalVar != 0 && totalVar < len(newMsg); totalVar = newMsg.Int16() {
	// 		msgs <- fmt.Sprintf("24 newMsg   ----->%#v\n", newMsg)
	// 		msgs <- fmt.Sprintf("1 totalVar  ----->%d\n", totalVar)
	// 	}
	// }
	//
	// if totalVar == 0 && len(newMsg) > 4 {
	// 	totalVar = newMsg.Int16()
	// }
	// if totalVar == 0 && len(newMsg) > 4 {
	// 	totalVar = newMsg.Int32()
	// }
	msgs <- fmt.Sprintf("totalVar   ----->%d\n", totalVar)
	for i := 0; i < totalVar; i++ {
		msgs <- fmt.Sprintf("2 newMsg   ----->%#v\n", newMsg)
		varLen := newMsg.Int32()
		// var1 := newMsg.Next(4)
		// // log.Debug("aa   -----> %#v\n", aa)
		// // log.Debug("aa bits ----->%8b\n", aa[len(aa)-1])
		// varLen := int(binary.BigEndian.Uint32(var1))
		// if varLen > len(newMsg) {
		// 	varLen = int(binary.BigEndian.Uint16(var1[:2]))
		// }
		if varLen > 0 {
			msgs <- fmt.Sprintf("varLen ----->%v\n", varLen)
			msgs <- fmt.Sprintf("newMsg   ----->%#v\n", newMsg)
			if varLen > len(newMsg) {
				varLen = len(newMsg) - 4
				msgs <- fmt.Sprintf("1 varLen ----->%v\n", varLen)
				msgs <- fmt.Sprintf("1 newMsg   ----->%#v\n", newMsg)
			}
			vars[i] = string(newMsg.Next(varLen))
			msgs <- fmt.Sprintf("vars   ----->%#v\n", vars)
			varsIdx = append(varsIdx, i)
			msgs <- fmt.Sprintf("varIdx  ----->%#v\n", varsIdx)
		}
	}

	sort.Sort(sort.Reverse(sort.IntSlice(varsIdx)))
	for _, k := range varsIdx {
		// messages = append(messages, strings.Replace(temp, fmt.Sprintf("$%d", k+1), fmt.Sprintf("'%s'", string(newMsg[k+1])), -1))
		temp = strings.Replace(temp, fmt.Sprintf("$%d", k+1), fmt.Sprintf("'%s'", string(vars[k])), -1)
		msgs <- fmt.Sprintf("message subst k ----->%v\n", k)
		msgs <- fmt.Sprintf("message subst newMsg ----->%#v\n", newMsg)
		msgs <- fmt.Sprintf("message subst msg ----->%v\n", vars[k+1])
		msgs <- fmt.Sprintf("message subst temp ----->%v\n", temp)
		msgs <- fmt.Sprintf("message subst param %s ----->%v\n", fmt.Sprintf("$%d", k+1), fmt.Sprintf("'%s'", vars[k]))
	}
	msgs <- fmt.Sprintf("end message  ----->%v\n", temp)
	// msgOut <- msgStruct{Type: "M", Content: temp}
	// } else {
	// }

	return msgStruct{Type: "M", Content: temp}
}
