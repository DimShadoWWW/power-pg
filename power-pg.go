package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/proxy"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"github.com/parnurzeal/gorequest"
)

var (
	localHost     = flag.String("l", ":5432", "puerto local para escuchar")
	dbHostname    = flag.String("h", "localhost", "hostname del servidor PostgreSQL")
	dbPort        = flag.String("r", "5432", "puerto del servidor PostgreSQL")
	dbName        = flag.String("d", "dbname", "nombre de la base de datos")
	dbUsername    = flag.String("u", "username", "usuario para acceder al servidor PostgreSQL")
	dbPassword    = flag.String("p", "password", "password para acceder al servidor PostgreSQL")
	remoteService = flag.String("s", "", "http://localhost:8080/query")
	recreate      = flag.Bool("R", false, "Recreate saved")
	// messages      = []string{}
)

type msgStruct struct {
	Type    string
	Content string
}

var (
	msgs     = make(chan string)
	msgBytes = make(chan []byte)
	msgCh    = make(chan proxy.Pkg)
	msgOut   = make(chan msgStruct)

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
	proxy.Start(localHost, dbHostname, dbPort, msgBytes, msgCh, *recreate, log)
}

func getQueryModificada(queryOriginal string) string {
	log.Debug(queryOriginal)
	return queryOriginal
}

func callURIs() {
	time.Sleep(time.Second * 3)
	inFile, _ := os.Open("canales_list.txt")
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		time.Sleep(time.Millisecond * 1500)

		// send message for new channel
		msgOut <- msgStruct{Type: "C", Content: scanner.Text()}

		_, _, errs := gorequest.New().Get(fmt.Sprintf("%s%s", *remoteService, scanner.Text())).End()
		if errs != nil {
			log.Fatalf("log failed: %v", errs)
		}
	}
	log.Info("done")
	os.Exit(0)
}

func recreateFunc() {
	laddr, err := net.ResolveTCPAddr("tcp", *localHost)
	if err != nil {
		log.Critical("Local recreation's connection failed: %s", err)
		return
	}

	conn, err := net.DialTCP("tcp", nil, laddr)
	if err != nil {
		log.Critical("Local recreation's connection failed: %s", err)
		return
	}

	f, err := os.Open("/all.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		msg := []byte{}
		for !strings.Contains(scanner.Text(), "ENDMSG") {
			msg = append(msg, scanner.Bytes()[:]...)
			scanner.Scan()
		}

		_, err := conn.Write(msg)
		if err != nil {
			log.Critical("Write failed '%s'\n", err)
		}
	}
}

func recreateLogDump() {
	f, err := os.OpenFile("/all.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for msg := range msgBytes {
		// log.Debug(msg)
		// .WriteString(fmt.Sprintf("%s\n", msg))
		_, err := f.Write(msg)
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}

		_, err = f.Write([]byte("\nENDMSG\n"))
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}
	}
}

func recreateLogDoNothing() {
	for range msgBytes {
	}
}

func baseLog() {
	f, err := os.OpenFile("/all2.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for msg := range msgs {
		log.Debug(msg)
		_, err := f.WriteString(fmt.Sprintf("%s\n", msg))
		if err != nil {
			log.Fatalf("log failed: %v", err)
		}
	}
}

func logReport() {
	var f *os.File

	spaces := regexp.MustCompile("[\t]+")
	// pdo_stmt_ := regexp.MustCompile("pdo_stmt_[0-9a-fA-F]{8}")
	multipleSpaces := regexp.MustCompile("    ")
	for {
		// select {
		// case msg1 := <-msgOut:
		msg := <-msgOut
		log.Debug("%#v\n", msg)
		if msg.Type == "C" {
			// c = 0
			f.Close()
			f, err := os.OpenFile(fmt.Sprintf("/reports/report-%s.md", msg.Content), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
			// c = 0
			if err != nil {
				panic(err)
			}
			_, err = f.WriteString(fmt.Sprintf("# %s\n", msg.Content))
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
		} else {
			// case msg2 := <-msgOut:
			// c = c + 1
			m := spaces.ReplaceAll([]byte(msg.Content), []byte{' '})
			m = multipleSpaces.ReplaceAll(m, []byte{' '})
			_, err := f.WriteString(fmt.Sprintf("\n```sql\n%s\n```\n", string(m)))
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
		}
	}
}
func base() {
	temp := ""
	for msg := range msgCh {
		msgs <- fmt.Sprintf("received ---->%#v\n", msg)
		if msg.Type == byte('P') {
			if strings.Contains(string(msg.Content), "$1") {
				msgs <- fmt.Sprintf("1 received ---->%#v\n", msg)
				msgs <- fmt.Sprintf("1 received ---->%d bytes\n", len(msg.Content))
				var newMsg proxy.ReadBuf
				newMsg = msg.Content
				_ = newMsg.Int32()

				// The name of the destination portal (an empty string selects the unnamed portal).
				p := bytes.Index(newMsg, []byte{112, 100, 111, 95, 115, 116, 109, 116, 95})
				// remove first string
				stringSize := 14
				msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
				msgs <- fmt.Sprintf("msg ---->%s\n", string(newMsg))
				msgs <- fmt.Sprintf("first string ---->%#v\n", newMsg[:stringSize+14])
				msgs <- fmt.Sprintf("first string ---->%s\n", string(newMsg[:stringSize+14]))
				newMsg = newMsg[p+stringSize+1:]
				p = bytes.Index(newMsg, []byte{0})
				newMsg = newMsg[:p]
				log.Debug("0 newMsg   ----->%s\n", newMsg)

				temp = string(newMsg)
				msgs <- fmt.Sprintf("1 temp ---->%#v\n", temp)
			} else {
				msgs <- fmt.Sprintf("2 received ---->%#v\n", msg)
				temp = ""
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

				log.Debug("SEP index ----->%v\n", sepIdx)
				log.Debug("SEP len   ----->%v\n", len(msg.Content))
				log.Debug("SEP CONT  ----->%v\n", msg.Content)
				// messages = append(messages, string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00")))
				msgOut <- msgStruct{Type: "M", Content: string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00"))}
			}
		} else {
			msgs <- fmt.Sprintf("3 received ---->%#v\n", msg)
			msgs <- fmt.Sprintf("3 temp ---->%#v\n", temp)
			msgs <- fmt.Sprintf("3 len(msg.Content) ---->%#v\n", len(msg.Content))
			if msg.Type == byte('B') && temp != "" && len(msg.Content) > 23 {
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
				for t1 := 0; t1 < totalVar; t1 = newMsg.Int16() {
					varType := newMsg.Int16()
					msgs <- fmt.Sprintf("var #%d typeId----->%#v\n", t1+1, varType)
				}

				msgs <- fmt.Sprintf("23 newMsg   ----->%#v\n", newMsg)
				msgs <- fmt.Sprintf("23 t        ----->%#v\n", totalVar)
				// var total
				// totalVar := newMsg.Int16()
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
			}
			msgOut <- msgStruct{Type: "M", Content: temp}
			temp = ""
		}
	}
}
