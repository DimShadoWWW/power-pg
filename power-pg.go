package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	// "database/sql"

	_ "github.com/lib/pq"

	"github.com/DimShadoWWW/power-pg/proxy"
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
	// messages      = []string{}
)

type msgStruct struct {
	Type    string
	Content string
}

var (
	db *sql.DB
)

func main() {
	flag.Parse()
	// dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
	// 	*dbUsername, *dbPassword, *dbName)
	// db, err := sql.Open("postgres", dbinfo)

	msgs := make(chan string)
	msgCh := make(chan proxy.Pkg)
	msgOut := make(chan msgStruct)
	if *remoteService != "" {
		go func() {
			time.Sleep(time.Second * 3)
			inFile, _ := os.Open("canales_list.txt")
			defer inFile.Close()
			scanner := bufio.NewScanner(inFile)
			scanner.Split(bufio.ScanLines)

			for scanner.Scan() {
				time.Sleep(time.Second * 1)
				// messages = []string{}
				// fmt.Println(scanner.Text())
				// msgOut <- fmt.Sprintf("# %s\n", scanner.Text())
				msgOut <- msgStruct{Type: "C", Content: scanner.Text()}
				_, _, errs := gorequest.New().Get(fmt.Sprintf("%s%s", *remoteService, scanner.Text())).End()
				if errs != nil {
					log.Fatalf("log failed: %v", errs)
				}
			}
			log.Println("done")
			os.Exit(0)
		}()
	}

	go func() {
		f, err := os.OpenFile("/all.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		for msg := range msgs {
			// fmt.Println(msg)
			_, err := f.WriteString(fmt.Sprintf("%s\n", msg))
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
		}
	}()

	go func() {
		f, err := os.OpenFile("/reports/report.md", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		// c := 0
		spaces := regexp.MustCompile("[\t]+")
		// pdo_stmt_ := regexp.MustCompile("pdo_stmt_[0-9a-fA-F]{8}")
		multipleSpaces := regexp.MustCompile("    ")
		for {
			// select {
			// case msg1 := <-msgOut:
			msg := <-msgOut
			if msg.Type == "C" {
				// c = 0
				f.Close()
				f, err = os.OpenFile(fmt.Sprintf("/reports/report-%s.md", msg.Content), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
				// c = 0
				if err != nil {
					panic(err)
				}
				_, err := f.WriteString(fmt.Sprintf("# %s\n", msg.Content))
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
	}()

	go func() {
		temp := ""
		for msg := range msgCh {
			msgs <- fmt.Sprintf("received ---->%#v\n", msg)
			if msg.Type == byte('P') {
				if strings.Contains(string(msg.Content), "$1") {
					msgs <- fmt.Sprintf("1 received ---->%#v\n", msg)
					var newMsg proxy.ReadBuf
					newMsg = msg.Content
					_ = newMsg.Int32()

					// The name of the destination portal (an empty string selects the unnamed portal).
					p := bytes.Index(newMsg, []byte{112, 100, 111, 95, 115, 116, 109, 116, 95})
					// remove first string
					msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
					msgs <- fmt.Sprintf("msg ---->%s\n", string(newMsg))
					msgs <- fmt.Sprintf("first string ---->%#v\n", newMsg[:p+16])
					msgs <- fmt.Sprintf("first string ---->%s\n", string(newMsg[:p+16]))
					newMsg = newMsg[p+16:]
					fmt.Printf("0 newMsg   ----->%#v\n", newMsg)

					// // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
					// p = bytes.Index(newMsg, []byte{0})
					// // remove second string
					// msgs <- fmt.Sprintf("second string: message ---->%#v\n", newMsg[:p])
					// temp = string(newMsg[:p])
					// msgs <- fmt.Sprintf("second string: message temp ---->%s\n", temp)

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

					fmt.Printf("SEP index ----->%v\n", sepIdx)
					fmt.Printf("SEP len   ----->%v\n", len(msg.Content))
					fmt.Printf("SEP CONT  ----->%v\n", msg.Content)
					// messages = append(messages, string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00")))
					msgOut <- msgStruct{Type: "M", Content: string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00"))}
				}
			} else {
				msgs <- fmt.Sprintf("3 received ---->%#v\n", msg)
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

					t := newMsg.Int16()
					msgs <- fmt.Sprintf("vars types numbers ---->%#v\n", t)
					for t1 := 0; t1 == t; t1 = newMsg.Int16() {
						msgs <- fmt.Sprintf("22 newMsg   ----->%#v\n", newMsg)
					}

					msgs <- fmt.Sprintf("23 newMsg   ----->%#v\n", newMsg)
					totalVar := t
					// newMsg.Int16()
					vars := make(map[int]string)
					var varsIdx []int
					if (totalVar == 0 && len(newMsg) > 4) || totalVar > len(newMsg) {
						msgs <- fmt.Sprintf("23.1 newMsg   ----->%#v\n", newMsg)
						msgs <- fmt.Sprintf("0 totalVar  ----->%d\n", totalVar)
						for totalVar := 0; totalVar != 0 && totalVar < len(newMsg); totalVar = newMsg.Int16() {
							msgs <- fmt.Sprintf("24 newMsg   ----->%#v\n", newMsg)
							msgs <- fmt.Sprintf("1 totalVar  ----->%d\n", totalVar)
						}
					}
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
						// // fmt.Printf("aa   -----> %#v\n", aa)
						// // fmt.Printf("aa bits ----->%8b\n", aa[len(aa)-1])
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
	}()

	proxy.Start(localHost, dbHostname, dbPort, getQueryModificada, msgs, msgCh)
}

func getQueryModificada(queryOriginal string) string {
	// log.Println("aa")
	// if queryOriginal[:5] != "power" {
	// 	return queryOriginal
	// }

	// log.Println(queryOriginal)
	fmt.Println(queryOriginal)
	return queryOriginal
}
