package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/proxy"
	"github.com/parnurzeal/gorequest"
)

var (
	localHost     = flag.String("l", ":9876", "Endereço e porta do listener local")
	remoteHost    = flag.String("r", "localhost:5432", "Endereço e porta do servidor PostgreSQL")
	remoteService = flag.String("s", "", "http://localhost:8080/query")
	// messages      = []string{}
)

type msgStruct struct {
	Type    string
	Content string
}

func main() {
	flag.Parse()
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
		for {
			c := 0
			// select {
			// case msg1 := <-msgOut:
			msg := <-msgOut
			if msg.Type == "C" {
				f.Close()
				f, err = os.OpenFile(fmt.Sprintf("/reports/report-%s.md", msg.Content), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
				c = 0
				if err != nil {
					panic(err)
				}
				_, err := f.WriteString(fmt.Sprintf("# %s\n", msg.Content))
				if err != nil {
					log.Fatalf("log failed: %v", err)
				}
			} else {
				// case msg2 := <-msgOut:
				c = c + 1
				_, err := f.WriteString(fmt.Sprintf("%d. %s\n", c, msg.Content))
				if err != nil {
					log.Fatalf("log failed: %v", err)
				}
			}
		}
		// for msg := range msgOut {
		// 	if strings.Contains(msg, "# ") {
		// 		f.Close()
		// 		f, err = os.OpenFile(fmt.Sprintf("/reports/report-%s.md", strings.Replace(msg, "# ", "", -1)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	// fmt.Println(msg)
		// 	_, err := f.WriteString(fmt.Sprintf("%s\n", msg))
		// 	if err != nil {
		// 		log.Fatalf("log failed: %v", err)
		// 	}
		// }
		// f.Close()
	}()

	go func() {
		temp := ""
		for msg := range msgCh {
			if msg.Type == 'P' {
				if strings.Contains(string(msg.Content), "$1") {
					var newMsg proxy.ReadBuf
					newMsg = msg.Content
					_ = newMsg.Int32()

					// The name of the destination portal (an empty string selects the unnamed portal).
					p := bytes.Index(newMsg, []byte{0})
					// remove first string
					msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
					msgs <- fmt.Sprintf("msg ---->%s\n", string(newMsg))
					msgs <- fmt.Sprintf("first string ---->%#v\n", newMsg[:p+1])
					msgs <- fmt.Sprintf("first string ---->%s\n", string(newMsg[:p+1]))
					newMsg = newMsg[p+1:]
					fmt.Printf("0 newMsg   ----->%#v\n", newMsg)

					// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
					p = bytes.Index(newMsg, []byte{0})
					// remove second string
					msgs <- fmt.Sprintf("second string: message ---->%#v\n", newMsg[:p+1])
					temp = string(newMsg[:p+1])
					msgs <- fmt.Sprintf("second string: message temp ---->%s\n", temp)

					// fmt.Printf("1 newMsg   ----->%#v\n", newMsg)
					//
					// msgs <- fmt.Sprintf("vars types numbers ---->%#v\n", t)
					// for i := 0; i < t; i++ {
					// 	t = newMsg.Int16()
					// 	msgs <- fmt.Sprintf("22 newMsg   ----->%#v\n", newMsg)
					// }

					// selectIdx := strings.Index(string(msg.Content), string([]byte{83, 69, 76, 69, 67, 84, 32}))
					// if selectIdx == -1 {
					// 	selectIdx = 0
					// }
					// sepIdx := strings.Index(string(msg.Content), string([]byte{0, 1, 0, 0}))
					// if sepIdx == -1 {
					// 	sepIdx = len(msg.Content) - 4
					// }
					//
					// // temp = string(bytes.Trim(msg.Content[selectIdx:sepIdx], "\x00"))
					// temp = string(msg.Content[selectIdx:sepIdx])
					// fmt.Printf("SEP index ----->%v\n", sepIdx)
					// fmt.Printf("SEP len   ----->%v\n", len(msg.Content))
					// fmt.Printf("SEP CONT  ----->%v\n", msg.Content)
				} else {
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
				if msg.Type == 'B' && temp != "" {
					var newMsg proxy.ReadBuf
					newMsg = msg.Content

					// The name of the destination portal (an empty string selects the unnamed portal).
					p := bytes.Index(newMsg, []byte{0})
					// remove first string
					msgs <- fmt.Sprintf("msg ---->%#v\n", newMsg)
					msgs <- fmt.Sprintf("first string ---->%#v\n", newMsg[:p+1])
					newMsg = newMsg[p+1:]
					fmt.Printf("0 newMsg   ----->%#v\n", newMsg)

					// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
					p = bytes.Index(newMsg, []byte{0})
					// remove second string
					msgs <- fmt.Sprintf("second string ---->%#v\n", newMsg[:p+1])
					newMsg = newMsg[p+1:]
					fmt.Printf("1 newMsg   ----->%#v\n", newMsg)

					t := newMsg.Int16()
					msgs <- fmt.Sprintf("vars types numbers ---->%#v\n", t)
					for i := 0; i < t; i++ {
						t = newMsg.Int16()
						msgs <- fmt.Sprintf("22 newMsg   ----->%#v\n", newMsg)
					}

					totalVar := newMsg.Int16()
					vars := make(map[int]string)
					var varsIdx []int
					for i := 0; i < totalVar; i++ {
						msgs <- fmt.Sprintf("2 newMsg   ----->%#v\n", newMsg)
						varLen := newMsg.Int32()
						// aa := newMsg.Next(4)
						// fmt.Printf("aa   -----> %#v\n", aa)
						// fmt.Printf("aa bits ----->%8b\n", aa[len(aa)-1])
						// varLen := int(binary.BigEndian.Uint32(aa))
						msgs <- fmt.Sprintf("varLen ----->%v\n", varLen)
						msgs <- fmt.Sprintf("newMsg   ----->%#v\n", newMsg)
						if varLen > len(newMsg) {
							varLen = len(newMsg) - 4
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
					msgOut <- msgStruct{Type: "M", Content: temp}
					// fmt.Printf("2 newMsg   ----->%#v\n", newMsg)

					// idxPdo := strings.Index(string(msg.Content), "pdo_stmt_")
					//
					// if idxPdo != -1 {
					// 	var newMsg proxy.ReadBuf
					// 	// B type allways ends with 0100
					// 	fmt.Printf("msg.Content   ----->%#v\n", msg.Content)
					// 	newMsg = msg.Content[idxPdo+22 : len(msg.Content)-4]
					// 	fmt.Printf("1 newMsg   ----->%#v\n", newMsg)
					// 	totalVar := newMsg.Int16()
					//
					// 	vars := make(map[int]string)
					// 	var varsIdx []int
					// 	for i := 0; i < totalVar; i++ {
					// 		fmt.Printf("2 newMsg   ----->%#v\n", newMsg)
					// 		// varLen := newMsg.Int32()
					// 		aa := newMsg.Next(4)
					// 		fmt.Printf("aa   -----> %#v\n", aa)
					// 		fmt.Printf("aa bits ----->%8b\n", aa[len(aa)-1])
					// 		varLen := int(binary.BigEndian.Uint32(aa))
					// 		fmt.Printf("varLen ----->%v\n", varLen)
					// 		fmt.Printf("newMsg   ----->%#v\n", newMsg)
					// 		vars[i] = string(newMsg.Next(varLen))
					// 		fmt.Printf("vars   ----->%#v\n", vars)
					// 		varsIdx = append(varsIdx, i)
					// 		fmt.Printf("varIdx  ----->%#v\n", varsIdx)
					// 	}
					// 	sort.Sort(sort.Reverse(sort.IntSlice(varsIdx)))
					// 	for _, k := range varsIdx {
					// 		messages = append(messages, strings.Replace(temp, fmt.Sprintf("$%d", k+1), fmt.Sprintf("'%s'", string(newMsg[k+1])), -1))
					// 	}
					// } else {
					// 	messages = append(messages, string(msg.Content[29:len(msg.Content)-4]))
					// }

				}
				temp = ""
			}
			// fmt.Printf("---------->%v\n", messages)
			// fmt.Printf("---------->%#v\n", messages)
			// for k, v := range messages {
			// 	msgOut <- v
			// }
		}
	}()

	proxy.Start(localHost, remoteHost, getQueryModificada, msgs, msgCh)
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
