package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/proxy"
	"github.com/parnurzeal/gorequest"
)

var (
	localHost     = flag.String("l", ":9876", "Endereço e porta do listener local")
	remoteHost    = flag.String("r", "localhost:5432", "Endereço e porta do servidor PostgreSQL")
	remoteService = flag.String("s", "", "http://localhost:8080/query")
	messages      = []string{}
)

func main() {
	flag.Parse()
	msgs := make(chan string)
	msgCh := make(chan proxy.Pkg)
	if *remoteService != "" {
		go func() {
			time.Sleep(time.Second * 3)
			_, _, errs := gorequest.New().Get(*remoteService).End()
			if errs != nil {
				log.Fatalf("log failed: %v", errs)
			}
			log.Println("done")
			os.Exit(0)
		}()
	}

	go func() {
		for msg := range msgs {
			fmt.Println(msg)
		}
	}()

	go func() {
		temp := ""
		for msg := range msgCh {
			if msg.Type == 'P' {
				if strings.Contains(string(msg.Content), "$1") {
					selectIdx := strings.Index(string(msg.Content), string([]byte{83, 69, 76, 69, 67, 84, 32}))
					if selectIdx == -1 {
						selectIdx = 0
					}
					sepIdx := strings.Index(string(msg.Content), string([]byte{0, 1, 0, 0}))
					if sepIdx == -1 {
						sepIdx = len(msg.Content) - 4
					}
					temp = string(msg.Content[selectIdx:sepIdx])
					fmt.Printf("SEP index ----->%v\n", sepIdx)
					fmt.Printf("SEP len   ----->%v\n", len(msg.Content))
					fmt.Printf("SEP CONT  ----->%v\n", msg.Content)
				} else {
					temp = ""
					selectIdx := strings.Index(string(msg.Content), string([]byte{83, 69, 76, 69, 67, 84, 32}))
					if selectIdx == -1 {
						selectIdx = 0
					}
					sepIdx := strings.Index(string(msg.Content), string([]byte{0, 0, 1, 0, 0}))
					if sepIdx == -1 {
						sepIdx := strings.Index(string(msg.Content), string([]byte{0, 1, 0, 0}))
						if sepIdx == -1 {
							sepIdx = len(msg.Content) - 4
						}
					}
					messages = append(messages, string(msg.Content[selectIdx:sepIdx]))
				}
			} else {
				if msg.Type == 'B' && len(msg.Content) > 28 && temp != "" {
					messages = append(messages, strings.Replace(temp, "$1", fmt.Sprintf("'%s'", string(msg.Content[29:len(msg.Content)-4])), -1))
				}
				temp = ""
			}
			fmt.Printf("---------->%v\n", messages)
			fmt.Printf("---------->%#v\n", messages)
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
