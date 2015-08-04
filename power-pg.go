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
			if msg.Type == 'P' && strings.Contains(string(msg.Content), "$1") {
				temp = string(msg.Content[:len(msg.Content)-4])
			} else {
				if msg.Type == 'B' && len(msg.Content) > 28 {
					messages = append(messages, strings.Replace(temp, "$1", fmt.Sprintf("'%s'", string(msg.Content[29:len(msg.Content)-4])), -1))
				}
				temp = ""
			}
			fmt.Printf("---------->%v\n", messages)
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
