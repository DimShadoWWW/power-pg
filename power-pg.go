package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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
	msgCh := make(chan string)
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
		for msg := range msgCh {
			fmt.Println("---------->", msg)
		}
	}()

	proxy.Start(localHost, remoteHost, getQueryModificada, msgCh)
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
