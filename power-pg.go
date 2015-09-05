package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DimShadoWWW/power-pg/proxy"
	"github.com/DimShadoWWW/power-pg/utils"
	"github.com/DimShadoWWW/text"
	"github.com/boltdb/bolt"
	"github.com/deckarep/golang-set"
	"github.com/op/go-logging"
	"github.com/parnurzeal/gorequest"
	"github.com/yosssi/gohtml"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	// "github.com/davecgh/go-spew/spew"
	// _ "github.com/lib/pq"
	// "github.com/lunny/nodb"
	// "github.com/lunny/nodb/config"
)

// log "github.com/Sirupsen/logrus"

var (
	db *bolt.DB

	baseDir       = flag.String("b", "/", "directorio base para archivos")
	localHost     = flag.String("l", ":5432", "puerto local para escuchar")
	dbHostname    = flag.String("H", "localhost", "hostname del servidor PostgreSQL")
	dbPort        = flag.String("r", "5432", "puerto del servidor PostgreSQL")
	dbName        = flag.String("d", "dbname", "nombre de la base de datos")
	dbUsername    = flag.String("u", "username", "usuario para acceder al servidor PostgreSQL")
	dbPassword    = flag.String("p", "password", "password para acceder al servidor PostgreSQL")
	remoteService = flag.String("s", "", "http://localhost:8080/query")
	recreate      = flag.Bool("R", false, "recreacion de escenario")

	graph seqStruct
	// messages      = []string{}
)

type seqStruct struct {
	Seq        []int
	SeqStrings map[int]string
	Output     []string
}

func (s *seqStruct) Process() {
	spaces := regexp.MustCompile("[ \r\t\n]+")
	multipleSpaces := regexp.MustCompile("    ")

	includedPlantUml := mapset.NewSet()
	included := mapset.NewSet()

	var initial, final int
	s.Output = append(s.Output, "@startuml\n")
	init := true
	// spew.Dump(s)
	// os.Exit(0)
	for _, i := range s.Seq {
		final = i
		//  && includedPlantUml.Contains(fmt.Sprintf("Query_%d -> Query_%d\n", initial, i))
		if includedPlantUml.Contains(fmt.Sprintf("Query_%d -> Query_%d\n", initial, final)) {
			initial = final
		} else {
			// first loop
			if init {
				s.Output = append(s.Output, fmt.Sprintf("[*] --> Query_%d\n", final))
				init = false
			} else {
				// if !included.Contains(initial) {
				s.Output = append(s.Output, fmt.Sprintf("Query_%d -down-> Query_%d\n", initial, final))
				// } else {
				// 	s.Output = append(s.Output, fmt.Sprintf("Query_%d -right-> Query_%d\n", initial, final))
				// }
				includedPlantUml.Add(fmt.Sprintf("Query_%d -> Query_%d\n", initial, final))
				if s.SeqStrings[i] != "" {
					log.Debug("string(s.SeqStrings[i]): %#v\n", string(s.SeqStrings[i]))
					m := spaces.ReplaceAll([]byte(s.SeqStrings[i]), []byte{' '})
					log.Debug("11\n")
					a := multipleSpaces.ReplaceAll(m, []byte{' '})
					log.Debug("22\n")
					// text.Wrap(s string, lim int)
					b := text.Wrap(string(a), 120)
					log.Debug("33\n")
					wrapped := strings.Split(b, "\n")
					log.Debug("44\n")
					for _, v := range wrapped {
						// l := wordwrap.WrapString(v, 150)
						// for _, v1 := range l {
						s.Output = append(s.Output, fmt.Sprintf("Query_%d : %s\n", initial, string(v)))
						// }
					}
					log.Debug("55\n")
				}
			}
			initial = final
		}
		included.Add(initial)
	}
	s.Output = append(s.Output, fmt.Sprintf("Query_%d --> %s\n@enduml\n", initial, "[*]"))
	log.Debug("66\n")
}

type msgStruct struct {
	Type      string
	ID        int64
	Content   string
	Time      time.Duration
	TimeStr   string
	TimeTotal time.Duration
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

	log = logging.MustGetLogger("")

	firstChannelCallTime time.Time
)

func main() {
	flag.Parse()

	defer chownDir(fmt.Sprintf("%s/reports/", *baseDir), 1000, 1000)

	dbTempPath := "/db"
	_, err := os.Stat(dbTempPath)
	if os.IsNotExist(err) {
		dbTempPath = fmt.Sprintf("%s/db", *baseDir)
		if _, err := os.Stat(dbTempPath); os.IsNotExist(err) {
			err = os.MkdirAll(dbTempPath, 0777)
		}
	}

	if _, err := os.Stat(dbTempPath); os.IsNotExist(err) {
		defer chownDir(dbTempPath, 1000, 1000)
	}

	db, err = bolt.Open(dbTempPath+"/dynfeed.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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

func chownDir(path string, uid int, gid int) {
	entries, err := ioutil.ReadDir(path)

	for _, entry := range entries {
		sfp := path + "/" + entry.Name()
		if entry.IsDir() {
			chownDir(sfp, uid, gid)
		} else {
			// perform copy
			err = os.Chown(sfp, uid, gid)
			// os.Chmod(sfp, 0644)
			if err != nil {
				log.Warning(err.Error())
			}
		}
	}
	return
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
			firstChannelCallTime = time.Now()
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

	spaces := regexp.MustCompile("[ \r\t\n]+")
	// pdo_stmt_ := regexp.MustCompile("pdo_stmt_[0-9a-fA-F]{8}")
	multipleSpaces := regexp.MustCompile("    ")
	fname := ""
	// var sqlIndex sqlStructList

	var idx int64
	channel := ""

	for msg := range msgOut {
		log.Debug("+")
		switch msg.Type {
		// New file
		case "C":
			log.Debug("CHANNEL")
			channel = msg.Content

			fname = fmt.Sprintf("%s/reports/report-%s.md", *baseDir, msg.Content)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
			// c = 0
			if err != nil {
				panic(err)
			}
			// _, err = f.WriteString(fmt.Sprintf("# %s\n", msg.Content) + `\input{test.tex}` + "\n")
			// if err != nil {
			// 	log.Fatalf("log failed: %v", err)
			// }
			f.Close()
			idx = 0

			// receive SQL Query
		case "M":
			log.Debug("Receiving SQL")
			log.Info("0")
			idx++
			m := spaces.ReplaceAll([]byte(msg.Content), []byte{' '})
			m = multipleSpaces.ReplaceAll(m, []byte{' '})
			sqlIdx := m[:30]

			err := db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(channel))
				if err != nil {
					log.Warning("create bucket: %s", err)
					return fmt.Errorf("create bucket: %s", err)
				}

				// query
				b1, err := b.CreateBucketIfNotExists([]byte("queries"))
				if err != nil {
					log.Warning("failed to create bucket queries\n")
					return fmt.Errorf("failed to create bucket queries\n")
				}
				err = b1.Put([]byte(fmt.Sprintf("%05d", idx)), []byte(m))
				if err != nil {
					log.Warning("put %s on bucket %s: %s", m, "queries", err)
					return fmt.Errorf("put %s on bucket %s: %s", m, "queries", err)
				}

				// query alikes
				b2, err := b.CreateBucketIfNotExists(sqlIdx)
				if err != nil {
					log.Warning("failed to create bucket %s\n", string(sqlIdx))
					return fmt.Errorf("failed to create bucket %s\n", string(sqlIdx))
				}
				err = b2.Put([]byte(fmt.Sprintf("%05d", idx)), []byte(m))
				if err != nil {
					log.Warning("put %s on bucket %s: %s", m, string(sqlIdx), err)
					return fmt.Errorf("put %s on bucket %s: %s", m, string(sqlIdx), err)
				}

				// times
				b3, err := b.CreateBucketIfNotExists([]byte("times"))
				if err != nil {
					log.Warning("failed to create bucket queries\n")
					return fmt.Errorf("failed to create bucket queries\n")
				}

				log.Warning("Running time: %s\n", msg.Time.String())

				err = b3.Put([]byte(fmt.Sprintf("%05d", idx)), []byte(msg.Time.String()))
				if err != nil {
					log.Warning("put %s on bucket %s: %s", m, "queries", err)
					return fmt.Errorf("put %s on bucket %s: %s", m, "queries", err)
				}
				return nil
			})
			if err != nil {
				log.Fatal(fmt.Sprintf("Failed to db: %s", err))
			}

			// SQL Query
		case "S":
			log.Debug("SQL")
			m := spaces.ReplaceAll([]byte(msg.Content), []byte{' '})

			var m1 = []byte(fmt.Sprintf("\n"+`{\Large Query No.`+" %d}\n", msg.ID) +
				"\n***\n" + fmt.Sprintf("tiempo: %s", msg.Time.String()) +
				"\n```sql,classoffset=1,morekeywords={XXXXXX},keywordstyle=\\color{black}\\colorbox{yellowgreen},classoffset=0,\n")
			m1 = append(m1, m[:]...)
			m1 = append(m1, []byte("\n```\n")[:]...)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(m1)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
			log.Info("Printed")

			// SQL template
		case "BM":
			log.Debug("SQL template")

			msg := append([]byte(fmt.Sprintf("\n"+`{\Large Query No.`+" %d}\n", msg.ID)), []byte(msg.Content)[:]...)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(msg)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
			log.Info("Printed")

			// SQL template
		case "MSG":
			log.Debug("Message")

			msg := []byte(fmt.Sprintf("\n```\n%#v\n```\n", []byte(msg.Content)))
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(msg)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
			log.Info("Printed")

			// SQL template
		case "BM1":
			log.Debug("SQL template equal")

			msg := append([]byte(fmt.Sprintf("\n"+`{\Large Query No.`+" %d}\n", msg.ID)), []byte(msg.Content)[:]...)
			f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			_, err = f.Write(msg)
			if err != nil {
				log.Fatalf("log failed: %v", err)
			}
			f.Close()
			log.Info("Printed")

			// Output
		case "O":
			log.Debug("Output")
			// Generate
			included := mapset.NewSet()
			// log.Warning("msg.Content: %s\n", msg.Content)

			// plantuml
			//
			// includedPlantUml := mapset.NewSet()
			graph.SeqStrings = make(map[int]string)

			err := db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(channel))

				b1 := b.Bucket([]byte("queries"))
				if b1 != nil {
					c := b1.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						sqlIdx := v[:30]

						b2 := b.Bucket(sqlIdx)
						if b2 != nil {
							c1 := b2.Cursor()
							k1, q1 := c1.First()
							kI, err := strconv.ParseInt(string(bytes.TrimLeft(k, "0")), 10, 64)
							if err != nil {
								log.Fatalf("failed to convert str to int64: %v", err)
							}
							// has many
							if b2.Stats().KeyN > 1 {
								// if !included.Contains(string(sqlIdx)) {
								_, q2 := c1.Last()
								template := ""
								if !bytes.Equal(q1, q2) {
									template = utils.GetVariables(string(q1), string(q2))
								}
								graph.SeqStrings[int(kI)] = template
							} else {
								graph.SeqStrings[int(kI)] = string(v)
							}
							k1Int, err := strconv.ParseInt(string(bytes.TrimLeft(k1, "0")), 10, 64)
							if err != nil {
								log.Fatalf("failed to convert str to int64: %v", err)
							}
							graph.Seq = append(graph.Seq, int(k1Int))
						}
					}
				}
				return nil
			})
			graph.Process()
			f, err := os.OpenFile(strings.Replace(fname, "report-", "diagram-", -1)+".pu", os.O_WRONLY|os.O_CREATE, 0777)
			for _, st := range graph.Output {
				_, err = f.WriteString(st)
				if err != nil {
					log.Fatalf("log failed: %v", err)
				}
			}
			f.Close()

			err = db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(channel))
				b1 := b.Bucket([]byte("queries"))
				if b1 != nil {
					c := b1.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						// log.Warning("string(k): %#v\n", k)
						sqlIdx := v[:30]

						b3 := b.Bucket([]byte("times"))
						// log.Warning("string(k): %#v\n", k)
						c3 := b3.Cursor()
						// log.Warning("string(k): %#v\n", k)
						_, thisQueryTime := c3.Seek(k)
						log.Warning("thisQueryTime: %s\n", string(thisQueryTime))

						b2 := b.Bucket(sqlIdx)
						if b2 != nil {
							// has many
							if b2.Stats().KeyN > 1 {
								if !included.Contains(string(sqlIdx)) {
									c1 := b2.Cursor()
									totalTime := int64(0)
									count := int64(0)
									for k1, v := c3.First(); k1 != nil; k1, v = c3.Next() {
										count++
										dur, err := time.ParseDuration(string(v))
										if err != nil {
											log.Fatalf("failure parsing duration 1 %#v\n", v)
											dur = 0
										}
										totalTime += int64(dur)
									}
									// math.Float64bits(
									promTime := totalTime / count

									_, q1 := c1.First()
									_, q2 := c1.Last()

									log.Warning("totalTimeDur: %#v\n", totalTimeDur)
									log.Warning("promTimeDur: %#v\n", promTimeDur)
									totalTimeDur := time.Unix(totalTime, 0).Sub(time.Unix(int64(0), 0))
									promTimeDur := time.Unix(promTime, 0).Sub(time.Unix(int64(0), 0))
									// totalTimeDur, err := time.ParseDuration(string(totalTime))
									// if err != nil {
									// 	log.Fatalf("failure parsing duration 3 %d\n%#v\n%s\n", totalTime, totalTimeDur, err)
									// }
									// promTimeDur, err := time.ParseDuration(string(promTime) + "ns")
									// if err != nil {
									// 	log.Fatalf("failure parsing duration 2 %d\n%#v\n%s\n", promTime, promTimeDur, err)
									// }

									if bytes.Equal(q1, q2) {
										// generate template comparing first and last values
										template := string(q1)

										m1 := []byte(fmt.Sprintf("\n\ntiempo individual promedio: %s\ntiempo total: %s\n", promTimeDur.String(), totalTimeDur.String()))
										m1 = append(m1, []byte("\n```sql,classoffset=1,morekeywords={XXXXXX},keywordstyle=\\color{black}\\colorbox{yellowgreen},classoffset=0\n")[:]...)
										m1 = append(m1, []byte(template)[:]...)
										m1 = append(m1, []byte("\n```\n")[:]...)
										// m1 = append(m1, []byte("\n\n> $\uparrow$ Esto es una plantilla que se repite\n\n")[:]...)
										log.Warning("k: %#v\n", k)
										if s, err := strconv.ParseInt(strings.Trim(string(k), " "), 10, 64); err == nil {
											msgOut <- msgStruct{Type: "BM1", ID: s, Content: string(m1) + "\n\n" +
												`> $\uparrow$ Esta query se realiza ` + strconv.Itoa(b2.Stats().KeyN) +
												` veces` + "\n\n" + `Ejemplos:` + "\n" +
												`\begin{minipage}[c]{\textwidth}` + "\n```sql,frame=lrtb\n" +
												string(q1) + "\n" + string(q2) +
												"\n```\n" + `\end{minipage}` + "\n\n"}
										} else {
											log.Fatalf("failed to convert str to int64 3: %v", err)
										}
									} else {
										// generate template comparing first and last values
										template := utils.GetVariables(string(q1), string(q2))

										m1 := []byte(fmt.Sprintf("\n\ntiempo individual promedio: %s\ntiempo total: %s\n", promTimeDur.String(), totalTimeDur.String()))
										m1 = append(m1, []byte("\n```sql,classoffset=1,morekeywords={XXXXXX},keywordstyle=\\color{black}\\colorbox{yellowgreen},classoffset=0\n")[:]...)
										m1 = append(m1, []byte(template)[:]...)
										m1 = append(m1, []byte("\n```\n")[:]...)
										// m1 = append(m1, []byte("\n\n> $\uparrow$ Esto es una plantilla que se repite\n\n")[:]...)
										if s, err := strconv.ParseInt(strings.Trim(string(k), " "), 10, 64); err == nil {
											msgOut <- msgStruct{Type: "BM", ID: s, Content: string(m1) + "\n\n" +
												`> $\uparrow$ Esto es una plantilla que se repite ` + strconv.Itoa(b2.Stats().KeyN) +
												` veces` + "\n\n" + `Ejemplos:` + "\n" + `\begin{minipage}[c]{\textwidth}` + "\n```sql,frame=lrtb\n" +
												string(q1) + "\n" + string(q2) +
												"\n```\n" + `\end{minipage}` + "\n\n"}
										} else {
											log.Fatalf("failed to convert str to int64: %v", err)
										}
									}
								}
							} else {
								if s, err := strconv.ParseInt(strings.Trim(string(k), " "), 10, 64); err == nil {
									m1 := []byte(v)
									msgOut <- msgStruct{Type: "S", ID: s, Content: string(m1), TimeStr: string(thisQueryTime)}
								} else {
									log.Fatalf("failed to convert str to int64: %v", err)
								}
							}
						}
						included.Add(string(sqlIdx))
					}
				}
				return nil
			})

			if err != nil {
				log.Fatal(fmt.Sprintf("Failed to db: %s", err))
			}
			msgOut <- msgStruct{Type: "E", Content: msg.Content}

			// Output
		case "E":
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
				msgOut <- msgStruct{Type: "M", Content: string(bytes.Trim(msg.Content, "\x00")), Time: msg.Time}
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

	return msgStruct{Type: "M", Content: temp, Time: msg.Time}
}
