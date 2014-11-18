package main

import (
	"encoding/json"
	"net/http"
	"os"
	"flag"
	"bufio"
	"fmt"
	"log"
	"io"
	"time"
	"strconv"
	"../common"
)

const (
	SEND_TEMPLATE = "http://%s/api/locationlogs/save_bulk"
)

type ts struct {
	Date string `json:"$date"`
}
type DumpLine struct {
	Loc [] float64 `json:"loc"`
	Timestamp ts `json:"timestamp"`
	User_id uint64 `json:"user_id"`
}


type LogEntry struct {
	Lon       float64 `json:"lon"`
	Lat       float64 `json:"lat"`
	Timestamp int64   `json:"timestamp"`
	User_id   string  `json:"user_id"`
}

type SingleLogEntry struct {
	Logs [1]LogEntry `json:"locationlogs"`
}

type Line struct {
	content []byte
	number uint
}

var routeUrl string

func sender(c chan Line) {
	for line := range c {
		var dumpline DumpLine
		e := json.Unmarshal(line.content, &dumpline)
		if e != nil {
			log.Fatalf("Unable to parse json: %s (string: %s)", e, string(line.content))
		}

		reader, writer := io.Pipe()
		go func() {
			var sendData SingleLogEntry
			sendData.Logs[0].Lon = dumpline.Loc[0]
			sendData.Logs[0].Lat = dumpline.Loc[1]
			sendData.Logs[0].User_id = strconv.FormatUint(dumpline.User_id, 10)
			ts, err := time.Parse(common.MBD_TSFORMAT, dumpline.Timestamp.Date)
			sendData.Logs[0].Timestamp = ts.Unix()
			if err != nil {
				log.Fatalf("Unable to parse timestamp: %s", err)
			}

			encoder := json.NewEncoder(writer)
			err = encoder.Encode(sendData)
			if err != nil {
				log.Fatalf("Unable to encode log data: %s", err)
			}
			writer.Close()
		}()

		res, err := http.Post(routeUrl, "application/json", reader)
		if err != nil {
			log.Printf("Falied sending %s", string(line.content))
		}
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		fmt.Printf("Processed %d\n", line.number)
	}
}

func main() {
	route := flag.String("router", "localhost:8082", "Router host.")
	dumpfile := flag.String("dumpfile", "locationlogs.json", "MongoDB locationlogs JSON dump.")
	threads := flag.Uint("threads", 4, "Number of threads to run, sending data.")
	help := flag.Bool("help", false, "Prints help.")
	flag.Parse()

	if *help {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		return
	}

	dump, err := os.Open(*dumpfile)

	if err != nil {
		log.Fatal(err)
	}

	routeUrl = fmt.Sprintf(SEND_TEMPLATE, *route)

	scanner := bufio.NewScanner(dump)

	channel := make(chan Line, *threads * 5)

	for i := uint(0); i < *threads; i++ {
		go sender(channel)
	}

	var i uint = 0
	for scanner.Scan() {
		i++
		line := Line{append([]byte(nil), scanner.Bytes()...), i}
		
		channel <- line
	}
	close(channel)
}