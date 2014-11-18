package main

import (
	"encoding/json"
	"os"
	"flag"
	"bufio"
	"fmt"
	"log"
	"time"
	"../common"
)

type ts struct {
	Date string `json:"$date"`
}
type DumpLine struct {
	Loc [] float64 `json:"loc"`
	Timestamp ts `json:"timestamp"`
	User_id uint64 `json:"user_id"`
}

const format_str = "%d %d %.16f %.16f\n"

func main() {
	help := flag.Bool("help", false, "Prints help.")
	flag.Parse()

	if *help {
		fmt.Printf("Input/output are stdin/stdout.")
		return
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {

		line := scanner.Bytes()

		var dumpline DumpLine
		e := json.Unmarshal(line, &dumpline)
		if e != nil {
			log.Fatalf("Unable to parse json: %s (string: %s)", e, string(line))
		}

		ts, err := time.Parse(common.MBD_TSFORMAT, dumpline.Timestamp.Date)
		if err != nil {
			log.Printf("Unable to parse timestamp %s: %s", dumpline.Timestamp.Date, err)
		}
		os.Stdout.WriteString(fmt.Sprintf(format_str, dumpline.User_id, ts.UnixNano()/(1000 * 1000 * 10), dumpline.Loc[0], dumpline.Loc[1]))
	}
}