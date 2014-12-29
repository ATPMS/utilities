package main

import (
	"time"
	"flag"
	"log"
	"fmt"
	"path/filepath"
	"os"
	"io"
	"compress/gzip"
)

func main() {
	dest := flag.String("dest", "raw_logs.txt", "Destination directory.")

	flag.Parse()

	if err := os.MkdirAll(*dest, 0777); err != nil {
		log.Fatal(err)
	}

	var count uint64 = 0
	var last_id uint64 = 0

	prev_ts := time.Unix(0, 0)

	var writer *gzip.Writer

	var id uint64
	var lon, lat, logged_at float64
	var session_id, vessel_id int32

	for {
		{
			n, err := fmt.Fscanln(os.Stdin, &id, &vessel_id, &session_id, &logged_at, &lon, &lat)
			if err == io.EOF {
				break
			} else if n != 6 {
				log.Fatalf("Unexpected input format: %v", err)
			}
		}

		ts := time.Unix(int64(logged_at), 0)

		if ts.Year() != prev_ts.Year() || ts.YearDay() != prev_ts.YearDay() {
			if writer != nil {
				writer.Close()
			}
			path := filepath.Join(*dest, ts.In(time.Local).Format("2006-01-02.log.gz"))
			file, err := os.OpenFile(path, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
			if err != nil {
				log.Fatalf("Cannot open %s for writing. Last id: %d, count: %d", path, last_id, count)
			}
			writer = gzip.NewWriter(file)
		}
		fmt.Fprintf(writer, "%d %d %.2f %.7f %.7f\n", int32(vessel_id), int32(session_id), logged_at, lon, lat)
		last_id = id
		count++
		if ( count % 1000 == 0 ) {
			log.Printf("Processed %d rows", count)
		}
	}

	if writer != nil {
		writer.Close()
	}
	log.Printf("Done proccessing %d rows. Last import: %d", count, last_id)
}
