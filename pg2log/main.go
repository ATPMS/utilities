package main

import (
	_ "github.com/lib/pq"
	"database/sql"
	"time"
	"flag"
	"log"
	"fmt"
	"path/filepath"
	"os"
	"compress/gzip"
)

func main() {
	conn_url := flag.String("url", "postgres://user:password@localhost/database", "Connection URL.")
	dest := flag.String("dest", "raw_logs.txt", "Destination directory.")
	start := flag.Uint64("start", 0, "Starting id.")

	flag.Parse()

	if err := os.MkdirAll(*dest, 0777); err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", *conn_url)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(fmt.Sprintf("SELECT id, lon, lat, EXTRACT(EPOCH FROM logged_at) AS timestamp, session_token, vessel_id FROM logs WHERE id > %d ORDER BY logged_at ASC", *start))

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var count uint64 = 0
	var last_id uint64 = 0

	prev_ts := time.Unix(0, 0)

	var writer *gzip.Writer

	for rows.Next() {
		var id uint64
		var lon, lat, logged_at float64
		var session_id, vessel_id int32
		if err := rows.Scan(&id, &lon, &lat, &logged_at, &session_id, &vessel_id); err != nil {
			log.Fatalf("Error in reading row %d (last id: %d): %v", count + 1, last_id, err)
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
	if rows.Err() != nil {
		log.Printf("Error encountered while processing: %v", rows.Err())
	}

	if writer != nil {
		writer.Close()
	}
	log.Printf("Done proccessing %d rows. Last import: %d", count, last_id)
}
