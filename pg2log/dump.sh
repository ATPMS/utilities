#! /usr/bin/env bash

DEST=raw_logs.txt

psql $@ <<'HERE' | ./pg2log -dest="${DEST}"
COPY (
		SELECT id, vessel_id, session_token, EXTRACT(EPOCH FROM logged_at) AS timestamp, lon, lat
			FROM logs ORDER BY logged_at ASC
	) TO STDOUT WITH DELIMITER ' '"
HERE