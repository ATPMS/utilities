#! /usr/bin/env sh

host=${1:-"localhost:27017"}
out=${2:-"locationlogs.json"}

mongoexport -h "$host" -d atpms -c locationlogs --out "$out"