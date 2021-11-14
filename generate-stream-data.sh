#!/usr/bin/env bash

STREAMING_TMP_FILE=data/tmp.dat
STREAMING_TMP_FILE_2=data/tmp2.dat

for i in {1..5}
do
  echo "Creating file $i"
  STREAMING_FILE="data/stream/$(date +%s).dat"
  curl -s https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat | sort -R | head -n 1000 >> "$STREAMING_TMP_FILE"
  current_ts=$(date '+%Y-%m-%d %H:%M:%S')
  awk -v d="$current_ts" -F, '{$(9)=d;}1' OFS=, "$STREAMING_TMP_FILE" >> "$STREAMING_TMP_FILE_2"
  mv "$STREAMING_TMP_FILE_2" "$STREAMING_FILE"
  rm "$STREAMING_TMP_FILE"
  echo "Created $STREAMING_FILE, sleeping 10 seconds"
  sleep 10
done;
